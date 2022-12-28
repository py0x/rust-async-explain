use std::collections::{HashMap, VecDeque};
use std::future::Future;
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};
use std::{thread, time};

use crossbeam::channel as cc;
use futures::task::{waker, ArcWake};
use uuid::Uuid;

struct Executor {
    task_sender: Option<cc::Sender<Arc<Task>>>,
    task_receiver: cc::Receiver<Arc<Task>>,
    runner_handle: Option<thread::JoinHandle<()>>,
}

impl Executor {
    fn new() -> Self {
        let (tx, rx) = cc::unbounded::<Arc<Task>>();

        Self {
            task_sender: Some(tx),
            task_receiver: rx,
            runner_handle: None,
        }
    }

    fn run(&mut self) {
        let task_rx = self.task_receiver.clone();

        let handle = thread::spawn(move || {
            while let Ok(task) = task_rx.recv() {
                let waker = waker(task.clone());
                let mut cx = Context::from_waker(&waker);

                let res = task.fut.lock().unwrap().as_mut().poll(&mut cx);

                println!("Executor: polled task, res: {:?}", res);
            }

            println!("Executor: finished running all tasks");
        });

        self.runner_handle = Some(handle);
    }

    fn spawn<F>(&mut self, fut: F)
    where
        F: Future<Output = String> + Send + 'static,
    {
        if let Some(task_tx) = &self.task_sender {
            let task = Arc::new(Task::new(fut, task_tx.clone()));
            task.spawn();

            println!("Executor: spawn() new task");
        } else {
            println!("Executor: spawn() failed: already shutdown");
        }
    }

    fn shutdown(&mut self) {
        if let Some(tx) = self.task_sender.take() {
            drop(tx);
        }

        if let Some(h) = self.runner_handle.take() {
            h.join().unwrap();
        }
    }
}

struct Task {
    fut: Mutex<Pin<Box<dyn Future<Output = String> + Send>>>,
    executor_queue: cc::Sender<Arc<Task>>,
}

impl Task {
    fn new<F>(fut: F, executor_queue: cc::Sender<Arc<Task>>) -> Self
    where
        F: Future<Output = String> + Send + 'static,
    {
        Task {
            fut: Mutex::new(Box::pin(fut)),
            executor_queue,
        }
    }

    fn spawn(self: &Arc<Self>) {
        self.executor_queue.send(self.clone()).unwrap();
    }
}

impl ArcWake for Task {
    fn wake_by_ref(arc_self: &Arc<Self>) {
        println!("Task: waked");

        // resend the task to the executor queue
        arc_self.spawn()
    }
}

type EventId = String;

enum EventStatus {
    Waiting(Waker),
    Ok,
}

struct EventCenter {
    inner: Arc<Mutex<EventCenterInner>>,
}

struct EventCenterInner {
    registry: Arc<Mutex<HashMap<EventId, EventStatus>>>,
    thread_handles: VecDeque<thread::JoinHandle<()>>,
}

impl EventCenter {
    fn new() -> Self {
        let inner = Arc::new(Mutex::new(EventCenterInner {
            registry: Arc::new(Mutex::new(HashMap::<EventId, EventStatus>::new())),
            thread_handles: VecDeque::new(),
        }));

        Self { inner }
    }

    fn register(&self, evt_id: EventId, waker: Waker) {
        let registry = self.inner.lock().unwrap().registry.clone();

        registry
            .lock()
            .unwrap()
            .insert(evt_id.clone(), EventStatus::Waiting(waker));

        // vars for moving to thread closure
        let registry2 = registry.clone();
        let task_id2 = evt_id.clone();

        let handle = thread::spawn(move || {
            // pretend that the event would be available in 2500 ms
            thread::sleep(time::Duration::from_millis(2500));

            let mut reg = registry2.lock().unwrap();

            // update event status
            let old_status = reg.insert(task_id2, EventStatus::Ok);

            // wake up the task
            if let Some(EventStatus::Waiting(waker)) = old_status {
                waker.wake();
            }
        });

        self.inner.lock().unwrap().thread_handles.push_back(handle);
    }

    fn deregister(&self, evt_id: &EventId) {
        let registry = self.inner.lock().unwrap().registry.clone();
        registry.lock().unwrap().remove(evt_id);
    }

    fn is_event_available(&self, evt_id: &EventId) -> bool {
        let registry = self.inner.lock().unwrap().registry.clone();

        if let Some(EventStatus::Ok) = registry.lock().unwrap().get(evt_id) {
            return true;
        }

        return false;
    }

    fn wait(&self) {
        loop {
            if let Some(h) = self.inner.lock().unwrap().thread_handles.pop_front() {
                h.join().unwrap();
            } else {
                break;
            }
        }
    }
}

impl Clone for EventCenter {
    fn clone(&self) -> Self {
        Self {
            inner: self.inner.clone(),
        }
    }
}

struct DummySocket {
    id: EventId,
    event_center: EventCenter,
    //The real Socket might wrap a `socket_fd` as an event source
}

impl DummySocket {
    fn new(event_center: EventCenter) -> Self {
        Self {
            id: Uuid::new_v4().to_string(),
            event_center,
        }
    }
}

impl Future for DummySocket {
    type Output = String;

    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let res = 'poll: {
            if self.event_center.is_event_available(&self.id) {
                self.event_center.deregister(&self.id);
                break 'poll Poll::Ready(self.id.to_string());
            }

            self.event_center
                .register(self.id.clone(), cx.waker().clone());

            break 'poll Poll::Pending;
        };

        println!("DummySocket: polling, id = {:?}, res = {:?}", self.id, res);

        return res;
    }
}

fn main() {
    // create the executor and event_center
    let mut executor = Executor::new();
    let event_center = EventCenter::new();

    // spawn an async task1
    let ec2 = event_center.clone();
    executor.spawn(async move { DummySocket::new(ec2).await });

    // spawn an async task2
    let ec3 = event_center.clone();
    executor.spawn(async move { DummySocket::new(ec3).await });

    // run the executor
    executor.run();

    // shutdown everything
    event_center.wait();
    executor.shutdown();
}
