use std::thread::{self, Builder};

use crossbeam::channel::{unbounded, Receiver, Sender};
use log::error;

use super::ThreadPool;
use crate::Result;

//
// The SharedQueueThreadPool when thread panic, create new thread. because
//
// Rust document write:
// ```
// https://doc.rust-lang.org/std/panic/fn.catch_unwind.html
// Note that this function may not catch all panics in Rust.
// ```
//
// not all panic can catch unwind.
//
// if all panic can catch, then catch_unwind scheme can avoid
// thread create spend.
//
pub struct SharedQueueThreadPool {
    tx: Sender<Box<dyn FnOnce() + Send + 'static>>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        let (tx, rx) = unbounded::<Box<dyn FnOnce() + Send + 'static>>();
        for _ in 0..threads {
            let tr = TaskReceiver(rx.clone());
            Builder::new().spawn(move || task_entry(tr))?;
        }

        Ok(SharedQueueThreadPool { tx })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        if let Err(e) = self.tx.send(Box::new(job)) {
            error!("SharedQueueThreadPool spawn fail, {:?}", e);
        }
    }
}

#[derive(Clone)]
struct TaskReceiver(Receiver<Box<dyn FnOnce() + Send + 'static>>);

impl Drop for TaskReceiver {
    fn drop(&mut self) {
        if thread::panicking() {
            let tr = self.clone();
            Builder::new()
                .spawn(move || task_entry(tr))
                .expect("thread panic, create new thread fail");
        }
    }
}

fn task_entry(tr: TaskReceiver) {
    for task in tr.0.iter() {
        task();
    }
}
