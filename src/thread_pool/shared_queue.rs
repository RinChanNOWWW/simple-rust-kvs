use super::ThreadPool;
use crate::Result;
use crossbeam::channel::{Receiver, Sender};
use std::thread;

type Job = Box<dyn FnOnce() + Send + 'static>;

#[derive(Clone)]
struct Worker(Receiver<Job>);

fn do_job(worker: Worker) {
    loop {
        match worker.0.recv() {
            Ok(job) => job(),
            Err(_) => {}
        }
    }
}

pub struct SharedQueueThreadPool {
    sender: Sender<Job>,
}

impl ThreadPool for SharedQueueThreadPool {
    fn new(threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        let (sender, receiver) = crossbeam::channel::unbounded();
        for _ in 0..threads {
            let worker = Worker(receiver.clone());
            thread::spawn(move || do_job(worker));
        }
        Ok(SharedQueueThreadPool { sender })
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.sender.send(Box::new(job)).unwrap();
    }
}

impl Drop for Worker {
    fn drop(&mut self) {
        if thread::panicking() {
            let worker = self.clone();
            thread::spawn(move || do_job(worker));
        }
    }
}
