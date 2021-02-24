use super::ThreadPool;
use crate::{KvsError, Result};
use rayon;

pub struct RayonThreadPool(rayon::ThreadPool);

impl ThreadPool for RayonThreadPool {
    fn new(num_threads: u32) -> Result<Self>
    where
        Self: Sized,
    {
        Ok(RayonThreadPool(
            rayon::ThreadPoolBuilder::new()
                .num_threads(num_threads as usize)
                .build()
                .map_err(|e| KvsError::OtherError(format!("{}", e)))?,
        ))
    }

    fn spawn<F>(&self, job: F)
    where
        F: FnOnce() + Send + 'static,
    {
        self.0.spawn(job);
    }
}
