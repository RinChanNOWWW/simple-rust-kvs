use crate::{KvsEngine, KvsError, Result};
use sled::{self, Db};

#[derive(Clone)]
pub struct SledKvsEngine(Db);

impl KvsEngine for SledKvsEngine {
    fn get(&self, key: String) -> Result<Option<String>> {
        let db = &self.0;
        match db.get(key)? {
            Some(vec) => Ok(Some(String::from_utf8(vec.to_vec())?)),
            None => Ok(None),
        }
    }
    fn set(&self, key: String, value: String) -> Result<()> {
        let db = &self.0;
        db.insert(key, value.into_bytes())?;
        db.flush()?;
        Ok(())
    }
    fn remove(&self, key: String) -> Result<()> {
        let db = &self.0;
        db.remove(key)?.ok_or(KvsError::KeyNotFound)?;
        db.flush()?;
        Ok(())
    }
}

impl SledKvsEngine {
    pub fn open<P: AsRef<std::path::Path>>(path: P) -> Result<Self> {
        let db = sled::open(path)?;
        Ok(SledKvsEngine(db))
    }
}
