use failure::Fail;
use io::{BufRead, BufReader, Write};
use ron::ser;
use serde::{Deserialize, Serialize};
use std::{collections::HashMap, env, io, ops::Deref};
use std::{
    fs,
    fs::{File, OpenOptions},
    path::PathBuf,
    result,
};

#[derive(Debug, Fail)]
pub enum KvStoreError {
    #[fail(display = "open kv file error: {}", _0)]
    IoError(io::Error),
    #[fail(display = "Key not found")]
    KeyNotFound,
}

#[derive(Debug, Serialize, Deserialize)]
enum Command {
    Set { key: String, value: String },
    Remove { key: String },
}

pub type Result<T> = result::Result<T, KvStoreError>;

pub struct KvStore {
    file: File,
    map: HashMap<String, String>,
}

impl KvStore {
    fn new(file: File) -> KvStore {
        KvStore {
            file,
            map: HashMap::new(),
        }
    }

    fn get_kv_map(&mut self) {
        if let Ok(file) = self.file.try_clone() {
            for line in BufReader::new(file).lines() {
                if let Ok(content) = line {
                    if let Ok(cmd) = ron::from_str::<Command>(&content[..]) {
                        match cmd {
                            Command::Set { key, value } => {
                                self.map.insert(key, value);
                            }
                            Command::Remove { key } => {
                                self.map.remove(&key);
                            }
                        }
                    }
                }
            }
        }
    }

    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let path_buf: PathBuf = path.into();
        let path = path_buf.clone();
        if !path_buf.exists() {
            fs::create_dir_all(path_buf).map_err(KvStoreError::IoError)?;
        }
        env::set_current_dir(path.into_boxed_path().deref()).unwrap();
        let f = OpenOptions::new()
            .create(true)
            .read(true)
            .append(true)
            .open("latest.kv")
            .map_err(KvStoreError::IoError)?;
        let mut kv = KvStore::new(f);
        kv.get_kv_map();
        Ok(kv)
    }

    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        self.map.insert(key.clone(), value.clone());
        let cmd = Command::Set { key, value };
        let mut s = ser::to_string(&cmd).unwrap();
        s.push('\n');
        self.file
            .write(s.as_bytes())
            .map_err(KvStoreError::IoError)?;
        Ok(())
    }

    pub fn get(&self, key: String) -> Result<Option<String>> {
        Ok(self.map.get(&key).and_then(|x| Some(x.to_string())))
    }

    pub fn remove(&mut self, key: String) -> Result<()> {
        if let None = self.map.get(&key) {
            return Err(KvStoreError::KeyNotFound);
        }
        self.map.remove(&key);
        let cmd = Command::Remove { key };
        let mut s = ser::to_string(&cmd).unwrap();
        s.push('\n');
        self.file
            .write(s.as_bytes())
            .map_err(KvStoreError::IoError)?;
        Ok(())
    }
}
