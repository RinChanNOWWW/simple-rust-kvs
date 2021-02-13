use std::collections::HashMap;

pub struct KvStore {
    storage: HashMap<String, String>
}

impl KvStore {
    pub fn new() -> Self {
        return KvStore{
            storage: HashMap::new()
        }
    }

    pub fn store() {

    }

    pub fn set(&mut self, key: String, value: String) {
        self.storage.insert(key, value);
    }

    pub fn get(&self, key: String) -> Option<String> {
        return self.storage.get(&key).cloned();
    }

    pub fn remove(&mut self, key: String) {
        self.storage.remove(&key);
    }
}