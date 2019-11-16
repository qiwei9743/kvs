#![deny(missing_docs)]

//! This project is only for pingcap project1.
//! Have fun.

use std::collections::HashMap;
/// core data structure for kvs store
#[derive(Default)]
pub struct KvStore(HashMap<String, String>);

impl KvStore {
    /// create a new object for KvStore.
    pub fn new() -> Self {
        Default::default()
    }

    /// get a value with a given key.
    pub fn get(&self, key: String) -> Option<String> {
        self.0.get(&key).map(|x| x.to_string())
    }

    /// set a key/value pairs
    pub fn set(&mut self, key: String, value: String) {
        self.0.insert(key, value);
    }

    /// remove a key/value pairs by a given key.
    pub fn remove(&mut self, key: String) {
        self.0.remove(&key);
    }
}

#[cfg(test)]
mod tests {
    #[test]
    fn it_works() {
        assert_eq!(2 + 2, 4);
    }
}
