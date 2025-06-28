use redis::Commands;
use std::cell::RefCell;
use std::rc::Rc;

pub struct RedisStorage {
    connection: Rc<RefCell<redis::Connection>>,
}

impl RedisStorage {
    pub fn new(connection: redis::Connection) -> Self {
        RedisStorage {
            connection: Rc::new(RefCell::new(connection)),
        }
    }

    pub fn connect(&self) -> Result<(), String> {
        // No need to borrow mutably here
        Ok(())
    }

    pub fn insert(&self, key: &str, value: &str) -> Result<(), String> {
        let mut con = self.connection.borrow_mut();
        con.set(key, value).map_err(|e| e.to_string())
    }

    pub fn retrieve(&self, key: &str) -> Result<Option<String>, String> {
        let mut con = self.connection.borrow_mut();
        con.get(key).map_err(|e| e.to_string())
    }

    pub fn delete(&self, key: &str) -> Result<(), String> {
        let mut con = self.connection.borrow_mut();
        con.del(key).map_err(|e| e.to_string())
    }
}

