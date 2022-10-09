use std::{
    fmt::Debug,
    path::Path,
    sync::{Arc, RwLock},
};

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use tantivy::{
    directory::{error, FileHandle, WatchCallback, WatchHandle, WritePtr},
    Directory,
};

#[derive(Debug)]
pub struct InitialisationError;

#[derive(Clone)]
pub struct TantivySqliteStorage {
    inner: Arc<RwLock<TantivySqliteStorageInner>>,
}

impl Debug for TantivySqliteStorage {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "TantivySqliteStorage")
    }
}

impl TantivySqliteStorage {
    pub fn new(
        connection_pool: Pool<SqliteConnectionManager>,
    ) -> Result<Self, InitialisationError> {
        Ok(Self {
            inner: Arc::new(RwLock::new(TantivySqliteStorageInner::new(
                connection_pool,
            )?)),
        })
    }
}

impl Directory for TantivySqliteStorage {
    fn get_file_handle(&self, path: &Path) -> Result<Box<dyn FileHandle>, error::OpenReadError> {
        todo!()
    }

    fn delete(&self, path: &Path) -> Result<(), error::DeleteError> {
        todo!()
    }

    fn exists(&self, path: &Path) -> Result<bool, error::OpenReadError> {
        todo!()
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, error::OpenWriteError> {
        todo!()
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, error::OpenReadError> {
        todo!()
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        todo!()
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        todo!()
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        todo!()
    }
}

struct TantivySqliteStorageInner {
    connection_pool: Pool<SqliteConnectionManager>,
}

const INIT_SQL: &str = "
BEGIN;
CREATE TABLE IF NOT EXISTS tantivy_blobs (filename TEXT UNIQUE NOT NULL, content BLOB NOT NULL);
COMMIT;
";

impl TantivySqliteStorageInner {
    pub fn new(
        connection_pool: Pool<SqliteConnectionManager>,
    ) -> Result<Self, InitialisationError> {
        let ret = Self { connection_pool };

        ret.init()?;
        Ok(ret)
    }

    fn init(&self) -> Result<(), InitialisationError> {
        let conn = self
            .connection_pool
            .get()
            .map_err(|_| InitialisationError)?;

        conn.execute_batch(INIT_SQL)
            .map_err(|_| InitialisationError)?;
        Ok(())
    }
}

#[cfg(test)]
mod test {
    use super::*;
    use uuid::Uuid;

    fn create_in_memory_database_string() -> String {
        format!("file:{}?mode=memory&cache=shared", Uuid::new_v4())
    }

    fn in_memory_connection_manager() -> SqliteConnectionManager {
        // see https://github.com/ivanceras/r2d2-sqlite/pull/45
        SqliteConnectionManager::file(create_in_memory_database_string())
    }

    #[test]
    fn can_successfully_init() -> Result<(), InitialisationError> {
        let manager = in_memory_connection_manager();

        let pool = Pool::builder().max_size(4).build(manager).unwrap();

        let _storage = TantivySqliteStorage::new(pool)?;

        Ok(())
    }
}
