use std::{
    fmt::Debug,
    os::unix::prelude::OsStrExt,
    path::Path,
    sync::{Arc, RwLock},
};

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use tantivy::{
    directory::{error, FileHandle, WatchCallback, WatchCallbackList, WatchHandle, WritePtr},
    Directory,
};

use thiserror::Error;

#[derive(Error, Debug)]
pub enum TantivySqliteStorageError {
    #[error("Sqlite command failed")]
    Sqlite(#[from] rusqlite::Error),
    #[error("r2d2 pool error")]
    Pool(#[from] r2d2::Error),
}

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
    ) -> Result<Self, TantivySqliteStorageError> {
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
        self.inner
            .write()
            .unwrap()
            .atomic_write(path, data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(()) // managed by sqlite
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(self.inner.read().unwrap().watch(watch_callback))
    }
}

struct TantivySqliteStorageInner {
    connection_pool: Pool<SqliteConnectionManager>,
    watch_callback_list: WatchCallbackList,
}

const INIT_SQL: &str = "
BEGIN;
CREATE TABLE IF NOT EXISTS tantivy_blobs (filename TEXT UNIQUE NOT NULL, content BLOB NOT NULL);
COMMIT;
";

impl TantivySqliteStorageInner {
    pub fn new(
        connection_pool: Pool<SqliteConnectionManager>,
    ) -> Result<Self, TantivySqliteStorageError> {
        let ret = Self {
            connection_pool,
            watch_callback_list: Default::default(),
        };

        ret.init()?;
        Ok(ret)
    }

    pub fn watch(&self, watch_callback: WatchCallback) -> WatchHandle {
        self.watch_callback_list.subscribe(watch_callback)
    }

    pub fn atomic_write(
        &mut self,
        path: &Path,
        data: &[u8],
    ) -> Result<(), TantivySqliteStorageError> {
        let conn = self.connection_pool.get()?;

        conn.execute(
            "INSERT OR REPLACE INTO tantivy_blobs VALUES (?, ?)",
            [path.as_os_str().as_bytes(), data],
        )?;

        if path.ends_with("meta.json") {
            self.watch_callback_list.broadcast();
        }

        Ok(())
    }

    fn init(&self) -> Result<(), TantivySqliteStorageError> {
        let conn = self.connection_pool.get()?;

        conn.execute_batch(INIT_SQL)?;
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
    fn can_successfully_init() -> Result<(), TantivySqliteStorageError> {
        let manager = in_memory_connection_manager();

        let pool = Pool::builder().max_size(4).build(manager).unwrap();

        let _storage = TantivySqliteStorage::new(pool)?;

        Ok(())
    }
}
