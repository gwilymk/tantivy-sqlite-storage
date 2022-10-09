use std::{
    fmt::Debug,
    os::unix::prelude::OsStrExt,
    path::{Path, PathBuf},
    sync::{Arc, RwLock},
};

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use rusqlite::OptionalExtension;
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
    #[error("File does not exist")]
    FileDoesNotExist(PathBuf),
}

impl From<TantivySqliteStorageError> for std::io::Error {
    fn from(e: TantivySqliteStorageError) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, e)
    }
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
        self.inner
            .read()
            .unwrap()
            .atomic_read(path)
            .map_err(|e| match e {
                TantivySqliteStorageError::FileDoesNotExist(path) => {
                    error::OpenReadError::FileDoesNotExist(path)
                }
                _ => error::OpenReadError::IoError {
                    io_error: e.into(),
                    filepath: path.to_path_buf(),
                },
            })
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

    pub fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, TantivySqliteStorageError> {
        let conn = self.connection_pool.get()?;

        let content = conn
            .query_row(
                "SELECT content FROM tantivy_blobs WHERE filename = ?",
                [path.as_os_str().as_bytes()],
                |row| row.get(0),
            )
            .optional()?;

        content.ok_or_else(|| TantivySqliteStorageError::FileDoesNotExist(path.to_path_buf()))
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

        let pool = Pool::builder().max_size(4).build(manager)?;

        let _storage = TantivySqliteStorage::new(pool)?;

        Ok(())
    }

    #[test]
    fn can_read_and_write() -> Result<(), TantivySqliteStorageError> {
        let manager = in_memory_connection_manager();
        let pool = Pool::builder().max_size(4).build(manager)?;

        let storage = TantivySqliteStorage::new(pool)?;

        let data = b"hello, world!";
        let path = Path::new("some/file/path.txt");
        storage.atomic_write(path, data).unwrap();

        let content = storage.atomic_read(path).unwrap();

        assert_eq!(content, data);

        Ok(())
    }

    #[test]
    fn returns_error_if_file_does_not_exist() -> Result<(), TantivySqliteStorageError> {
        let manager = in_memory_connection_manager();
        let pool = Pool::builder().max_size(4).build(manager)?;

        let storage = TantivySqliteStorage::new(pool)?;

        let path = Path::new("some/file/path.txt");
        let error = storage.atomic_read(path).unwrap_err();

        assert!(matches!(error, error::OpenReadError::FileDoesNotExist(_)));

        Ok(())
    }
}
