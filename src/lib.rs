//! An implementation of a storage layer for tantivy which builds on sqlite.
//!
//! Allows you to put all your search index data into sqlite making it so that
//! all the data is in one file rather than tantivy's default behaviour which is
//! to spread it out over multiple places.
//!
//! Has a few disadvantages though. Because of sqlite's threading, it isn't possible
//! to io partial reads while allowing concurrent writes. So a lot of operations
//! are ultimately serialised.
//!
//! All the data is stored in a table called `tantivy_blobs`. You should not interact
//! with this table directly, and instead let tantivy manage that for you.
//!
//! # Example
//!
//! You can use the library as follows:
//!
//! ```
//! use tantivy::{Index, schema::Schema};
//!
//! use r2d2::Pool;
//! use r2d2_sqlite::SqliteConnectionManager;
//!
//! use tantivy_sqlite_storage::TantivySqliteStorage;
//!
//! // Note that you have to use file here for in-memory, but you'll probably point this to a real sqlite database
//! // if you were actually using it.
//! # fn main() -> Result<(), Box<dyn std::error::Error>> {
//! let connection_manager = SqliteConnectionManager::file("file:tantivy-shared-file?mode=memory&cache=shared");
//! let pool = Pool::builder().max_size(4).build(connection_manager)?;
//! let storage = TantivySqliteStorage::new(pool)?;
//!
//! // build your schema
//! # let mut schema_builder = Schema::builder();
//! # let schema = schema_builder.build();
//! let index = Index::open_or_create(storage, schema.clone())?;
//! # Ok(())
//! # }
//! ```
#![deny(
    missing_docs,
    missing_debug_implementations,
    unreachable_pub,
    rustdoc::broken_intra_doc_links
)]
#![warn(rust_2018_idioms)]

use std::{
    fmt::Debug,
    io::{BufWriter, Cursor, Write},
    ops::Range,
    os::unix::prelude::OsStrExt,
    path::{Path, PathBuf},
    sync::Arc,
};

use r2d2::Pool;
use r2d2_sqlite::SqliteConnectionManager;

use rusqlite::{DatabaseName, OptionalExtension};

use tantivy::{
    directory::{
        error, FileHandle, OwnedBytes, TerminatingWrite, WatchCallback, WatchCallbackList,
        WatchHandle, WritePtr,
    },
    Directory, HasLen,
};

use thiserror::Error;

use parking_lot::RwLock;

/// The possible errors produced by this library.
#[derive(Error, Debug)]
pub enum TantivySqliteStorageError {
    /// An error directly from rusqlite
    #[error("Sqlite command failed")]
    Sqlite(#[from] rusqlite::Error),
    /// An error directly from r2d2
    #[error("r2d2 pool error")]
    Pool(#[from] r2d2::Error),
    /// A requested file doesn't exist
    #[error("File does not exist")]
    FileDoesNotExist(PathBuf),
    /// File already exists
    #[error("File already exists")]
    FileAlreadyExists(PathBuf),
}

impl From<TantivySqliteStorageError> for std::io::Error {
    fn from(e: TantivySqliteStorageError) -> Self {
        std::io::Error::new(std::io::ErrorKind::Other, e)
    }
}

impl TantivySqliteStorageError {
    fn into_open_read_error(self, path: &Path) -> error::OpenReadError {
        match self {
            TantivySqliteStorageError::FileDoesNotExist(path) => {
                error::OpenReadError::FileDoesNotExist(path)
            }
            _ => error::OpenReadError::IoError {
                io_error: self.into(),
                filepath: path.to_path_buf(),
            },
        }
    }

    fn into_delete_error(self, path: &Path) -> error::DeleteError {
        match self {
            TantivySqliteStorageError::FileDoesNotExist(path) => {
                error::DeleteError::FileDoesNotExist(path)
            }
            _ => error::DeleteError::IoError {
                io_error: self.into(),
                filepath: path.to_path_buf(),
            },
        }
    }

    fn into_open_write_error(self, path: &Path) -> error::OpenWriteError {
        error::OpenWriteError::IoError {
            io_error: self.into(),
            filepath: path.to_path_buf(),
        }
    }
}

/// The main struct of this crate. This is an implementation of [`tantivy::Directory`].
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
    /// Creates a new storage.
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
        let handle_data = self
            .inner
            .read()
            .read_handle(path)
            .map_err(|e| e.into_open_read_error(path))?;
        let handle = ReadHandle {
            data: handle_data,
            conn: self.inner.clone(),
        };

        Ok(Box::new(handle))
    }

    fn delete(&self, path: &Path) -> Result<(), error::DeleteError> {
        self.inner
            .write()
            .delete(path)
            .map_err(|e| e.into_delete_error(path))
    }

    fn exists(&self, path: &Path) -> Result<bool, error::OpenReadError> {
        self.inner
            .read()
            .exists(path)
            .map_err(|e| error::OpenReadError::IoError {
                io_error: e.into(),
                filepath: path.to_path_buf(),
            })
    }

    fn open_write(&self, path: &Path) -> Result<WritePtr, error::OpenWriteError> {
        self.inner
            .write()
            .create_empty_file(path)
            .map_err(|e| e.into_open_write_error(path))?;

        Ok(BufWriter::new(Box::new(TantivySqliteStorageWritePtr::new(
            path,
            self.clone(),
        ))))
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, error::OpenReadError> {
        self.inner
            .read()
            .atomic_read(path)
            .map_err(|e| e.into_open_read_error(path))
    }

    fn atomic_write(&self, path: &Path, data: &[u8]) -> std::io::Result<()> {
        self.inner
            .write()
            .atomic_write(path, data)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))
    }

    fn sync_directory(&self) -> std::io::Result<()> {
        Ok(())
    }

    fn watch(&self, watch_callback: WatchCallback) -> tantivy::Result<WatchHandle> {
        Ok(self.inner.read().watch(watch_callback))
    }
}

struct TantivySqliteStorageInner {
    connection_pool: Pool<SqliteConnectionManager>,
    watch_callback_list: WatchCallbackList,
}

impl TantivySqliteStorageInner {
    fn new(
        connection_pool: Pool<SqliteConnectionManager>,
    ) -> Result<Self, TantivySqliteStorageError> {
        let ret = Self {
            connection_pool,
            watch_callback_list: Default::default(),
        };

        ret.init()?;
        Ok(ret)
    }

    fn watch(&self, watch_callback: WatchCallback) -> WatchHandle {
        self.watch_callback_list.subscribe(watch_callback)
    }

    fn exists(&self, path: &Path) -> Result<bool, TantivySqliteStorageError> {
        let conn = self.connection_pool.get()?;

        let exists: Option<i32> = conn
            .query_row(
                "SELECT 1 FROM tantivy_blobs WHERE filename = ?",
                [path.as_os_str().as_bytes()],
                |row| row.get(0),
            )
            .optional()?;

        Ok(exists.is_some())
    }

    fn delete(&mut self, path: &Path) -> Result<(), TantivySqliteStorageError> {
        let conn = self.connection_pool.get()?;

        let num_deleted = conn.execute(
            "DELETE FROM tantivy_blobs WHERE filename = ?",
            [path.as_os_str().as_bytes()],
        )?;

        if num_deleted == 0 {
            return Err(TantivySqliteStorageError::FileDoesNotExist(
                path.to_path_buf(),
            ));
        }

        Ok(())
    }

    fn create_empty_file(&mut self, path: &Path) -> Result<(), TantivySqliteStorageError> {
        let conn = self.connection_pool.get()?;

        let num_rows_modified = conn.execute(
            "INSERT OR IGNORE INTO tantivy_blobs VALUES (?, ?)",
            [path.as_os_str().as_bytes(), b""],
        )?;

        if num_rows_modified != 1 {
            Err(TantivySqliteStorageError::FileAlreadyExists(
                path.to_path_buf(),
            ))
        } else {
            Ok(())
        }
    }

    fn atomic_write(&mut self, path: &Path, data: &[u8]) -> Result<(), TantivySqliteStorageError> {
        let conn = self.connection_pool.get()?;

        conn.execute(
            "INSERT OR REPLACE INTO tantivy_blobs VALUES (?, ?)",
            [path.as_os_str().as_bytes(), data],
        )?;

        if path == Path::new("meta.json") {
            self.watch_callback_list.broadcast();
        }

        Ok(())
    }

    fn atomic_read(&self, path: &Path) -> Result<Vec<u8>, TantivySqliteStorageError> {
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

    fn read_handle(&self, path: &Path) -> Result<ReadHandleData, TantivySqliteStorageError> {
        let conn = self.connection_pool.get()?;

        let handle_data = conn
            .query_row(
                "SELECT rowid, length(content) FROM tantivy_blobs WHERE filename = ?",
                [path.as_os_str().as_bytes()],
                |row| Ok((row.get(0)?, row.get(1)?)),
            )
            .optional()?;

        handle_data
            .map(|(rowid, length)| ReadHandleData { rowid, length })
            .ok_or_else(|| TantivySqliteStorageError::FileDoesNotExist(path.to_path_buf()))
    }

    fn read_bytes(
        &self,
        rowid: i64,
        range: Range<usize>,
    ) -> Result<OwnedBytes, TantivySqliteStorageError> {
        let conn = self.connection_pool.get()?;

        let blob = conn.blob_open(DatabaseName::Main, "tantivy_blobs", "content", rowid, true)?;

        let mut buf = vec![0; range.len()];

        blob.read_at_exact(&mut buf, range.start)?;
        Ok(OwnedBytes::new(buf))
    }

    fn init(&self) -> Result<(), TantivySqliteStorageError> {
        let conn = self.connection_pool.get()?;

        conn.execute("CREATE TABLE IF NOT EXISTS tantivy_blobs (filename TEXT UNIQUE NOT NULL, content BLOB NOT NULL)", [])?;
        Ok(())
    }
}

struct ReadHandleData {
    rowid: i64,
    length: usize,
}

struct ReadHandle {
    data: ReadHandleData,
    conn: Arc<RwLock<TantivySqliteStorageInner>>,
}

impl std::fmt::Debug for ReadHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "ReadHandle({})", self.data.rowid)
    }
}

impl HasLen for ReadHandle {
    fn len(&self) -> usize {
        self.data.length
    }
}

impl FileHandle for ReadHandle {
    fn read_bytes(&self, range: Range<usize>) -> std::io::Result<OwnedBytes> {
        Ok(self.conn.read().read_bytes(self.data.rowid, range)?)
    }
}

struct TantivySqliteStorageWritePtr {
    data: Cursor<Vec<u8>>,
    path: PathBuf,
    storage: TantivySqliteStorage,
}

impl TantivySqliteStorageWritePtr {
    fn new(path: &Path, storage: TantivySqliteStorage) -> Self {
        Self {
            data: Cursor::new(Vec::new()),
            path: path.to_path_buf(),
            storage,
        }
    }
}

impl Write for TantivySqliteStorageWritePtr {
    fn write(&mut self, buf: &[u8]) -> std::io::Result<usize> {
        self.data.write(buf)
    }

    fn flush(&mut self) -> std::io::Result<()> {
        self.storage.atomic_write(&self.path, self.data.get_ref())
    }
}

impl TerminatingWrite for TantivySqliteStorageWritePtr {
    fn terminate_ref(&mut self, _: tantivy::directory::AntiCallToken) -> std::io::Result<()> {
        self.flush()
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
    fn can_successfully_init() -> Result<(), Box<dyn std::error::Error>> {
        let manager = in_memory_connection_manager();

        let pool = Pool::builder().max_size(4).build(manager)?;

        let _storage = TantivySqliteStorage::new(pool)?;

        Ok(())
    }

    #[test]
    fn can_read_and_write() -> Result<(), Box<dyn std::error::Error>> {
        let manager = in_memory_connection_manager();
        let pool = Pool::builder().max_size(4).build(manager)?;

        let storage = TantivySqliteStorage::new(pool)?;

        let data = b"hello, world!";
        let path = Path::new("some/file/path.txt");
        storage.atomic_write(path, data)?;

        let content = storage.atomic_read(path)?;

        assert_eq!(content, data);

        Ok(())
    }

    #[test]
    fn returns_error_if_file_does_not_exist() -> Result<(), Box<dyn std::error::Error>> {
        let manager = in_memory_connection_manager();
        let pool = Pool::builder().max_size(4).build(manager)?;

        let storage = TantivySqliteStorage::new(pool)?;

        let path = Path::new("some/file/path.txt");
        let error = storage.atomic_read(path).unwrap_err();

        assert!(matches!(error, error::OpenReadError::FileDoesNotExist(_)));

        Ok(())
    }

    #[test]
    fn error_to_delete_empty_files() -> Result<(), Box<dyn std::error::Error>> {
        let manager = in_memory_connection_manager();
        let pool = Pool::builder().max_size(4).build(manager)?;

        let storage = TantivySqliteStorage::new(pool)?;

        let path = Path::new("some/file/path.txt");
        let error = storage.delete(path).unwrap_err();

        assert!(matches!(error, error::DeleteError::FileDoesNotExist(_)));

        Ok(())
    }

    #[test]
    fn can_delete_files() -> Result<(), Box<dyn std::error::Error>> {
        let manager = in_memory_connection_manager();
        let pool = Pool::builder().max_size(4).build(manager)?;

        let storage = TantivySqliteStorage::new(pool)?;

        let path = Path::new("some/file/path.txt");
        let data = b"hello, world!";
        storage.atomic_write(path, data)?;

        storage.delete(path)?;

        let error = storage.atomic_read(path).unwrap_err();
        assert!(matches!(error, error::OpenReadError::FileDoesNotExist(_)));

        Ok(())
    }

    #[test]
    fn can_check_if_file_exists() -> Result<(), Box<dyn std::error::Error>> {
        let manager = in_memory_connection_manager();
        let pool = Pool::builder().max_size(4).build(manager)?;

        let storage = TantivySqliteStorage::new(pool)?;

        let path = Path::new("some/file/path.txt");

        assert!(!storage.exists(path)?);

        let data = b"hello, world!";
        storage.atomic_write(path, data)?;

        assert!(storage.exists(path)?);

        storage.delete(path)?;
        assert!(!storage.exists(path)?);

        Ok(())
    }

    #[test]
    fn can_create_file_handles() -> Result<(), Box<dyn std::error::Error>> {
        let manager = in_memory_connection_manager();
        let pool = Pool::builder().max_size(4).build(manager)?;

        let storage = TantivySqliteStorage::new(pool)?;

        let data = b"hello, world!";
        let path = Path::new("some/file/path.txt");
        storage.atomic_write(path, data)?;

        let file_handle = storage.get_file_handle(path)?;

        let content = file_handle.read_bytes(3..7)?;

        assert_eq!(&*content, b"lo, ");
        assert_eq!(file_handle.len(), 13);

        Ok(())
    }

    #[test]
    fn creates_empty_file_if_writing() -> Result<(), Box<dyn std::error::Error>> {
        let manager = in_memory_connection_manager();
        let pool = Pool::builder().max_size(4).build(manager)?;

        let storage = TantivySqliteStorage::new(pool)?;

        let data = b"hello, world!";
        let path = Path::new("some/file/path.txt");

        let mut write_ptr = storage.open_write(path)?;

        assert!(storage.exists(path)?);

        write_ptr.write_all(data)?;
        write_ptr.terminate()?;

        let content = storage.atomic_read(path)?;

        assert_eq!(&content, data);

        Ok(())
    }
}
