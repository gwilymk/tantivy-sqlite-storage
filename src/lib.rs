use std::path::Path;

use tantivy::{
    directory::{error, FileHandle, WatchCallback, WatchHandle, WritePtr},
    Directory,
};

#[derive(Debug, Clone)]
pub struct TantivySqliteStorage {}

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
