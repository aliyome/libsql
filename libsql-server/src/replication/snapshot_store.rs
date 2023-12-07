#![allow(dead_code)]

use std::io::{Write, Seek};
use std::path::{PathBuf, Path};
use std::sync::Arc;

use libsql_replication::frame::FrameBorrowed;
use libsql_replication::snapshot::SnapshotFileHeader;

use tempfile::NamedTempFile;
use zerocopy::{FromZeroes, AsBytes};

use crate::namespace::NamespaceName;

use super::FrameNo;


pub struct SnapshotStore {
    inner: Arc<SnapshotStoreInner>,
}

struct SnapshotStoreInner {
    /// path to the temporary directory
    snapshots_path: PathBuf,
    temp_path: PathBuf,
}

impl SnapshotStoreInner {
    fn snapshots_path(&self) -> &Path {
        &self.snapshots_path
    }
}

impl SnapshotStore {
    pub async fn new(db_path: &Path) -> crate::Result<Self> {
        let snapshots_path = db_path.join("snapshots");
        tokio::fs::create_dir_all(&snapshots_path).await?;

        let temp_path = db_path.join("tmp");
        tokio::fs::create_dir_all(&temp_path).await?;

        let inner = Arc::new(SnapshotStoreInner { snapshots_path, temp_path});

        Ok(Self { inner })
    }

    pub fn builder(&self, namespace_name: NamespaceName, db_size: u32) -> crate::Result<SnapshotBuilder> {
        let mut snapshot_file = NamedTempFile::new_in(&self.inner.temp_path)?;

        snapshot_file.write_all(SnapshotFileHeader::new_zeroed().as_bytes()).unwrap();

        Ok(SnapshotBuilder {
            snapshot_file,
            store: self.inner.clone(),
            frame_count: 0,
            end_frame_no: 0,
            last_seen_frame_no: None,
            db_size,
            name: namespace_name,
        })
    }
}

pub struct SnapshotBuilder {
    /// Temporary file to hold the snapshot, before it's persisted with as self.name
    snapshot_file: NamedTempFile,
    store: Arc<SnapshotStoreInner>,
    frame_count: u64,
    end_frame_no: FrameNo,
    last_seen_frame_no: Option<FrameNo>,
    db_size: u32,
    name: NamespaceName,
}

impl SnapshotBuilder {
    pub fn add_frame(&mut self, frame: &FrameBorrowed) -> crate::Result<()> {
        let frame_no = frame.header().frame_no.get();
        match self.last_seen_frame_no {
            Some(last_seen) => {
                assert!(last_seen > frame_no);
            }
            None => {
                self.end_frame_no = frame_no;
            }
        }

        self.last_seen_frame_no = Some(frame_no);
        self.frame_count += 1;

        self.snapshot_file.write_all(frame.as_bytes()).unwrap();

        Ok(())
    }

    pub fn finish(mut self) -> crate::Result<()> {
        self.snapshot_file.seek(std::io::SeekFrom::Start(0)).unwrap();
        // TODO handle error.
        let start_frame_no = self.last_seen_frame_no.unwrap();
        let end_frame_no = self.end_frame_no;
        let header = SnapshotFileHeader {
            log_id: 0.into(),
            start_frame_no: end_frame_no.into(),
            end_frame_no: end_frame_no.into(),
            frame_count: self.frame_count.into(),
            size_after: self.db_size.into(),
            _pad: Default::default(),
        };
        self.snapshot_file.write_all(header.as_bytes()).unwrap();
        self.snapshot_file.flush().unwrap();
        // todo: can't use name, since it's not formatted correctly. use hash instead, or id.
        let snapshot_name = format!("{}-{start_frame_no}-{end_frame_no}.snap", self.name);
        self.snapshot_file.persist(self.store.snapshots_path().join(snapshot_name)).unwrap();

        Ok(())
    }
}

