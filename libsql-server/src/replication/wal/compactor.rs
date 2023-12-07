#![allow(dead_code)]

use std::num::NonZeroU32;

use libsql_replication::frame::{FrameHeader, FrameBorrowed};
use libsql_sys::ffi::Sqlite3DbHeader;
use libsql_sys::wal::{Sqlite3Wal, CheckpointCallback, Wal, BusyHandler};
use libsql_sys::wal::wrapper::{WrapWal, WrappedWal};
use zerocopy::FromBytes;

use crate::namespace::NamespaceName;
use crate::replication::FrameNo;
use crate::replication::snapshot_store::{SnapshotStore, SnapshotBuilder};

type CompactorWal = WrappedWal<CompactorWrapper, Sqlite3Wal>;

struct CompactorWrapper {
    store: SnapshotStore,
    name: NamespaceName,
}

impl<T: Wal> WrapWal<T> for CompactorWrapper {
    fn checkpoint(
        &mut self,
        wrapped: &mut T,
        db: &mut libsql_sys::wal::Sqlite3Db,
        mode: libsql_sys::wal::CheckpointMode,
        busy_handler: Option<&mut dyn BusyHandler>,
        sync_flags: u32,
        buf: &mut [u8],
        checkpoint_cb: Option<&mut dyn CheckpointCallback>,
    ) -> libsql_sys::wal::Result<(u32, u32)> {
        struct CompactorCallback<'a> {
            inner: Option<&'a mut dyn CheckpointCallback>,
            builder: Option<SnapshotBuilder>,
            base_frame_no: Option<FrameNo>,
        }

        impl<'a> CheckpointCallback for CompactorCallback<'a> {
            fn frame(
                &mut self,
                page: &[u8],
                page_no: NonZeroU32,
                frame_no: NonZeroU32,
            ) -> libsql_sys::wal::Result<()> {
                // We retrive the base_replication_index. The first time this method is being
                // called, it must be with page 1, patched with the current replication index,
                // because we just injected it.
                let base_frame_no = match self.base_frame_no {
                    None => {
                        assert_eq!(page_no.get(), 1);
                        // first frame must be newly injected frame , with the final frame_index
                        let header = Sqlite3DbHeader::read_from_prefix(page).unwrap();
                        let base_frame_no = header.replication_index.get() - frame_no.get() as u64;
                        self.base_frame_no = Some(base_frame_no);
                        base_frame_no
                    }
                    Some(frame_no) => frame_no,

                };
                let absolute_frame_no = base_frame_no + frame_no.get() as u64;
                let frame = FrameBorrowed::from_parts(
                    &FrameHeader {
                        checksum: 0.into(), // TODO!: handle checksum
                        frame_no: absolute_frame_no.into(),
                        page_no: page_no.get().into(),
                        size_after: 0.into(),
                    },
                    page,
                );

                self.builder.as_mut().unwrap().add_frame(&frame).unwrap();

                if let Some(ref mut inner) = self.inner {
                    return inner.frame(page, page_no, frame_no);
                }

                Ok(())
            }

            fn finish(&mut self) -> libsql_sys::wal::Result<()> {
                self.builder.take().unwrap().finish().unwrap();

                if let Some(ref mut inner) = self.inner {
                    return inner.finish();
                }

                Ok(())
            }
        }

        wrapped.begin_read_txn()?;
        let db_size = wrapped.db_size();
        wrapped.end_read_txn();

        let builder = self.store.builder(self.name.clone(), db_size).unwrap();
        let mut cb = CompactorCallback {
            inner: checkpoint_cb,
            builder: Some(builder),
            base_frame_no: None,
        };

        wrapped.checkpoint(db, mode, busy_handler, sync_flags, buf, Some(&mut cb))
    }
}

