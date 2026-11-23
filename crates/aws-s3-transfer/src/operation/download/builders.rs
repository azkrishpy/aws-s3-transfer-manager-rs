/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */
use std::sync::Arc;

use crate::telemetry::TARGET_TRANSFER_DOWNLOAD;

#[cfg(any(unix, windows, target_os = "wasi"))]
use super::ManagedDownloadHandle;
use super::{DownloadHandle, DownloadInputBuilder};

/// Fluent builder for constructing a single object download transfer.
#[derive(Debug)]
pub struct DownloadFluentBuilder {
    handle: Arc<crate::client::Handle>,
    pub(crate) inner: DownloadInputBuilder,
}

impl DownloadFluentBuilder {
    pub(crate) fn new(handle: Arc<crate::client::Handle>) -> Self {
        Self {
            handle,
            inner: ::std::default::Default::default(),
        }
    }

    /// Initiate a download transfer for a single object.
    #[tracing::instrument(skip_all, level = "debug", target = TARGET_TRANSFER_DOWNLOAD, name = "initiate-download", fields(
        bucket = self.inner.get_bucket().as_deref().unwrap_or_default(),
        key = self.inner.get_key().as_deref().unwrap_or_default(),
    ))]
    pub fn initiate(self) -> Result<DownloadHandle, crate::error::Error> {
        let input = self.inner.build()?;
        crate::operation::download::Download::orchestrate(self.handle, input, false)
    }

    /// Download the object and write it to the given file path.
    ///
    /// Data is written to a temporary file in the same directory, then
    /// atomically renamed to the destination on success. The temporary file
    /// is deleted on failure, cancellation, or drop. The destination has the
    /// exact downloaded length.
    ///
    /// A file already at `path` is replaced when the download succeeds. It is
    /// unchanged until then, and left unchanged when the download fails or is
    /// cancelled. Keeping it costs throughput when a large file is overwritten
    /// repeatedly: its unwritten data stays in the page cache while the new file
    /// is written. [`remove_and_write_to_path`](Self::remove_and_write_to_path)
    /// removes it first instead.
    ///
    /// Successful completion means every byte was accepted by the operating
    /// system and the temporary file was renamed. It does not call `sync_data`,
    /// `sync_all`, or synchronize the parent directory, and therefore does not
    /// promise persistence across a system crash or power loss.
    #[cfg(any(unix, windows, target_os = "wasi"))]
    pub async fn write_to_path(
        self,
        path: impl Into<std::path::PathBuf>,
    ) -> Result<ManagedDownloadHandle, crate::error::Error> {
        let input = self.inner.build()?;
        crate::operation::download::Download::orchestrate_to_path(
            self.handle,
            input,
            path.into(),
            None,
            &crate::operation::download::sink::FileSinkFactory,
        )
        .await
    }

    /// Remove the file at `path`, if any, then download the object and write it
    /// to `path` as [`write_to_path`](Self::write_to_path) does.
    ///
    /// The existing file is removed before any request is sent, so a failed or
    /// cancelled download leaves no file at `path`. Use this when the previous
    /// contents need not survive a failure. Removal unlinks the directory entry
    /// at `path`, as the final rename replaces it: a symbolic link is removed,
    /// not the file it points to. A directory at `path` is not removed, and
    /// fails with [`ErrorKind::IOError`](crate::error::ErrorKind::IOError).
    #[cfg(any(unix, windows, target_os = "wasi"))]
    pub async fn remove_and_write_to_path(
        self,
        path: impl Into<std::path::PathBuf>,
    ) -> Result<ManagedDownloadHandle, crate::error::Error> {
        let input = self.inner.build()?;
        let path = path.into();
        match crate::io::fs::remove_file_async(&path).await {
            Err(e) if e.kind() != std::io::ErrorKind::NotFound => {
                return Err(crate::error::io_at("removing", &path, e));
            }
            _ => {}
        }
        crate::operation::download::Download::orchestrate_to_path(
            self.handle,
            input,
            path,
            None,
            &crate::operation::download::sink::FileSinkFactory,
        )
        .await
    }

    /// Download the object and write it to the given open file.
    ///
    /// The caller is responsible for the file lifecycle (creation, cleanup).
    /// The transfer manager writes to the file using positioned writes at
    /// offsets starting from 0 and resizes it to the exact downloaded length,
    /// removing any previous tail. The file cursor is ignored. Append and
    /// nonzero destination offsets are not supported by this operation.
    ///
    /// A file that is open in append mode when this method is called, such as
    /// one opened with [`OpenOptions::append`](std::fs::OpenOptions::append),
    /// is rejected with
    /// [`ErrorKind::InputInvalid`](crate::error::ErrorKind::InputInvalid) on
    /// every platform. No request is sent, and the file is not modified.
    ///
    /// On failure or cancellation, the file may contain a noncontiguous mixture
    /// of downloaded and previous data, and positioned writes may have extended
    /// it. Treat its contents as invalid unless [`ManagedDownloadHandle::join`]
    /// succeeds.
    ///
    /// Successful completion does not synchronize file data or metadata. Call
    /// the appropriate synchronization method on another handle when crash
    /// persistence is required.
    #[cfg(any(unix, windows, target_os = "wasi"))]
    pub fn write_to_file(
        self,
        file: std::fs::File,
    ) -> Result<ManagedDownloadHandle, crate::error::Error> {
        let input = self.inner.build()?;
        crate::operation::download::Download::orchestrate_to_file(
            self.handle,
            input,
            file,
            &crate::operation::download::sink::FileSinkFactory,
        )
    }
}

impl DownloadInputBuilder {
    /// Initiate a download transfer for a single object with this input using the given client.
    pub fn initiate_with(
        self,
        client: &crate::Client,
    ) -> Result<DownloadHandle, crate::error::Error> {
        let mut fluent_builder = client.download();
        fluent_builder.inner = self;
        fluent_builder.initiate()
    }
}
