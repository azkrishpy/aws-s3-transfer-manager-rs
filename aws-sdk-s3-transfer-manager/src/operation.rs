/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{fs::Metadata, path::Path, sync::Arc};

use aws_runtime::sdk_feature::AwsSdkFeature;
use aws_sdk_s3::client::customize::CustomizableOperation;

use crate::config::user_agent::OperationFeatures;
use crate::error;
use crate::transfer::{StateMachineTerminalReceiver, TransferContext};

/// Types for single object upload operation
pub mod upload;

/// Types for single object download operation
pub mod download;

/// Types for multiple object download operation
pub mod download_objects;

/// Types for multiple object upload operation
pub mod upload_objects;

// The default delimiter of the S3 object key
pub(crate) const DEFAULT_DELIMITER: &str = "/";

/// Conservative per-transfer backstop on concurrently-materialized child
/// transfers (shared by upload_objects and download_objects).
///
/// The scheduler's hierarchical fair-share drives throughput and the walker
/// provides natural rate-limiting, so this cap primarily bounds working-set
/// memory. 512 covers the highest useful concurrency observed in practice
/// (100 Gbps links sustain ~200-250 concurrent transfers; 600 Gbps multi-NIC
/// configurations stay below 500). Callers that genuinely need more can
/// override via `max_concurrent_downloads` / `max_concurrent_uploads`.
pub(crate) const DEFAULT_MAX_CONCURRENT_CHILDREN: usize = 512;

/// One transfer manager operation: its lifecycle, and what its S3 requests share.
pub(crate) struct OperationContext {
    transfer: TransferContext,
    kind: OperationKind,
    /// Operation features only.
    listing: aws_sdk_s3::config::Builder,
    /// `listing` plus the bucket's retry partition.
    control: aws_sdk_s3::config::Builder,
    /// `control` plus the tightened stalled-stream grace.
    body: aws_sdk_s3::config::Builder,
}

#[derive(Clone, Copy, Debug)]
pub(crate) enum OperationKind {
    Upload { from_file: bool },
    Download { to_file: bool },
    UploadObjects,
    DownloadObjects,
}

impl OperationContext {
    /// A top-level operation. Its requests share `bucket`'s retry partition, or
    /// the client's default one when `bucket` is `None`.
    pub(crate) fn new(
        handle: Arc<crate::client::Handle>,
        kind: OperationKind,
        bucket: Option<&str>,
    ) -> (Self, StateMachineTerminalReceiver) {
        let (transfer, completion_rx) = TransferContext::new(handle);
        (
            Self::with_transfer(transfer, kind, None, bucket),
            completion_rx,
        )
    }

    /// An operation linked to `self` as its parent: cancelling `self` cancels it,
    /// and its terminal transition wakes `self`.
    pub(crate) fn child(
        &self,
        kind: OperationKind,
        bucket: Option<&str>,
    ) -> (Self, StateMachineTerminalReceiver) {
        let (transfer, completion_rx) =
            TransferContext::new_child(self.transfer.handle.clone(), self.transfer.id.id);
        (
            Self::with_transfer(transfer, kind, Some(self.kind), bucket),
            completion_rx,
        )
    }

    fn with_transfer(
        transfer: TransferContext,
        kind: OperationKind,
        parent: Option<OperationKind>,
        bucket: Option<&str>,
    ) -> Self {
        let listing =
            aws_sdk_s3::config::Builder::default().interceptor(operation_features(kind, parent));
        let mut control = listing.clone();
        if let Some(bucket) = bucket {
            control = control.retry_partition(transfer.handle.retry_partition(bucket));
        }
        let body = control
            .clone()
            .stalled_stream_protection(crate::retry::tightened_ssp());
        Self {
            transfer,
            kind,
            listing,
            control,
            body,
        }
    }

    pub(crate) fn transfer(&self) -> &TransferContext {
        &self.transfer
    }

    /// The override for one request of this operation.
    ///
    /// A listing stays on the client's default retry partition, and only a request that
    /// streams an object body tightens stalled-stream protection. A discovery GET keeps the
    /// client's grace; a re-issue of its range gets it.
    pub(crate) fn config_override(&self, request: S3Request) -> aws_sdk_s3::config::Builder {
        match request {
            S3Request::ListObjectsV2 => self.listing.clone(),
            S3Request::HeadObject
            | S3Request::GetObjectDiscovery
            | S3Request::CreateMultipartUpload
            | S3Request::CompleteMultipartUpload
            | S3Request::AbortMultipartUpload => self.control.clone(),
            S3Request::GetObjectRange | S3Request::PutObject | S3Request::UploadPart => {
                self.body.clone()
            }
        }
    }

    /// Applies [`config_override`](Self::config_override).
    pub(crate) fn customize<T, E, B>(
        &self,
        request: S3Request,
        op: CustomizableOperation<T, E, B>,
    ) -> CustomizableOperation<T, E, B> {
        op.config_override(self.config_override(request))
    }
}

impl std::fmt::Debug for OperationContext {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("OperationContext")
            .field("transfer", &self.transfer)
            .field("kind", &self.kind)
            .finish_non_exhaustive()
    }
}

/// The features an operation of `kind` adds to the requests it issues, on top of the
/// client's. A child takes its directory feature from the operation that spawned it.
fn operation_features(kind: OperationKind, parent: Option<OperationKind>) -> OperationFeatures {
    let directory = match parent.unwrap_or(kind) {
        OperationKind::UploadObjects => Some(AwsSdkFeature::S3TransferUploadDirectory),
        OperationKind::DownloadObjects => Some(AwsSdkFeature::S3TransferDownloadDirectory),
        _ => None,
    };
    let file = matches!(
        kind,
        OperationKind::Upload { from_file: true } | OperationKind::Download { to_file: true }
    )
    .then_some(AwsSdkFeature::S3FilePath);
    OperationFeatures(directory.into_iter().chain(file).collect())
}

/// Every S3 request the transfer manager sends. The name decides its override.
#[derive(Clone, Copy, Debug)]
pub(crate) enum S3Request {
    HeadObject,
    GetObjectDiscovery,
    GetObjectRange,
    ListObjectsV2,
    PutObject,
    CreateMultipartUpload,
    UploadPart,
    CompleteMultipartUpload,
    AbortMultipartUpload,
}

// Checks if the target path at `path`, with the provided `metadata`, represents a directory.
//
// The caller is responsible for providing the correct `Metadata`. If the `Metadata` is obtained
// via `fs::metadata`, it can only determine whether the path is a file or a directory, but it cannot
// indicate whether the path is a symbolic link. On the other hand, if `Metadata` is obtained through
// `fs::symlink_metadata`, it can identify symbolic links, but calling `is_dir()` on a symlink will
// return false, even if the symlink points to a directory.
pub(crate) fn validate_target_is_dir(metadata: &Metadata, path: &Path) -> Result<(), error::Error> {
    if metadata.is_dir() {
        Ok(())
    } else {
        Err(error::invalid_input(format!(
            "target is not a directory: {path:?}"
        )))
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// The request's name alone picks its override: a listing stays on the default retry
    /// partition, every other request shares the bucket's, and only body transfers tighten
    /// stalled-stream protection.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn request_name_picks_its_override() {
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        let config = crate::Config::builder().sdk_client(s3_client).build();
        let handle = crate::client::Handle::new_for_test(config, 1);
        let partition = handle.retry_partition("bucket");
        let kind = OperationKind::Upload { from_file: false };
        let (op, _rx) = OperationContext::new(handle, kind, Some("bucket"));
        for (request, partitioned, tightened) in [
            (S3Request::HeadObject, true, false),
            (S3Request::GetObjectDiscovery, true, false),
            (S3Request::GetObjectRange, true, true),
            (S3Request::ListObjectsV2, false, false),
            (S3Request::PutObject, true, true),
            (S3Request::CreateMultipartUpload, true, false),
            (S3Request::UploadPart, true, true),
            (S3Request::CompleteMultipartUpload, true, false),
            (S3Request::AbortMultipartUpload, true, false),
        ] {
            let config = op.config_override(request).build();
            assert_eq!(
                config.retry_partition(),
                partitioned.then_some(&partition),
                "{request:?}"
            );
            assert_eq!(
                config.stalled_stream_protection().map(|s| s.grace_period()),
                tightened.then(|| crate::retry::tightened_ssp().grace_period()),
                "{request:?}"
            );
        }
    }
}
