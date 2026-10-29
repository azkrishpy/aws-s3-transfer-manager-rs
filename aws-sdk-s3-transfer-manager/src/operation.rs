/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

use std::{fs::Metadata, path::Path, sync::Arc};

use aws_sdk_s3::client::customize::CustomizableOperation;

use crate::config::user_agent::operation_features;
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
#[derive(Clone, Debug)]
pub(crate) struct OperationContext {
    transfer: TransferContext,
    kind: OperationKind,
    requests: Arc<RequestOverrides>,
}

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
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
        let mut control =
            aws_sdk_s3::config::Builder::default().interceptor(operation_features(kind, parent));
        if let Some(bucket) = bucket {
            control = control.retry_partition(transfer.handle.retry_partition(bucket));
        }
        let body = control
            .clone()
            .stalled_stream_protection(crate::retry::tightened_ssp());
        Self {
            transfer,
            kind,
            requests: Arc::new(RequestOverrides { control, body }),
        }
    }

    pub(crate) fn transfer(&self) -> &TransferContext {
        &self.transfer
    }

    /// The override for one request of this operation.
    pub(crate) fn config_override(&self, request: S3Request) -> aws_sdk_s3::config::Builder {
        if request.tightens_stall_protection() {
            self.requests.body.clone()
        } else {
            self.requests.control.clone()
        }
    }

    /// Applies [`config_override`](Self::config_override). The only `config_override`
    /// call for a transfer manager request; add further per-request customization
    /// with `.interceptor(..)`, which composes with it.
    pub(crate) fn customize<T, E, B>(
        &self,
        request: S3Request,
        op: CustomizableOperation<T, E, B>,
    ) -> CustomizableOperation<T, E, B> {
        op.config_override(self.config_override(request))
    }
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

impl S3Request {
    /// Whether the request streams an object body under the tightened stalled-stream
    /// grace. A discovery GET keeps the client's; a re-issue of its range gets it.
    fn tightens_stall_protection(self) -> bool {
        matches!(
            self,
            Self::GetObjectRange | Self::PutObject | Self::UploadPart
        )
    }
}

/// An operation's request overrides, built once when the operation is created.
#[derive(Debug)]
struct RequestOverrides {
    /// Retry partition and operation features.
    control: aws_sdk_s3::config::Builder,
    /// `control` plus the tightened stalled-stream grace.
    body: aws_sdk_s3::config::Builder,
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

    /// The request's name alone picks its override: every request shares the operation's retry
    /// partition, and only body transfers tighten stalled-stream protection.
    #[cfg_attr(miri, ignore)]
    #[tokio::test]
    async fn request_name_picks_its_override() {
        let s3_client = aws_smithy_mocks::mock_client!(aws_sdk_s3, []);
        let config = crate::Config::builder().sdk_client(s3_client).build();
        let handle = crate::client::Handle::new_for_test(config, 1);
        let kind = OperationKind::Upload { from_file: false };
        let (op, _rx) = OperationContext::new(handle, kind, Some("bucket"));
        for (request, tightened) in [
            (S3Request::HeadObject, false),
            (S3Request::GetObjectDiscovery, false),
            (S3Request::GetObjectRange, true),
            (S3Request::ListObjectsV2, false),
            (S3Request::PutObject, true),
            (S3Request::CreateMultipartUpload, false),
            (S3Request::UploadPart, true),
            (S3Request::CompleteMultipartUpload, false),
            (S3Request::AbortMultipartUpload, false),
        ] {
            let config = op.config_override(request).build();
            assert_eq!(
                config.stalled_stream_protection().is_some(),
                tightened,
                "{request:?}"
            );
            assert!(config.retry_partition().is_some(), "{request:?}");
        }
    }
}
