/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! [`OperationRecorder`]: an S3 client interceptor that records each request attempt.

use std::collections::BTreeSet;
use std::sync::{Arc, Mutex};

use aws_sdk_s3::config::interceptors::BeforeTransmitInterceptorContextRef;
use aws_sdk_s3::config::{ConfigBag, Intercept, RuntimeComponents};
use aws_smithy_runtime_api::client::orchestrator::Metadata;

use crate::upload_retry::fault_connector::part_number;

/// One request attempt: its operation name and, for an UploadPart, its part number.
type Attempt = (String, Option<u32>);

/// Records the operation name, and the `partNumber` of an UploadPart, of every request attempt the
/// S3 client sends.
#[derive(Debug, Clone, Default)]
pub(crate) struct OperationRecorder {
    /// `(operation name, part number)` in send order, one entry per attempt.
    seen: Arc<Mutex<Vec<Attempt>>>,
}

impl OperationRecorder {
    /// Number of recorded attempts of `operation`.
    pub(crate) fn count(&self, operation: &str) -> usize {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(op, _)| op == operation)
            .count()
    }

    /// The distinct part numbers sent by UploadPart, so a retried part counts once.
    pub(crate) fn part_numbers(&self) -> BTreeSet<u32> {
        self.seen
            .lock()
            .unwrap()
            .iter()
            .filter(|(op, _)| op == "UploadPart")
            .filter_map(|(_, part)| *part)
            .collect()
    }
}

impl Intercept for OperationRecorder {
    fn name(&self) -> &'static str {
        "OperationRecorder"
    }

    // Runs once per attempt, so SDK retries are counted.
    fn read_before_transmit(
        &self,
        context: &BeforeTransmitInterceptorContextRef<'_>,
        _rc: &RuntimeComponents,
        cfg: &mut ConfigBag,
    ) -> Result<(), aws_sdk_s3::error::BoxError> {
        if let Some(metadata) = cfg.load::<Metadata>() {
            let part = part_number(context.request());
            self.seen
                .lock()
                .unwrap()
                .push((metadata.name().to_string(), part));
        }
        Ok(())
    }
}
