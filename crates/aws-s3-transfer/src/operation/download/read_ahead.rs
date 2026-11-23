/*
 * Copyright Amazon.com, Inc. or its affiliates. All Rights Reserved.
 * SPDX-License-Identifier: Apache-2.0
 */

//! Per-transfer read-ahead window: the occupancy bound on speculative issuance.
//!
//! Issuance of a speculative part requires `issued - released < window()`, where
//! `issued` counts parts claimed for issuance and `released` counts parts freed by
//! either delivery surface (stream pull or disk drain; see `recv_buffer`). The
//! quantity `issued - released` is the transfer's resident occupancy in parts, so
//! the gate bounds how far issuance runs ahead of consumption and nothing more.
//! It is one of three independent upper bounds on issuance; the others — total
//! in-flight concurrency and the client's memory budget — live elsewhere, and
//! issuance takes their min.
//!
//! The window is free buffer space, `window - occupancy`, an exact subtraction
//! recomputed as the buffer fills and drains. It is not estimated from a rate and
//! it does not decay, so a gap in delivery does not shrink it: a consumer paused
//! between bursts, or blocked behind a not-yet-filled part, still leaves a full
//! window of slack to make progress.
//!
//! # Pacing
//!
//! In `Auto` mode the window is the transfer's share of the client's memory
//! budget, in parts: the budget divided by the number of live downloads, never
//! below [`DEFAULT_WINDOW_PARTS`](crate::operation::download::read_ahead::DEFAULT_WINDOW_PARTS)
//! (or the whole budget, if smaller). A download running alone may fill the
//! budget; concurrent downloads split it, and each share is recomputed on every
//! gate read as downloads start and finish. The pacing behaviors follow from the
//! occupancy gate directly:
//!
//! - Fast consumer: `released` keeps pace, `issued - released` stays small, the
//!   gate does not bind, and the transfer runs at the concurrency limit.
//! - Slow consumer: `released` lags, `issued - released` fills to the window, the
//!   gate closes, and issuance pauses until the consumer drains. This transfer's
//!   resident occupancy is then bounded at `window` parts, its share of the budget.
//! - Blocked consumer (a stalled or missing part): `released` cannot advance past
//!   the hole, occupancy pins at the window, and issuance stops at `window`, leaving
//!   a full window of slack to make progress around the hole rather than collapsing
//!   to one part.
//!
//! The window is stored in an [`std::sync::atomic::AtomicU64`] so it can be changed
//! with a lock-free store while a transfer runs (see
//! [`ReadAhead::set_window`](crate::operation::download::read_ahead::ReadAhead::set_window)).

use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;

/// Minimum `Auto` window, in parts: set well above the bandwidth-delay product of
/// a 100 Gbps link so the gate does not bind a consumer that keeps pace, however
/// many downloads share the budget. The memory budget still bounds the sum.
pub(crate) const DEFAULT_WINDOW_PARTS: u64 = 1024;

/// Stored window value meaning "use the `Auto` budget share". `Parts(n)` resolves
/// to `n + 1`, so it never collides.
const AUTO: u64 = 0;

/// Resolve the effective read-ahead window in parts for a download, in precedence
/// order: the per-request [`ReadAhead`](crate::config::ReadAhead) override if set,
/// else the client default. See [`window_parts_for`] for the per-mode mapping.
pub(crate) fn resolve_window(
    request: Option<&crate::config::ReadAhead>,
    client_default: &crate::config::ReadAhead,
) -> u64 {
    window_parts_for(request.unwrap_or(client_default))
}

/// Map the public [`ReadAhead`](crate::config::ReadAhead) knob to a stored window.
/// `Auto` is the budget share, resolved on each read; `Parts(n)` is `n + 1` — `n`
/// parts of speculation beyond the part the consumer is waiting on, which is always
/// admitted, so `Parts(0)` is demand paging. The single definition of the knob's
/// meaning, used at transfer construction and by the dynamic control surface.
pub(crate) fn window_parts_for(mode: &crate::config::ReadAhead) -> u64 {
    match mode {
        crate::config::ReadAhead::Auto => AUTO,
        crate::config::ReadAhead::Parts(n) => (*n as u64).saturating_add(1),
    }
}

/// Client-wide `Auto` window: the memory budget in parts, divided among live downloads.
#[derive(Debug)]
pub(crate) struct WindowShare {
    /// Memory budget in parts of the client's part size.
    budget_parts: u64,
    /// Live downloads, counted from construction until drop of their [`ReadAhead`].
    // ceiling: a finished download counts until its handle drops; shares only fall to
    // the floor. Decrement at the terminal transition if held handles matter.
    active: AtomicU64,
}

impl WindowShare {
    /// Share `budget_bytes` in parts of `part_size` bytes.
    pub(crate) fn new(budget_bytes: u64, part_size: u64) -> Self {
        Self {
            budget_parts: budget_bytes / part_size.max(1),
            active: AtomicU64::new(0),
        }
    }

    /// Budget divided by live downloads, at least the floor, and at least one part.
    fn window(&self) -> u64 {
        let floor = DEFAULT_WINDOW_PARTS.min(self.budget_parts);
        let share = self.budget_parts / self.active.load(Ordering::Relaxed).max(1);
        share.max(floor).max(1)
    }
}

/// Per-transfer read-ahead window: the occupancy bound on speculative issuance.
///
/// `window()` is the hot read (the issuance gate, per claim) and is lock-free.
/// Shared `&self` across the issuer and consumer tasks. Counts as a live download
/// in its [`WindowShare`] until dropped.
pub(crate) struct ReadAhead {
    /// Stored window in parts, or [`AUTO`]. Read by the gate on every claim.
    window: AtomicU64,
    share: Arc<WindowShare>,
}

impl ReadAhead {
    /// Create a controller with a stored window from [`window_parts_for`].
    pub(crate) fn new(window: u64, share: Arc<WindowShare>) -> Self {
        share.active.fetch_add(1, Ordering::Relaxed);
        Self {
            window: AtomicU64::new(window),
            share,
        }
    }

    /// The current read-ahead window, in parts. Lock-free; the issuance gate reads
    /// this per claim (`issued - released < window()`).
    pub(crate) fn window(&self) -> u64 {
        match self.window.load(Ordering::Relaxed) {
            AUTO => self.share.window(),
            parts => parts,
        }
    }

    /// Set the stored window from [`window_parts_for`]. Lock-free. Used to apply a
    /// dynamic read-ahead change to a running transfer; the next gate read observes it.
    pub(crate) fn set_window(&self, parts: u64) {
        self.window.store(parts, Ordering::Relaxed);
    }

    /// Test-only: force the window to a fixed value, so transfer-level tests can
    /// drive the issuance gate deterministically (e.g. exercise the closed-gate
    /// path without claiming a thousand parts).
    #[cfg(test)]
    pub(crate) fn force_window(&self, w: u64) {
        self.set_window(w);
    }
}

impl Drop for ReadAhead {
    fn drop(&mut self) {
        self.share.active.fetch_sub(1, Ordering::Relaxed);
    }
}

impl std::fmt::Debug for ReadAhead {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ReadAhead")
            .field("window", &self.window())
            .finish()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn auto_window_is_the_budget_share_of_live_downloads() {
        let share = Arc::new(WindowShare::new(4096 * 8, 8));
        let first = ReadAhead::new(AUTO, share.clone());
        assert_eq!(first.window(), 4096, "a lone download may fill the budget");
        let second = ReadAhead::new(AUTO, share.clone());
        assert_eq!((first.window(), second.window()), (2048, 2048));
        let more: Vec<_> = (0..6)
            .map(|_| ReadAhead::new(AUTO, share.clone()))
            .collect();
        assert_eq!(
            first.window(),
            DEFAULT_WINDOW_PARTS,
            "eight shares stop at the floor"
        );
        drop(more);
        drop(second);
        assert_eq!(
            first.window(),
            4096,
            "finished downloads return their share"
        );

        first.set_window(window_parts_for(&crate::config::ReadAhead::Parts(6)));
        assert_eq!(first.window(), 7, "an explicit window ignores the share");
        first.set_window(window_parts_for(&crate::config::ReadAhead::Auto));
        assert_eq!(first.window(), 4096);

        for (budget_parts, expected) in [(100, 100), (0, 1)] {
            let small = ReadAhead::new(AUTO, Arc::new(WindowShare::new(budget_parts, 1)));
            assert_eq!(small.window(), expected, "budget of {budget_parts} parts");
        }
    }

    #[test]
    fn window_parts_for_maps_the_knob() {
        use crate::config::ReadAhead as Knob;
        assert_eq!(window_parts_for(&Knob::Auto), AUTO);
        // Parts(n) -> n + 1: n speculative parts plus the always-admitted demand part.
        assert_eq!(
            window_parts_for(&Knob::Parts(0)),
            1,
            "Parts(0) is demand paging"
        );
        assert_eq!(window_parts_for(&Knob::Parts(1)), 2);
        assert_eq!(window_parts_for(&Knob::Parts(255)), 256);
        // The + 1 saturates rather than overflowing at the top of the range.
        assert_eq!(
            window_parts_for(&Knob::Parts(usize::MAX)),
            (usize::MAX as u64).saturating_add(1)
        );
    }
}
