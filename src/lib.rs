//! # wicked-overlay — Lane X: the cross-store differentiator engine
//!
//! The `xedge.db` single-writer cross-edge overlay ([`XedgeStore`] / [`XedgeReader`]) + the in-proc
//! [`OverlayReader`]: [`GraphRead`](wicked_estate_core::GraphRead) that unions a HOME engine with
//! FOREIGN engines at read time. This is the engine of the cross-store "about-arm" differentiator:
//! recall a grounding DOC from a CODE seed — the one move brain's markdown+BM25 cannot do.
//!
//! Estate-dep-only. The spine it programs against
//! ([`symbol_epoch`](wicked_estate_core::GraphRead::symbol_epoch),
//! [`traverse_multi`](wicked_estate_core::GraphRead::traverse_multi),
//! [`with_read_inline`](wicked_estate_core::AsyncGraphStore::with_read_inline)) lives in the LOCAL
//! estate 0.12.0 (`lane-a/epoch`). Authoritative grounding: `wicked-estate/docs/recon/
//! design-lane-X-overlay-v3.md` + `wicked-memory/docs/recon/lane-X-build-plan.md`.
//!
//! ## What's built here (Wave 2, Lane X — T-X-SPEC + T-X-OVL)
//! - [`xedge`] — the `xedge.db` schema, [`XedgeStore`] (single writer), [`XedgeReader`] (cheap
//!   clone), epoch-stamped rows that convert to [`Edge`](wicked_estate_core::Edge).
//! - [`overlay`] — [`OverlayReader`], implementing all **23** `GraphRead` methods per the plan's
//!   HOME-ONLY / FOLD / ROUTE delegation table.
//!
//! The real `wicked-memory` bench wiring (T-X-OVL-WIRE) is a LATER serialized step and is NOT here;
//! the crate's own tests carry a self-contained `cross_engine_recall_at_k` demonstration over a
//! zero-lexical-overlap corpus to prove the mechanism in isolation.

pub mod overlay;
pub mod xedge;

pub use overlay::{CrossBudget, ESTATE_ENGINE, ForeignEngine, ForeignPools, OverlayReader};
pub use xedge::{Endpoint, MEMORY_EPOCH, XEDGE_SCHEMA_VERSION, XEdge, XedgeReader, XedgeStore};
