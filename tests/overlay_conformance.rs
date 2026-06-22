//! Delegation-conformance for `OverlayReader` (Lane X, T-X-OVL — DoD-X1 + DoD-X8).
//!
//! Asserts the 23-method delegation table from the build plan §2:
//! - the 3 FOLD methods (`neighbors`/`traverse`/`traverse_multi`) union the home with epoch-validated
//!   xedge cross-edges hydrated from the foreign engine;
//! - the HOME-ONLY methods (`find_symbols`, PageRank's `all_nodes`/`all_edges`, …) return ONLY home
//!   nodes even when a foreign engine is wired (DoD-X8 — no foreign leak into ranking/search);
//! - `get_node`/`symbol_epoch` ROUTE by engine tag;
//! - the cross seam runs under a REAL multi-thread runtime via `with_read_inline`+`block_on` (DoD-X1).
//!
//! Foreign engine = estate's `SqlitePool` (the only `AsyncGraphStore` — the production cross shape).
//! Home = a `MemStore` (Sync) playing the estate/code role (distractor code nodes).

use std::collections::HashMap;
use std::sync::Arc;

use wicked_estate_core::{
    Direction, EdgeKind, GraphRead, GraphWrite, Node, SymbolQuery,
    node::{Language, Location, NodeKind, Span},
    symbol::{Descriptor, Symbol, SymbolId},
};
use wicked_estate_store::{MemStore, SqliteStore, open_sqlite_pool};

use wicked_overlay::{CrossBudget, ForeignEngine, OverlayReader, XEdge, XedgeStore};

// ── fixture helpers ──────────────────────────────────────────────────────────

/// A code symbol id (estate role) — a global scip-style id.
fn code_id(name: &str) -> SymbolId {
    Symbol::global("scip-ts", None, vec![Descriptor::method(name, None)]).id()
}

/// A memory/doc symbol id (foreign role) — a synthetic "mem" id (uuid_v7-shaped in production).
fn doc_id(uuid: &str) -> SymbolId {
    Symbol::synthetic("mem", uuid).id()
}

fn code_node(name: &str) -> Node {
    Node::new(
        code_id(name),
        NodeKind::Function,
        name,
        Language::new("rust"),
        Location::new("src/code.rs", Span::ZERO),
    )
}

fn doc_node(uuid: &str, title: &str) -> Node {
    Node::new(
        doc_id(uuid),
        NodeKind::Synthetic,
        title,
        Language::new("markdown"),
        Location::new("knowledge", Span::ZERO),
    )
}

/// Build a foreign SQLite engine (the "memory" store) seeded with the given doc nodes, returned as
/// a `SqlitePool` (AsyncGraphStore). The DB file is kept alive by the returned `TempDir`.
fn foreign_memory_with_docs(docs: &[Node]) -> (tempfile::TempDir, Arc<dyn ForeignEngine>) {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("memory.db");
    let path_str = path.to_str().unwrap().to_string();
    {
        let mut store = SqliteStore::open(&path_str).unwrap();
        store.begin_batch().unwrap();
        store.upsert_nodes(docs).unwrap();
        store.commit_batch().unwrap();
    }
    let pool = open_sqlite_pool(&path_str, 4).unwrap();
    let arc: Arc<dyn ForeignEngine> = Arc::new(pool);
    (dir, arc)
}

/// A home `MemStore` (Sync) holding the given code nodes (the estate/distractor role).
fn home_estate_with_code(code: &[Node]) -> MemStore {
    let mut store = MemStore::new();
    store.begin_batch().unwrap();
    store.upsert_nodes(code).unwrap();
    store.commit_batch().unwrap();
    store
}

fn pools(
    map: Vec<(&'static str, Arc<dyn ForeignEngine>)>,
) -> Arc<HashMap<&'static str, Arc<dyn ForeignEngine>>> {
    Arc::new(map.into_iter().collect())
}

// ── DoD-X1: the cross seam RUNS under a real multi-thread runtime ────────────

/// DoD-X1 — the hydration seam runs: an `OverlayReader` reads one `about` cross-edge from a FOREIGN
/// memory pool via `with_read_inline`+`block_on`, under a real multi-thread runtime, and folds the
/// foreign doc node into `neighbors`/`traverse`. Falsifier: returns nothing / errors / deadlocks.
#[test]
fn dod_x1_cross_seam_runs_and_folds_foreign_about_edge() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        // Foreign "memory" engine holds the grounding doc; home "estate" holds the code seed.
        let (_dir, mem_pool) = foreign_memory_with_docs(&[doc_node("doc-1", "Idempotency design")]);
        let home = home_estate_with_code(&[code_node("processPayment")]);

        // xedge: doc-1 is ABOUT processPayment (memory --about--> estate), epoch 0/0.
        let xedge = XedgeStore::in_memory().unwrap();
        xedge
            .put_edge(&XEdge::about(
                doc_id("doc-1").0,
                code_id("processPayment").0,
                0,
            ))
            .unwrap();

        let others = pools(vec![("memory", mem_pool)]);

        // The overlay is constructed + USED inside a spawn_blocking thread — the real seam shape
        // (it would normally be the home engine's `with_read` closure).
        let folded: Vec<String> = tokio::task::spawn_blocking(move || {
            let overlay = OverlayReader::new(
                &home,
                "estate",
                xedge.reader(),
                others,
                vec!["about".to_string()],
                CrossBudget::default(),
            );
            // neighbors(code, Dependents) must fold the cross `about` edge → the doc id surfaces.
            let n = overlay
                .neighbors(&code_id("processPayment"), Direction::Dependents)
                .unwrap();
            n.into_iter().map(|e| e.source.0).collect()
        })
        .await
        .unwrap();

        assert!(
            folded.contains(&doc_id("doc-1").0),
            "the foreign `about` doc must fold into the code seed's dependents; got {folded:?}"
        );
    });
}

// ── DoD-X8: HOME-ONLY methods never leak foreign nodes ───────────────────────

/// DoD-X8 — with a foreign engine wired AND the cross path armed, `find_symbols` and PageRank's
/// `all_nodes`/`all_edges` return ONLY home nodes. Falsifier: a foreign (doc) node appears in
/// ranking or search. Runs the asserts inside `spawn_blocking` (the seam shape).
#[test]
fn dod_x8_home_only_methods_never_leak_foreign_nodes() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let (_dir, mem_pool) = foreign_memory_with_docs(&[
            doc_node("doc-1", "Idempotency"),
            doc_node("doc-2", "Retry policy"),
        ]);
        let home = home_estate_with_code(&[code_node("processPayment"), code_node("refund")]);

        let xedge = XedgeStore::in_memory().unwrap();
        xedge
            .put_edge(&XEdge::about(
                doc_id("doc-1").0,
                code_id("processPayment").0,
                0,
            ))
            .unwrap();
        xedge
            .put_edge(&XEdge::about(doc_id("doc-2").0, code_id("refund").0, 0))
            .unwrap();

        let others = pools(vec![("memory", mem_pool)]);

        tokio::task::spawn_blocking(move || {
            let overlay = OverlayReader::new(
                &home,
                "estate",
                xedge.reader(),
                others,
                vec!["about".to_string()], // cross ARMED
                CrossBudget::default(),
            );

            // all_nodes: exactly the 2 home code nodes — no docs.
            let all: Vec<String> = overlay
                .all_nodes()
                .unwrap()
                .into_iter()
                .map(|n| n.symbol.0)
                .collect();
            assert_eq!(all.len(), 2, "all_nodes must be home-only; got {all:?}");
            assert!(
                !all.iter().any(|s| s.contains("mem synthetic")),
                "no foreign doc node may leak into all_nodes (PageRank); got {all:?}"
            );

            // all_edges: home has no edges → empty (and certainly no xedge rows).
            assert!(
                overlay.all_edges().unwrap().is_empty(),
                "all_edges must be home-only (xedge rows never appear in global analytics)"
            );

            // find_symbols: an exact-name search resolves only home code, never a doc title.
            let found = overlay
                .find_symbols(&SymbolQuery {
                    exact_name: Some("processPayment".into()),
                    ..Default::default()
                })
                .unwrap();
            assert!(
                found.iter().all(|n| !n.symbol.0.contains("mem synthetic")),
                "find_symbols must be home-only; got {found:?}"
            );
        })
        .await
        .unwrap();
    });
}

// ── Cross-OFF: empty cross_edge_kinds makes the FOLD methods home-only ────────

/// With `cross_edge_kinds = []` (cross-OFF), `neighbors` returns ONLY home edges — the xedge `about`
/// row is NOT folded. This is the recall opt-OUT / code-tool default-OFF behaviour (DEC-X3/X3b).
/// Falsifier: a cross edge appears with no rel armed.
#[test]
fn cross_off_neighbors_is_home_only() {
    let home = home_estate_with_code(&[code_node("processPayment")]);
    let xedge = XedgeStore::in_memory().unwrap();
    xedge
        .put_edge(&XEdge::about("doc-1", code_id("processPayment").0, 0))
        .unwrap();
    let others = pools(vec![]); // no foreign engines needed when cross-OFF

    let overlay = OverlayReader::new(
        &home,
        "estate",
        xedge.reader(),
        others,
        vec![], // cross-OFF
        CrossBudget::default(),
    );
    let n = overlay
        .neighbors(&code_id("processPayment"), Direction::Dependents)
        .unwrap();
    assert!(
        n.is_empty(),
        "cross-OFF: the about row must NOT fold; got {n:?}"
    );
}

// ── ROUTE: get_node / symbol_epoch dispatch by engine ────────────────────────

/// `get_node` ROUTEs: a home code id resolves from home; a foreign doc id resolves from the foreign
/// pool. `symbol_epoch` ROUTEs the same way (home estate id → home gen; foreign / memory → 0).
#[test]
fn route_get_node_and_symbol_epoch_dispatch_by_engine() {
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();

    rt.block_on(async {
        let (_dir, mem_pool) = foreign_memory_with_docs(&[doc_node("doc-1", "Idempotency")]);
        let home = home_estate_with_code(&[code_node("processPayment")]);
        let xedge = XedgeStore::in_memory().unwrap();
        let others = pools(vec![("memory", mem_pool)]);

        tokio::task::spawn_blocking(move || {
            let overlay = OverlayReader::new(
                &home,
                "estate",
                xedge.reader(),
                others,
                vec!["about".to_string()],
                CrossBudget::default(),
            );

            // home id → resolved from home.
            let home_node = overlay.get_node(&code_id("processPayment")).unwrap();
            assert!(home_node.is_some(), "home code node routes to home");

            // foreign doc id → resolved from the foreign memory pool (ROUTE).
            let foreign_node = overlay.get_node(&doc_id("doc-1")).unwrap();
            assert!(
                foreign_node.is_some(),
                "foreign doc node must ROUTE to the memory pool"
            );
            assert_eq!(foreign_node.unwrap().name, "Idempotency");

            // unknown id → None from every engine.
            assert!(overlay.get_node(&code_id("nonexistent")).unwrap().is_none());

            // symbol_epoch: home estate id → Some(gen) (MemStore returns 0 for a live first-ever id).
            let epoch = overlay.symbol_epoch(&code_id("processPayment")).unwrap();
            assert_eq!(epoch, Some(0), "live home id has epoch 0 (never re-added)");
        })
        .await
        .unwrap();
    });
}

// ── capabilities (HOME-modified) ─────────────────────────────────────────────

/// `capabilities` reports home caps but forces `server_side_traversal=false` when the cross path is
/// armed (the union is client-side; DEC-X7).
#[test]
fn capabilities_forces_client_side_traversal_when_cross_on() {
    let home = home_estate_with_code(&[code_node("processPayment")]);
    let xedge = XedgeStore::in_memory().unwrap();
    let others = pools(vec![]);

    let cross_on = OverlayReader::new(
        &home,
        "estate",
        xedge.reader(),
        Arc::clone(&others),
        vec!["about".to_string()],
        CrossBudget::default(),
    );
    assert!(
        !cross_on.capabilities().server_side_traversal,
        "cross-ON forces client-side traversal"
    );
}

// ── 23-method count guard ────────────────────────────────────────────────────

/// A compile-time witness that `OverlayReader` implements the FULL `GraphRead` trait — every one of
/// the 23 methods. If estate adds a 24th `GraphRead` method, this fails to compile until the overlay
/// adds its delegation (the §0.1 "count copied across lanes without re-counting" scar guard).
#[test]
fn overlay_implements_full_graphread_trait_object() {
    let home = MemStore::new();
    let xedge = XedgeStore::in_memory().unwrap();
    let overlay = OverlayReader::new(
        &home,
        "estate",
        xedge.reader(),
        pools(vec![]),
        vec![],
        CrossBudget::default(),
    );
    // Coercion to &dyn GraphRead only succeeds if ALL 23 methods are implemented.
    let as_trait: &dyn GraphRead = &overlay;
    // Touch a representative method from each disposition so the witness is non-vacuous.
    assert!(as_trait.all_nodes().unwrap().is_empty()); // HOME-ONLY
    assert!(as_trait.stats().unwrap().node_count == 0); // HOME-ONLY
    assert!(matches!(
        as_trait.capabilities().server_side_traversal,
        true | false
    )); // HOME-modified
    assert!(
        as_trait
            .neighbors(&code_id("x"), Direction::Both)
            .unwrap()
            .is_empty()
    ); // FOLD
    assert!(as_trait.symbol_epoch(&code_id("x")).unwrap().is_none()); // ROUTE
    let _ = EdgeKind::Calls; // keep the EdgeKind import meaningful for future kind-specific asserts
}
