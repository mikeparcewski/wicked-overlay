//! The local cross-engine-recall demonstration (Lane X, T-X-OVL) — the differentiator mechanism
//! PROVEN IN ISOLATION, plus the FOLD `traverse_multi`=union correctness and the DEC-X6 epoch
//! fail-closed drop.
//!
//! This is the crate's OWN self-contained proof that a code seed in `main` resolves a grounding doc
//! in `scope` via an `about` cross-edge over a ZERO-lexical-overlap corpus — the recall of a DOC
//! from CODE that markdown+BM25 cannot do. It is NOT the real `wicked-memory` bench wiring
//! (T-X-OVL-WIRE, a later serialized step); it proves the OverlayReader mechanism stands alone.
//!
//! Roles: `main` = home estate engine (code seeds + lexical distractors); `scope` = foreign memory
//! engine (the gold docs, reached ONLY through the overlay); `xedge` = the `about` bridge.

use std::collections::HashMap;
use std::sync::Arc;

use wicked_estate_core::{
    Direction, GraphRead, GraphWrite, Node, TraversalSpec,
    edge::{Edge, EdgeKind, ResolutionTier},
    node::{Language, Location, NodeKind, Span},
    symbol::{Descriptor, Symbol, SymbolId},
};
use wicked_estate_store::{MemStore, SqliteStore, open_sqlite_pool};

use wicked_overlay::{CrossBudget, ForeignEngine, OverlayReader, XEdge, XedgeStore};

// ── corpus: 4 (code-seed, gold-doc) pairs with ZERO lexical overlap ──────────
//
// Each code symbol name shares NO token with its gold doc's title — the lift can ONLY come from the
// cross-store `about` edge, never from a lexical match (the differentiator's whole point, DEC-X5
// ceiling C over the frozen `xedge-seed.jsonl` corpus).

struct Pair {
    code: &'static str, // estate code symbol name (the seed)
    doc_uuid: &'static str,
    doc_title: &'static str, // memory doc title — zero token overlap with `code`
}

const CORPUS: &[Pair] = &[
    Pair {
        code: "charge_card",
        doc_uuid: "k-001",
        doc_title: "Idempotent retry windows for billing settlement",
    },
    Pair {
        code: "rotate_keys",
        doc_uuid: "k-002",
        doc_title: "Envelope encryption and HSM custody policy",
    },
    Pair {
        code: "evict_session",
        doc_uuid: "k-003",
        doc_title: "Sliding-window TTL self-heal for token caches",
    },
    Pair {
        code: "shard_ledger",
        doc_uuid: "k-004",
        doc_title: "Partition rebalancing under hot-key skew",
    },
];

fn code_id(name: &str) -> SymbolId {
    Symbol::global("scip-ts", None, vec![Descriptor::method(name, None)]).id()
}
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

/// Build the foreign `scope` (memory) engine holding ONLY the gold docs, plus the home `main`
/// (estate) engine holding the code seeds + lexical distractors, plus the `about` xedge bridge.
fn build_corpus() -> (
    tempfile::TempDir,
    Arc<MemStore>,
    XedgeStore,
    Arc<dyn ForeignEngine>,
) {
    // scope: the gold docs live ONLY here (a separate store from `main`).
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("scope-memory.db");
    let path_str = path.to_str().unwrap().to_string();
    {
        let mut scope = SqliteStore::open(&path_str).unwrap();
        scope.begin_batch().unwrap();
        let docs: Vec<Node> = CORPUS
            .iter()
            .map(|p| doc_node(p.doc_uuid, p.doc_title))
            .collect();
        scope.upsert_nodes(&docs).unwrap();
        scope.commit_batch().unwrap();
    }
    let scope_pool = open_sqlite_pool(&path_str, 4).unwrap();
    let scope: Arc<dyn ForeignEngine> = Arc::new(scope_pool);

    // main: the code seeds + a pile of lexical distractor code (no doc tokens anywhere).
    let mut main = MemStore::new();
    main.begin_batch().unwrap();
    let mut code: Vec<Node> = CORPUS.iter().map(|p| code_node(p.code)).collect();
    for d in ["parse_args", "open_socket", "format_row", "hash_blob"] {
        code.push(code_node(d));
    }
    main.upsert_nodes(&code).unwrap();
    main.commit_batch().unwrap();

    // xedge: each gold doc is ABOUT its code seed (memory --about--> estate), epoch 0/0.
    let xedge = XedgeStore::in_memory().unwrap();
    for p in CORPUS {
        xedge
            .put_edge(&XEdge::about(doc_id(p.doc_uuid).0, code_id(p.code).0, 0))
            .unwrap();
    }

    (dir, Arc::new(main), xedge, scope)
}

fn pools(scope: Arc<dyn ForeignEngine>) -> Arc<HashMap<&'static str, Arc<dyn ForeignEngine>>> {
    let mut m: HashMap<&'static str, Arc<dyn ForeignEngine>> = HashMap::new();
    m.insert("memory", scope);
    Arc::new(m)
}

/// `cross_engine_recall_at_k(main, scope)` — the demonstration. For each code seed, expand the
/// overlay's cross-neighbors and count a hit iff the gold doc for that seed is surfaced. Returns the
/// recall fraction in `[0,1]`. `cross_edge_kinds` empty ⇒ cross-OFF (the baseline). `main` is an
/// `Arc` (MemStore is not `Clone`) so it can move into the blocking closure with a `'static` bound.
fn cross_engine_recall_at_k(
    main: Arc<MemStore>,
    scope: Arc<dyn ForeignEngine>,
    xedge: &XedgeStore,
    cross_edge_kinds: Vec<String>,
) -> f64 {
    let others = pools(scope);
    // The overlay is built + used on a blocking thread (the real `with_read` seam shape).
    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let reader = xedge.reader();
    rt.block_on(async move {
        tokio::task::spawn_blocking(move || {
            let overlay = OverlayReader::new(
                &*main,
                "estate",
                reader,
                others,
                cross_edge_kinds,
                CrossBudget::default(),
            );
            let mut hits = 0usize;
            for p in CORPUS {
                let gold = doc_id(p.doc_uuid).0;
                // The recall graph-arm: who is ABOUT this code seed? (cross dependents).
                let neighbors = overlay
                    .neighbors(&code_id(p.code), Direction::Dependents)
                    .unwrap();
                if neighbors.iter().any(|e| e.source.0 == gold) {
                    hits += 1;
                }
            }
            hits as f64 / CORPUS.len() as f64
        })
        .await
        .unwrap()
    })
}

/// THE DIFFERENTIATOR PROOF (local, in isolation). Over a zero-lexical-overlap corpus, cross-ON
/// recall is non-zero (and here perfect, 4/4) while cross-OFF recall is ZERO — the gold lives only
/// in `scope` and is unreachable without the overlay. The lift can ONLY be the `about` edge.
/// Falsifier: cross-ON recall == 0 (the seam doesn't fold) OR cross-OFF recall > 0 (a co-resident
/// leak — the gold would have to be in `main`, which it is not).
#[test]
fn cross_engine_recall_is_nonzero_on_zero_overlap_corpus() {
    let (_dir, main, xedge, scope) = build_corpus();

    let cross_off = cross_engine_recall_at_k(Arc::clone(&main), Arc::clone(&scope), &xedge, vec![]);
    let cross_on = cross_engine_recall_at_k(
        Arc::clone(&main),
        Arc::clone(&scope),
        &xedge,
        vec!["about".to_string()],
    );

    assert_eq!(
        cross_off, 0.0,
        "cross-OFF baseline must be 0 — the gold docs live ONLY in `scope`, unreachable without the overlay"
    );
    assert!(
        cross_on > 0.0,
        "cross-ON recall must be > 0 — the about-arm surfaces the grounding doc from the code seed"
    );
    // Strong form: with one about row per pair, recall is perfect.
    assert_eq!(cross_on, 1.0, "every code seed resolves its gold doc (4/4)");
    // The lift is strictly positive and sourced ONLY from the cross edge.
    assert!(
        cross_on >= cross_off + 0.5,
        "the differentiator lift (cross_on - cross_off) clears the bench margin M=0.5"
    );
}

/// The lift is SOURCED from xedge: delete the `about` rows and cross-ON recall collapses to the
/// cross-OFF baseline (0) — the falsifier the bench's `cross_edge_lifts_recall_from_xedge` pins
/// (DoD-X6: "present with the xedge row, ABSENT with it deleted").
#[test]
fn recall_lift_vanishes_when_xedge_rows_absent() {
    let (_dir, main, _full_xedge, scope) = build_corpus();
    // An EMPTY xedge overlay (no about rows) — same engines, no bridge.
    let empty_xedge = XedgeStore::in_memory().unwrap();
    let cross_on_no_rows =
        cross_engine_recall_at_k(main, scope, &empty_xedge, vec!["about".to_string()]);
    assert_eq!(
        cross_on_no_rows, 0.0,
        "with NO xedge about rows the lift must vanish — proves the lift is sourced from xedge, not co-residence"
    );
}

// ── FOLD traverse_multi = union of single-seed traverse (DoD-X2 spirit) ──────

/// The overlay's `traverse_multi` over multiple seeds equals the union of single-seed `traverse`,
/// INCLUDING the folded cross edges/nodes. Pins the FOLD method's multi-anchor expansion against the
/// per-seed reference (the conformance kit's equality property, extended to the overlay's cross fold).
#[test]
fn overlay_traverse_multi_matches_union_of_traverse() {
    let (_dir, main, xedge, scope) = build_corpus();
    let others = pools(scope);

    let rt = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
        .unwrap();
    let reader = xedge.reader();
    rt.block_on(async move {
        tokio::task::spawn_blocking(move || {
            let overlay = OverlayReader::new(
                &*main,
                "estate",
                reader,
                others,
                vec!["about".to_string()],
                CrossBudget::default(),
            );
            let seeds = [code_id("charge_card"), code_id("rotate_keys")];
            let mut spec = TraversalSpec::blast_radius(2);
            spec.direction = Direction::Both;

            let multi = overlay.traverse_multi(&seeds, &spec).unwrap();

            // Reference: union of single-seed traverse over the same overlay.
            let mut union_nodes = std::collections::BTreeSet::new();
            for s in &seeds {
                for n in overlay.traverse(s, &spec).unwrap().nodes {
                    union_nodes.insert(n.symbol.0);
                }
            }
            let multi_nodes: std::collections::BTreeSet<String> =
                multi.nodes.iter().map(|n| n.symbol.0.clone()).collect();

            assert_eq!(
                multi_nodes, union_nodes,
                "overlay traverse_multi node set must equal the union of single-seed traverse"
            );
            // The fold pulled the two gold docs across the boundary (one per seed).
            assert!(multi_nodes.contains(&doc_id("k-001").0));
            assert!(multi_nodes.contains(&doc_id("k-002").0));
        })
        .await
        .unwrap();
    });
}

// ── DEC-X6: read-time epoch fail-closed (the stale cross-edge is dropped) ─────

/// DEC-X6 fail-closed: an `about` row stamped with a STALE estate epoch (row gen != live gen) is
/// DROPPED at read time — it never resolves to the live-WRONG node. Here the home (estate) reports a
/// live epoch of 0 for the code id, but the xedge row is stamped epoch=7 (as if written before a
/// reuse bump) → the row is dropped, recall for that seed is 0. Falsifier: the stale row folds anyway.
#[test]
fn stale_epoch_about_edge_is_dropped_fail_closed() {
    let (_dir, main, _xedge, scope) = build_corpus();
    let stale = XedgeStore::in_memory().unwrap();
    // Stamp the target (estate) endpoint with a NON-matching epoch (7) — MemStore live gen is 0.
    stale
        .put_edge(&XEdge::about(
            doc_id("k-001").0,
            code_id("charge_card").0,
            7,
        ))
        .unwrap();

    let recall = cross_engine_recall_at_k(main, scope, &stale, vec!["about".to_string()]);
    assert_eq!(
        recall, 0.0,
        "a stale-epoch about edge (row gen=7, live gen=0) must be dropped fail-closed — never resolves"
    );
}

// keep Edge/EdgeKind/ResolutionTier meaningful (used to assert folded-edge shape would compile).
#[allow(dead_code)]
fn _edge_shape_witness() -> Edge {
    Edge::new(
        code_id("a"),
        code_id("b"),
        EdgeKind::Other("about".into()),
        ResolutionTier::Heuristic,
        "witness",
    )
}
