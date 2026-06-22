//! `OverlayReader` — the in-proc cross-store union reader (Lane X, T-X-OVL). ★ THE ENGINE.
//!
//! An `OverlayReader` wraps a HOME engine's `&dyn GraphRead` and, for the three FOLD methods, unions
//! it with FOREIGN engines' async pools at read time via the `xedge` overlay. It implements ALL **23**
//! [`GraphRead`] methods (re-counted on the published `lane-a/epoch` trait — design v3's "28" is a
//! stale miscount; see the build plan §0.1), each classified HOME-ONLY / FOLD / ROUTE per the plan §2
//! delegation table:
//!
//! | # | method | disposition | # | method | disposition |
//! |---|---|---|---|---|---|
//! | 1 | `capabilities` | HOME (modified) | 13 | `edge_history` | HOME-ONLY |
//! | 2 | `get_node` | ROUTE | 14 | `file_content` | HOME-ONLY |
//! | 3 | `find_symbols` | HOME-ONLY | 15 | `symbol_source` | HOME-ONLY |
//! | 4 | `neighbors` | FOLD | 16 | `changes_since` | HOME-ONLY |
//! | 5 | `traverse` | FOLD (→ multi) | 17 | `node_semantics` | HOME-ONLY |
//! | 6 | `traverse_multi` | FOLD | 18 | `find_by_requirement` | HOME-ONLY |
//! | 7 | `all_nodes` | HOME-ONLY | 19 | `annotations` | HOME-ONLY |
//! | 8 | `all_edges` | HOME-ONLY | 20 | `annotations_by_type` | HOME-ONLY |
//! | 9 | `unresolved_refs_for_name` | HOME-ONLY | 21 | `annotations_stale_since` | HOME-ONLY |
//! | 10 | `file_digest` | HOME-ONLY | 22 | `symbol_epoch` | ROUTE |
//! | 11 | `file_git_sha` | HOME-ONLY | 23 | `stats` | HOME-ONLY |
//! | 12 | `repo_info` | HOME-ONLY | | | |
//!
//! **Count:** 2 ROUTE + 3 FOLD + 1 HOME-modified + 17 HOME-ONLY = 23. The cross-store value is
//! concentrated in exactly the 3 FOLD methods; everything ranking/search/analytics stays HOME-ONLY so
//! NO foreign node leaks into PageRank or `find_symbols` (DoD-X8).
//!
//! ## The no-deadlock seam (DEC-X1 / DEC-X1b — PROVEN-IN-DESIGN by tokio source trace)
//! The OverlayReader is sync (`GraphRead`) and is constructed INSIDE the home `with_read` closure,
//! itself running on a `spawn_blocking` thread. Its FOLD methods drive each foreign engine through
//! `others[engine].with_read_inline(|g| …)` inside ONE [`Handle::block_on`] per foreign engine per
//! ply. `with_read_inline` runs the foreign read on the held thread WITHOUT re-entering
//! `spawn_blocking`, so net blocking-pool occupancy per cross-recall is 1, not `1+k` — the bound
//! T-A-INLINE's `with_read_inline_no_deadlock_under_saturation` already proves (DoD-XA1b).
//!
//! ## Read-time epoch validation (DEC-X6 — fail-closed, loud)
//! Each folded xedge row's stamped endpoint epoch is validated against the LIVE `symbol_epoch`
//! (estate → the routed engine's `symbol_epoch`; memory → constant 0). A mismatch drops the row with
//! a loud `XEDGE-STALE-EPOCH:` marker — a deleted-then-re-added code symbol (epoch bumped, T-A-EPOCH
//! M8) never resolves a stale `about` edge to a live-WRONG node.

use std::collections::HashMap;
use std::sync::Arc;
use tokio::runtime::Handle;
use wicked_estate_core::{
    AsyncGraphStore, Direction, Edge, GraphRead, Node, Result, StoreCapabilities, Subgraph,
    SymbolQuery, TraversalSpec, annotation::Annotation, change::Change, history::HistoricalEdge,
    refs::UnresolvedRef, repo::RepoInfo, semantics::NodeSemantics, symbol::SymbolId,
};

use crate::xedge::{MEMORY_EPOCH, XEdge, XedgeReader};

/// The estate engine tag — the only engine whose endpoints carry a non-vacuous epoch (its intern is
/// append-only; reuse-after-delete bumps `symbol_epoch`). All other engines treated as epoch-0.
pub const ESTATE_ENGINE: &str = "estate";

/// Object-safe foreign-engine read seam (the dyn-compat fix the design sketch glossed over).
///
/// [`AsyncGraphStore`] is NOT dyn-compatible — its `with_read`/`with_read_inline` carry generic type
/// parameters (`F`, `T`), so `dyn AsyncGraphStore` is illegal. The OverlayReader nonetheless needs a
/// HETEROGENEOUS, engine-tagged map of foreign pools. `ForeignEngine` is the object-safe bridge: it
/// exposes exactly the cross-store reads the FOLD/ROUTE methods need (`get_node`, `symbol_epoch`) as
/// NON-generic blocking methods, so `dyn ForeignEngine` is object-safe. The blanket impl runs each
/// read through the engine's `with_read_inline` inside ONE `Handle::block_on` on the held blocking
/// thread (DEC-X1b — net occupancy 1, no nested `spawn_blocking`).
///
/// PRECONDITION: the caller is already on a blocking-pool thread (the OverlayReader, inside the home
/// `with_read` closure). On a normal async worker these methods would block the runtime.
pub trait ForeignEngine: Send + Sync {
    /// `get_node` on this foreign engine, driven from the held blocking thread (no nested spawn).
    fn get_node_blocking(&self, id: &SymbolId) -> Result<Option<Node>>;
    /// `symbol_epoch` on this foreign engine (estate → live gen; memory pool → 0 for live ids).
    fn symbol_epoch_blocking(&self, id: &SymbolId) -> Result<Option<u64>>;
}

/// The no-deadlock seam, shared by every [`ForeignEngine`] method (DEC-X1b). `block_in_place`
/// from a `spawn_blocking` thread is a no-op wrapper (tokio 1.52.3 worker.rs:432-436); `block_on`
/// then parks THIS held thread until the foreign `with_read_inline` read completes.
fn run_inline<P, F, T>(pool: &P, f: F) -> Result<T>
where
    P: AsyncGraphStore + ?Sized,
    F: for<'b> FnOnce(&'b dyn GraphRead) -> Result<T> + Send + 'static,
    T: Send + 'static,
{
    let handle = Handle::current();
    tokio::task::block_in_place(move || {
        handle.block_on(async move { pool.with_read_inline(f).await })
    })
}

/// Blanket impl: ANY `AsyncGraphStore` (e.g. estate's `SqlitePool`) is a `ForeignEngine`. The
/// generics are monomorphized HERE inside a concrete impl, so the `ForeignEngine` methods stay
/// non-generic and `dyn ForeignEngine` is object-safe.
impl<A: AsyncGraphStore + ?Sized> ForeignEngine for A {
    fn get_node_blocking(&self, id: &SymbolId) -> Result<Option<Node>> {
        let id = id.clone();
        run_inline(self, move |g| g.get_node(&id))
    }

    fn symbol_epoch_blocking(&self, id: &SymbolId) -> Result<Option<u64>> {
        let id = id.clone();
        run_inline(self, move |g| g.symbol_epoch(&id))
    }
}

/// The foreign-pool map the OverlayReader captures: engine tag → object-safe [`ForeignEngine`].
/// Build it from any [`AsyncGraphStore`] (e.g. `SqlitePool`) via [`ForeignPools::insert`].
pub type ForeignPools = HashMap<&'static str, Arc<dyn ForeignEngine>>;

/// Per-recall cross-expansion budget (the OverlayReader-side analogue of the estate `TraversalSpec`
/// cross fields, which are a T-A-RELEASE-gated estate-spine touch this crate does NOT make — see the
/// build plan §0.2). Carried on the reader so the cross path is bounded independent of the estate
/// `TraversalSpec` shape.
#[derive(Debug, Clone, Copy)]
pub struct CrossBudget {
    /// Max cross-store hops per expansion (Default 1 — memory/knowledge are leaves off a code seed,
    /// DEC-X4). `>1` is gated on the bench latency (DEC-X5), out of scope here.
    pub max_cross_hops: u32,
    /// Max nodes pulled across the boundary per expansion (Default 64, DEC-X3).
    pub max_cross_nodes: usize,
}

impl Default for CrossBudget {
    fn default() -> Self {
        Self {
            max_cross_hops: 1,
            max_cross_nodes: 64,
        }
    }
}

/// The cross-store union reader. Borrows the HOME reader for the closure body; owns/`Arc`s
/// everything else so the `'static` bound on `with_read`'s `F` is satisfied (design v3 §DEC-X1).
///
/// Generic over the home reader `H` (not a bare `&dyn GraphRead`) for a load-bearing reason: the
/// implemented [`GraphRead`] trait has a `Send` supertrait, so an `OverlayReader` that BORROWS its
/// home must be `Send`, which requires the borrow's referent to be `Sync` (`&T: Send ⟺ T: Sync`).
/// `dyn GraphRead` is `Send` but NOT `Sync`, and `SqliteStore` is `!Sync` (it holds a `rusqlite`
/// `Connection`). The generic lets the home be any `Sync` reader — a `MemStore`, a `SqlitePool`
/// (`Arc`-backed, `Sync`), or an explicit `&(dyn GraphRead + Sync)`. See the crate-level note and
/// the build report: wiring a `!Sync` SQLite `with_read(&dyn GraphRead)` home is an estate-seam
/// integration concern for T-X-OVL-WIRE, not a defect in this 23-method delegation.
pub struct OverlayReader<'a, H: GraphRead + ?Sized> {
    /// The home engine's read handle (borrowed inside the home `with_read` closure).
    home: &'a H,
    /// The home engine tag (`"estate"` / `"memory"` / …) — the engine `home` actually backs.
    home_engine: &'static str,
    /// The cross-store edge overlay (cheap owned clone).
    xedge: XedgeReader,
    /// Foreign engine pools, by engine tag. The FOLD methods drive these via `with_read_inline`
    /// behind the object-safe [`ForeignEngine`] seam.
    others: Arc<ForeignPools>,
    /// xedge rels eligible to cross (empty = NONE — the inverse of estate's `edge_kinds`, DEC-X3).
    /// Recall passes `["about"]`; a cross-OFF caller passes `[]`.
    cross_edge_kinds: Vec<String>,
    /// The per-recall cross-expansion budget.
    budget: CrossBudget,
}

impl<'a, H: GraphRead + ?Sized> OverlayReader<'a, H> {
    /// Construct an overlay over `home` (the engine `home_engine` backs). `cross_edge_kinds` empty
    /// ⇒ the FOLD methods behave HOME-ONLY (cross-OFF); pass `["about"]` for the about-arm.
    pub fn new(
        home: &'a H,
        home_engine: &'static str,
        xedge: XedgeReader,
        others: Arc<ForeignPools>,
        cross_edge_kinds: Vec<String>,
        budget: CrossBudget,
    ) -> Self {
        Self {
            home,
            home_engine,
            xedge,
            others,
            cross_edge_kinds,
            budget,
        }
    }

    /// True when the cross path is armed (at least one rel is eligible to cross).
    fn cross_on(&self) -> bool {
        !self.cross_edge_kinds.is_empty()
    }

    /// `cross_edge_kinds` as `&str`s for the [`XedgeReader`] query.
    fn cross_rels(&self) -> Vec<&str> {
        self.cross_edge_kinds.iter().map(String::as_str).collect()
    }

    /// The live epoch of `(engine, id)` for read-time validation. estate (home or foreign) →
    /// `symbol_epoch`; every other engine → constant 0 (uuid_v7, never reused). `None` ⇒ no live node.
    fn live_epoch(&self, engine: &str, id: &SymbolId) -> Result<Option<u64>> {
        if engine != ESTATE_ENGINE {
            return Ok(Some(MEMORY_EPOCH));
        }
        if engine == self.home_engine {
            return self.home.symbol_epoch(id);
        }
        // estate is a FOREIGN engine — route the epoch read through its pool.
        match self.others.get(engine) {
            Some(pool) => pool.symbol_epoch_blocking(id),
            None => Ok(None),
        }
    }

    /// Keep an xedge row only if its stamped endpoint epochs match the LIVE epochs (fail-closed on
    /// inequality, loud). The cross-store endpoint is whichever end is NOT the home id being expanded;
    /// both ends are validated so neither a stale source nor a stale target resolves (DEC-X6.3).
    fn epoch_valid(&self, row: &XEdge) -> bool {
        for end in [&row.source, &row.target] {
            let live = match self.live_epoch(&end.engine, &SymbolId(end.stable_id.clone())) {
                Ok(v) => v,
                Err(_) => {
                    // A read error on the epoch is treated as "cannot prove fresh" ⇒ drop, loud.
                    eprintln!(
                        "XEDGE-STALE-EPOCH: edge to {}:{} dropped (epoch read failed; prune queued)",
                        end.engine, end.stable_id
                    );
                    return false;
                }
            };
            match live {
                Some(live_epoch) if live_epoch == end.epoch => {}
                Some(live_epoch) => {
                    eprintln!(
                        "XEDGE-STALE-EPOCH: edge to {}:{} dropped (row gen={}, live gen={} — id reused; prune queued)",
                        end.engine, end.stable_id, end.epoch, live_epoch
                    );
                    return false;
                }
                None => {
                    eprintln!(
                        "XEDGE-STALE-EPOCH: edge to {}:{} dropped (no live node; prune queued)",
                        end.engine, end.stable_id
                    );
                    return false;
                }
            }
        }
        true
    }

    /// The cross-store dependents of `id` (home engine = `self.home_engine`): xedge rows whose TARGET
    /// is `(home_engine, id)` and whose rel ∈ `cross_edge_kinds`, epoch-validated, hydrated into
    /// `core::Edge`s. The FOLD core (method #4). Bounded by `max_cross_nodes`.
    fn cross_neighbors(&self, id: &SymbolId, dir: Direction) -> Result<Vec<Edge>> {
        if !self.cross_on() {
            return Ok(vec![]);
        }
        // Dependents = edges pointing AT id (in_edges); Dependencies = edges FROM id (out_edges).
        // Both = the union. This mirrors the estate edge-direction invariant.
        let rels = self.cross_rels();
        let rows = match dir {
            Direction::Dependents => self.xedge.in_edges(self.home_engine, id.as_str(), &rels)?,
            Direction::Dependencies => {
                self.xedge.out_edges(self.home_engine, id.as_str(), &rels)?
            }
            Direction::Both => {
                let mut r = self.xedge.in_edges(self.home_engine, id.as_str(), &rels)?;
                r.extend(self.xedge.out_edges(self.home_engine, id.as_str(), &rels)?);
                r
            }
        };
        let mut out = Vec::new();
        for row in rows {
            if out.len() >= self.budget.max_cross_nodes {
                break;
            }
            if self.epoch_valid(&row) {
                out.push(row.to_core_edge());
            }
        }
        Ok(out)
    }

    /// Fetch the cross-store endpoint NODE for a folded edge from the foreign engine that owns it, so
    /// `traverse`'s subgraph carries the real foreign node (not just the edge). Routes by engine tag.
    fn cross_endpoint_node(&self, id: &SymbolId, home_anchor: &SymbolId) -> Result<Option<Node>> {
        // The foreign end is the endpoint that is NOT the home anchor.
        if id == home_anchor {
            return Ok(None);
        }
        self.route_get_node(id)
    }

    /// ROUTE `get_node` by the id's engine. The id string is an estate `SymbolId` for estate and a
    /// uuid_v7 for memory; we try the home engine first (the common case), then each foreign pool.
    /// This is the #2 ROUTE method's core, shared with cross-endpoint hydration.
    fn route_get_node(&self, id: &SymbolId) -> Result<Option<Node>> {
        if let Some(n) = self.home.get_node(id)? {
            return Ok(Some(n));
        }
        if !self.cross_on() {
            return Ok(None);
        }
        for pool in self.others.values() {
            if let Some(n) = pool.get_node_blocking(id)? {
                return Ok(Some(n));
            }
        }
        Ok(None)
    }
}

// ─────────────────────────────────────────────────────────────────────────────
// GraphRead — the 23-method delegation (HOME-ONLY / FOLD / ROUTE per the table above).
// The `H: Sync` bound is required by `GraphRead`'s `Send` supertrait: a borrowing `OverlayReader`
// is `Send` only if `&H: Send`, i.e. `H: Sync` (see the struct doc).
// ─────────────────────────────────────────────────────────────────────────────

impl<H: GraphRead + Sync + ?Sized> GraphRead for OverlayReader<'_, H> {
    // #1 capabilities — HOME (modified): report home caps but force server_side_traversal=false for
    // the cross path (the union is client-side; DEC-X7).
    fn capabilities(&self) -> StoreCapabilities {
        let mut caps = self.home.capabilities();
        if self.cross_on() {
            caps.server_side_traversal = false;
        }
        caps
    }

    // #2 get_node — ROUTE by engine tag.
    fn get_node(&self, id: &SymbolId) -> Result<Option<Node>> {
        self.route_get_node(id)
    }

    // #3 find_symbols — HOME-ONLY (search must not pull foreign nodes into results; DoD-X8).
    fn find_symbols(&self, query: &SymbolQuery) -> Result<Vec<Node>> {
        self.home.find_symbols(query)
    }

    // #4 neighbors — FOLD (gated): home neighbors ∪ epoch-validated xedge rows. The about-arm's core.
    fn neighbors(&self, id: &SymbolId, dir: Direction) -> Result<Vec<Edge>> {
        let mut edges = self.home.neighbors(id, dir)?;
        edges.extend(self.cross_neighbors(id, dir)?);
        Ok(edges)
    }

    // #5 traverse — FOLD: traverse(start) ≡ traverse_multi(&[start]) (DEC-X7).
    fn traverse(&self, start: &SymbolId, spec: &TraversalSpec) -> Result<Subgraph> {
        self.traverse_multi(std::slice::from_ref(start), spec)
    }

    // #6 traverse_multi — FOLD (gated): home multi-seed CTE (via the home's own traverse_multi
    // specialization) THEN one cross ply folding xedge rows for each seed, hydrating the foreign
    // endpoint node. Bounded by spec.max_nodes (home) + budget.max_cross_nodes (cross) + epoch.
    fn traverse_multi(&self, starts: &[SymbolId], spec: &TraversalSpec) -> Result<Subgraph> {
        // Home expansion: delegate to the home engine's (specialized) multi-seed traversal.
        let mut sub = self.home.traverse_multi(starts, spec)?;
        if !self.cross_on() || self.budget.max_cross_hops == 0 {
            return Ok(sub);
        }

        // One cross ply (DEC-X4 default max_cross_hops=1): for each home anchor, fold the
        // cross-store edges + hydrate the foreign endpoint node, depth = anchor_depth + 1.
        let mut seen_nodes: std::collections::HashSet<String> =
            sub.nodes.iter().map(|n| n.symbol.0.clone()).collect();
        let mut seen_edges: std::collections::HashSet<(String, String, String)> =
            sub.edges.iter().map(Edge::dedup_key).collect();
        let mut cross_added = 0usize;

        // Anchor set = the seeds plus everything the home walk reached (depth 0 for seeds).
        let anchors: Vec<(SymbolId, u32)> = sub
            .nodes
            .iter()
            .map(|n| {
                let d = sub.depths.get(&n.symbol.0).copied().unwrap_or(0);
                (n.symbol.clone(), d)
            })
            .collect();

        for (anchor, depth) in anchors {
            if cross_added >= self.budget.max_cross_nodes {
                break;
            }
            let rows = {
                let mut r = self.cross_neighbors(&anchor, Direction::Dependents)?;
                r.extend(self.cross_neighbors(&anchor, Direction::Dependencies)?);
                r
            };
            for edge in rows {
                if cross_added >= self.budget.max_cross_nodes {
                    sub.truncated = true;
                    break;
                }
                let key = edge.dedup_key();
                if !seen_edges.insert(key) {
                    continue;
                }
                // The foreign endpoint is the end that is NOT the anchor.
                let foreign = if edge.source == anchor {
                    &edge.target
                } else {
                    &edge.source
                };
                if let Some(node) = self.cross_endpoint_node(foreign, &anchor)? {
                    if seen_nodes.insert(node.symbol.0.clone()) {
                        sub.depths
                            .entry(node.symbol.0.clone())
                            .and_modify(|d| *d = (*d).min(depth + 1))
                            .or_insert(depth + 1);
                        sub.nodes.push(node);
                        cross_added += 1;
                    }
                }
                sub.edges.push(edge);
            }
        }
        Ok(sub)
    }

    // #7 all_nodes — HOME-ONLY (PageRank stays home — ranked_symbols must not rank foreign; DoD-X8).
    fn all_nodes(&self) -> Result<Vec<Node>> {
        self.home.all_nodes()
    }

    // #8 all_edges — HOME-ONLY (global analytics home-only; DoD-X8).
    fn all_edges(&self) -> Result<Vec<Edge>> {
        self.home.all_edges()
    }

    // #9 unresolved_refs_for_name — HOME-ONLY (unresolved refs are a home-engine concept).
    fn unresolved_refs_for_name(&self, name: &str) -> Result<Vec<UnresolvedRef>> {
        self.home.unresolved_refs_for_name(name)
    }

    // #10 file_digest — HOME-ONLY (file/content reads are home-engine).
    fn file_digest(&self, file: &str) -> Result<Option<String>> {
        self.home.file_digest(file)
    }

    // #11 file_git_sha — HOME-ONLY (home git provenance).
    fn file_git_sha(&self, file: &str) -> Result<Option<String>> {
        self.home.file_git_sha(file)
    }

    // #12 repo_info — HOME-ONLY (home repo metadata).
    fn repo_info(&self) -> Result<Option<RepoInfo>> {
        self.home.repo_info()
    }

    // #13 edge_history — HOME-ONLY (xedge has no history concept).
    fn edge_history(&self, file: &str) -> Result<Vec<HistoricalEdge>> {
        self.home.edge_history(file)
    }

    // #14 file_content — HOME-ONLY (home content store).
    fn file_content(&self, file: &str) -> Result<Option<String>> {
        self.home.file_content(file)
    }

    // #15 symbol_source — HOME-ONLY (home content slice).
    fn symbol_source(&self, node: &Node) -> Result<Option<String>> {
        self.home.symbol_source(node)
    }

    // #16 changes_since — HOME-ONLY (home change-log; xedge has its OWN reconcile cursor).
    fn changes_since(&self, cursor: u64) -> Result<Vec<Change>> {
        self.home.changes_since(cursor)
    }

    // #17 node_semantics — HOME-ONLY (home annotations).
    fn node_semantics(&self, symbol: &SymbolId) -> Result<Option<NodeSemantics>> {
        self.home.node_semantics(symbol)
    }

    // #18 find_by_requirement — HOME-ONLY (home requirement index).
    fn find_by_requirement(&self, requirement: &str) -> Result<Vec<Node>> {
        self.home.find_by_requirement(requirement)
    }

    // #19 annotations — HOME-ONLY (home annotations).
    fn annotations(&self, symbol: &SymbolId) -> Result<Vec<Annotation>> {
        self.home.annotations(symbol)
    }

    // #20 annotations_by_type — HOME-ONLY (home annotations).
    fn annotations_by_type(&self, ty: &str) -> Result<Vec<(SymbolId, Annotation)>> {
        self.home.annotations_by_type(ty)
    }

    // #21 annotations_stale_since — HOME-ONLY (home annotations).
    fn annotations_stale_since(&self, cutoff: i64) -> Result<Vec<(SymbolId, Annotation)>> {
        self.home.annotations_stale_since(cutoff)
    }

    // #22 symbol_epoch — ROUTE by engine: estate → home/foreign symbol_epoch; memory → 0. Used
    // INTERNALLY by #4/#6 to validate xedge rows; exposed here for completeness + the about-arm.
    fn symbol_epoch(&self, id: &SymbolId) -> Result<Option<u64>> {
        // The home engine owns its ids; route to home first (the common path).
        if let Some(g) = self.home.symbol_epoch(id)? {
            return Ok(Some(g));
        }
        // Not a live home id — try foreign pools (a memory id resolves to 0 there).
        if !self.cross_on() {
            return Ok(None);
        }
        for pool in self.others.values() {
            if let Some(g) = pool.symbol_epoch_blocking(id)? {
                return Ok(Some(g));
            }
        }
        Ok(None)
    }

    // #23 stats — HOME-ONLY (home stats).
    fn stats(&self) -> Result<wicked_estate_core::query::GraphStats> {
        self.home.stats()
    }
}
