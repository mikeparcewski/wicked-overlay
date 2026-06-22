//! `xedge.db` — the single-writer cross-store edge overlay (Lane X, T-X-SPEC).
//!
//! DESIGN v3 DEC-1 / D-X1 / D-X2: each engine (estate / memory / knowledge) keeps its OWN file +
//! OWN single writer; cross-domain edges live HERE, in a tiny dedicated single-writer overlay keyed
//! on `(engine, stable-id, epoch)`. This is NOT a [`wicked_estate_core::GraphStore`] (D-X2): it is
//! keyed on `(engine, stable_id, src_epoch, tgt_epoch)`, not a native graph. Rows convert to
//! [`wicked_estate_core::Edge`] via [`Edge::new`] so `edge_json` and every estate formatter work
//! unchanged (design v3 §0 — "xedge rows convert to `core::Edge`").
//!
//! A cross-edge is directed: `(src_engine, src_id @ src_epoch) --rel--> (tgt_engine, tgt_id @ tgt_epoch)`.
//! Following the estate edge-direction invariant (source = dependent, target = dependency), the
//! about-arm row for "memory M is *about* code C" is `(memory, M) --about--> (estate, C)`: the
//! dependency is the code symbol, so [`in_edges`](XedgeReader::in_edges)`("estate", C, ["about"])`
//! surfaces M as a dependent of C — exactly the seed the recall graph-arm walks (DEC-X9).
//!
//! ## Epoch stamping (DEC-X6 / DEC-X6-SEQ)
//! estate's intern is append-only with no generation; a deleted-then-re-added symbol re-creates the
//! SAME [`SymbolId`] string. To keep a stale cross-edge from resolving to a live-WRONG node, each
//! endpoint carries its `symbol_epoch` (estate) / constant 0 (memory, uuid_v7 never reused) at
//! write time, validated at read time (fail-closed on inequality). The published estate epoch is
//! non-vacuous (T-A-EPOCH/M8: g >= 1 after the first delete-then-re-add).

use rusqlite::{Connection, OptionalExtension, params};
use std::sync::{Arc, Mutex};
use wicked_estate_core::{Edge, EdgeKind, Provenance, Result, edge::Confidence, symbol::SymbolId};

/// The on-disk schema version, written into the `meta` table day-one (OQ-X9 — mirrors memory's
/// `MEM_SCHEMA_VERSION`). Bump on any incompatible `xedge` schema change.
pub const XEDGE_SCHEMA_VERSION: i64 = 1;

/// The memory engine's constant epoch. mem-ids are uuid_v7, minted once and never reused, so a
/// memory endpoint never goes stale — its epoch is always 0 (design v3 §1 / DEC-X6.3).
pub const MEMORY_EPOCH: u64 = 0;

/// One side of a cross-edge: an engine-tagged, epoch-stamped stable id.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Endpoint {
    /// The owning engine: `"estate"`, `"memory"`, `"knowledge"`, … (the foreign-pool map key).
    pub engine: String,
    /// The stable id WITHIN that engine ([`SymbolId`] string for estate, uuid_v7 for memory).
    pub stable_id: String,
    /// The endpoint's epoch at write time (estate `symbol_epoch`; 0 for memory).
    pub epoch: u64,
}

impl Endpoint {
    pub fn new(engine: impl Into<String>, stable_id: impl Into<String>, epoch: u64) -> Self {
        Self {
            engine: engine.into(),
            stable_id: stable_id.into(),
            epoch,
        }
    }
}

/// A cross-store edge row: a directed, epoch-stamped, attributed relationship between two engines.
#[derive(Debug, Clone, PartialEq)]
pub struct XEdge {
    pub source: Endpoint,
    pub target: Endpoint,
    /// The relationship name (e.g. `"about"`, `"mentions"`, `"governs"`).
    pub rel: String,
    pub confidence: f32,
    /// Free-form provenance string (the producing redirect / subscriber id).
    pub provenance: String,
}

impl XEdge {
    /// An `about` edge: memory `mem_id` (@0) is *about* estate code `code_id` (@`code_epoch`).
    /// Direction follows the estate invariant — the code symbol is the dependency (target).
    pub fn about(mem_id: impl Into<String>, code_id: impl Into<String>, code_epoch: u64) -> Self {
        Self {
            source: Endpoint::new("memory", mem_id, MEMORY_EPOCH),
            target: Endpoint::new("estate", code_id, code_epoch),
            rel: "about".to_string(),
            confidence: 1.0,
            provenance: "capture_about".to_string(),
        }
    }

    /// Convert this cross-edge to a `core::Edge` so estate formatters (`edge_json`, …) render it
    /// unchanged. The `rel` maps to [`EdgeKind::Other`]; confidence/provenance are carried through
    /// the [`Metadata`](wicked_estate_core::node::Metadata)-free `Synthesizer` provenance so the
    /// "every edge carries {confidence, provenance, resolved_by}" invariant holds.
    pub fn to_core_edge(&self) -> Edge {
        Edge {
            source: SymbolId(self.source.stable_id.clone()),
            target: SymbolId(self.target.stable_id.clone()),
            kind: EdgeKind::Other(self.rel.clone()),
            confidence: Confidence::new(self.confidence),
            provenance: Provenance::Synthesizer(self.provenance.clone()),
            resolved_by: format!("xedge:{}", self.rel),
            location: None,
            metadata: Default::default(),
        }
    }
}

/// The single-writer `xedge.db` store. Holds the only write connection (OQ-X8: single writer; an
/// in-process [`Mutex`] is the advisory lock for this build — a file lock / writer actor is the
/// multi-process upgrade, not load-bearing for the read-union). Clone-cheap [`XedgeReader`]s are
/// minted from it for the read path ([`OverlayReader`](crate::OverlayReader)).
pub struct XedgeStore {
    /// `Arc<Mutex<..>>` so a `reader()` shares the same underlying DB (in-memory or file) — a
    /// second `Connection::open` on `:memory:` would get a DISTINCT empty database.
    conn: Arc<Mutex<Connection>>,
}

impl XedgeStore {
    /// Open (creating if absent) the `xedge.db` at `path`, applying the schema + `meta` row.
    pub fn open(path: &str) -> Result<Self> {
        let conn = Connection::open(path).map_err(map_err)?;
        Self::from_conn(conn)
    }

    /// An in-memory `xedge` overlay (tests, the bench's seeded knowledge arm).
    pub fn in_memory() -> Result<Self> {
        let conn = Connection::open_in_memory().map_err(map_err)?;
        Self::from_conn(conn)
    }

    fn from_conn(conn: Connection) -> Result<Self> {
        conn.execute_batch(
            "PRAGMA journal_mode=WAL;
             CREATE TABLE IF NOT EXISTS meta (
                 key   TEXT PRIMARY KEY,
                 value TEXT NOT NULL
             );
             CREATE TABLE IF NOT EXISTS xedges (
                 src_engine TEXT    NOT NULL,
                 src_id     TEXT    NOT NULL,
                 src_epoch  INTEGER NOT NULL,
                 tgt_engine TEXT    NOT NULL,
                 tgt_id     TEXT    NOT NULL,
                 tgt_epoch  INTEGER NOT NULL,
                 rel        TEXT    NOT NULL,
                 confidence REAL    NOT NULL,
                 provenance TEXT    NOT NULL,
                 PRIMARY KEY (src_engine, src_id, src_epoch, tgt_engine, tgt_id, tgt_epoch, rel)
             );
             -- The read path is keyed by the TARGET endpoint (in_edges: who points AT this id?).
             CREATE INDEX IF NOT EXISTS idx_xedges_tgt ON xedges (tgt_engine, tgt_id, rel);
             CREATE INDEX IF NOT EXISTS idx_xedges_src ON xedges (src_engine, src_id, rel);",
        )
        .map_err(map_err)?;
        // Stamp the schema version day-one (idempotent).
        conn.execute(
            "INSERT INTO meta (key, value) VALUES ('XEDGE_SCHEMA_VERSION', ?1)
             ON CONFLICT(key) DO NOTHING",
            params![XEDGE_SCHEMA_VERSION.to_string()],
        )
        .map_err(map_err)?;
        Ok(Self {
            conn: Arc::new(Mutex::new(conn)),
        })
    }

    /// The persisted schema version (OQ-X9).
    pub fn schema_version(&self) -> Result<i64> {
        let conn = self.conn.lock().expect("xedge mutex poisoned");
        let v: Option<String> = conn
            .query_row(
                "SELECT value FROM meta WHERE key = 'XEDGE_SCHEMA_VERSION'",
                [],
                |r| r.get(0),
            )
            .optional()
            .map_err(map_err)?;
        Ok(v.and_then(|s| s.parse().ok()).unwrap_or(0))
    }

    /// Insert (or replace on the full PK) one cross-edge. Single-writer: the [`Mutex`] serializes
    /// writers in-process. Epoch stamping is the CALLER's responsibility (it holds the live
    /// `symbol_epoch` at put time); the put-time TOCTOU re-validate-before-commit is T-X-EPOCHVAL.
    pub fn put_edge(&self, edge: &XEdge) -> Result<()> {
        let conn = self.conn.lock().expect("xedge mutex poisoned");
        conn.execute(
            "INSERT INTO xedges
                (src_engine, src_id, src_epoch, tgt_engine, tgt_id, tgt_epoch, rel, confidence, provenance)
             VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8, ?9)
             ON CONFLICT (src_engine, src_id, src_epoch, tgt_engine, tgt_id, tgt_epoch, rel)
             DO UPDATE SET confidence = excluded.confidence, provenance = excluded.provenance",
            params![
                edge.source.engine,
                edge.source.stable_id,
                edge.source.epoch as i64,
                edge.target.engine,
                edge.target.stable_id,
                edge.target.epoch as i64,
                edge.rel,
                edge.confidence as f64,
                edge.provenance,
            ],
        )
        .map_err(map_err)?;
        Ok(())
    }

    /// Total cross-edge row count (health / test assertion).
    pub fn len(&self) -> Result<u64> {
        let conn = self.conn.lock().expect("xedge mutex poisoned");
        let n: i64 = conn
            .query_row("SELECT COUNT(*) FROM xedges", [], |r| r.get(0))
            .map_err(map_err)?;
        Ok(n as u64)
    }

    pub fn is_empty(&self) -> Result<bool> {
        Ok(self.len()? == 0)
    }

    /// Mint a cheap, owned, `Send` reader sharing this store's database (D-X2 — the read seam the
    /// `OverlayReader` captures inside the home `with_read` closure).
    pub fn reader(&self) -> XedgeReader {
        XedgeReader {
            conn: Arc::clone(&self.conn),
        }
    }
}

/// A cheap, owned, `Send` read handle over `xedge.db` (D-X2). NOT a `GraphStore`. The
/// [`OverlayReader`](crate::OverlayReader)'s FOLD methods consult it for cross-edges incident to a
/// home id. Reads are epoch-FILTERED at the caller (the overlay validates each row's stamped epoch
/// against the live `symbol_epoch` before folding it in — T-X-EPOCHVAL read-time backstop).
#[derive(Clone)]
pub struct XedgeReader {
    conn: Arc<Mutex<Connection>>,
}

impl XedgeReader {
    /// Cross-edges whose TARGET endpoint is `(engine, id)` and whose `rel` is in `rels`
    /// (empty `rels` = ANY rel). These are the *dependents* of `(engine, id)` across the store
    /// boundary — the about-arm's `in_edges("estate", code_id, ["about"])` read (DEC-X9).
    pub fn in_edges(&self, engine: &str, id: &str, rels: &[&str]) -> Result<Vec<XEdge>> {
        self.query_by_endpoint(EndpointSide::Target, engine, id, rels)
    }

    /// Cross-edges whose SOURCE endpoint is `(engine, id)` and whose `rel` is in `rels`
    /// (empty `rels` = ANY rel) — the *dependencies* of `(engine, id)` across the boundary.
    pub fn out_edges(&self, engine: &str, id: &str, rels: &[&str]) -> Result<Vec<XEdge>> {
        self.query_by_endpoint(EndpointSide::Source, engine, id, rels)
    }

    fn query_by_endpoint(
        &self,
        side: EndpointSide,
        engine: &str,
        id: &str,
        rels: &[&str],
    ) -> Result<Vec<XEdge>> {
        let (engine_col, id_col) = match side {
            EndpointSide::Target => ("tgt_engine", "tgt_id"),
            EndpointSide::Source => ("src_engine", "src_id"),
        };
        let conn = self.conn.lock().expect("xedge mutex poisoned");
        // Build the rel filter as an inline IN-list of bound params (rels are small + trusted).
        let mut sql = format!(
            "SELECT src_engine, src_id, src_epoch, tgt_engine, tgt_id, tgt_epoch, rel, confidence, provenance
             FROM xedges WHERE {engine_col} = ?1 AND {id_col} = ?2"
        );
        let mut bound: Vec<Box<dyn rusqlite::ToSql>> =
            vec![Box::new(engine.to_string()), Box::new(id.to_string())];
        if !rels.is_empty() {
            let placeholders: Vec<String> =
                (0..rels.len()).map(|i| format!("?{}", i + 3)).collect();
            sql.push_str(&format!(" AND rel IN ({})", placeholders.join(", ")));
            for r in rels {
                bound.push(Box::new(r.to_string()));
            }
        }
        let mut stmt = conn.prepare(&sql).map_err(map_err)?;
        let param_refs: Vec<&dyn rusqlite::ToSql> = bound.iter().map(|b| b.as_ref()).collect();
        let rows = stmt
            .query_map(param_refs.as_slice(), |row| {
                Ok(XEdge {
                    source: Endpoint::new(
                        row.get::<_, String>(0)?,
                        row.get::<_, String>(1)?,
                        row.get::<_, i64>(2)? as u64,
                    ),
                    target: Endpoint::new(
                        row.get::<_, String>(3)?,
                        row.get::<_, String>(4)?,
                        row.get::<_, i64>(5)? as u64,
                    ),
                    rel: row.get::<_, String>(6)?,
                    confidence: row.get::<_, f64>(7)? as f32,
                    provenance: row.get::<_, String>(8)?,
                })
            })
            .map_err(map_err)?;
        let mut out = Vec::new();
        for r in rows {
            out.push(r.map_err(map_err)?);
        }
        Ok(out)
    }
}

enum EndpointSide {
    Source,
    Target,
}

fn map_err(e: rusqlite::Error) -> wicked_estate_core::Error {
    wicked_estate_core::Error::Invalid(format!("xedge: {e}"))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn schema_version_stamped_day_one() {
        let store = XedgeStore::in_memory().unwrap();
        assert_eq!(store.schema_version().unwrap(), XEDGE_SCHEMA_VERSION);
    }

    /// T-X-SPEC schema correctness: round-trip a row through `put_edge` → `in_edges` → `core::Edge`,
    /// and assert the converted edge carries confidence + provenance + resolved_by (the estate
    /// "every edge" invariant) so `edge_json` renders it.
    #[test]
    fn put_edge_in_edges_roundtrip_to_core_edge() {
        let store = XedgeStore::in_memory().unwrap();
        let e = XEdge::about("mem-uuid-1", "estate-code-1", 3);
        store.put_edge(&e).unwrap();

        let reader = store.reader();
        let got = reader
            .in_edges("estate", "estate-code-1", &["about"])
            .unwrap();
        assert_eq!(got.len(), 1, "exactly one about edge targets the code id");
        assert_eq!(got[0], e);
        assert_eq!(got[0].target.epoch, 3, "estate endpoint epoch round-trips");
        assert_eq!(
            got[0].source.epoch, MEMORY_EPOCH,
            "memory endpoint is epoch 0"
        );

        // Converts to a core::Edge with the full {confidence, provenance, resolved_by} triple.
        let ce = got[0].to_core_edge();
        assert_eq!(ce.source, SymbolId("mem-uuid-1".into()));
        assert_eq!(ce.target, SymbolId("estate-code-1".into()));
        assert_eq!(ce.kind, EdgeKind::Other("about".into()));
        assert_eq!(ce.confidence.get(), 1.0);
        assert_eq!(ce.resolved_by, "xedge:about");
        assert!(matches!(ce.provenance, Provenance::Synthesizer(_)));
    }

    #[test]
    fn in_edges_filters_by_rel_and_endpoint() {
        let store = XedgeStore::in_memory().unwrap();
        store.put_edge(&XEdge::about("m1", "c1", 0)).unwrap();
        store
            .put_edge(&XEdge {
                source: Endpoint::new("memory", "m2", 0),
                target: Endpoint::new("estate", "c1", 0),
                rel: "mentions".into(),
                confidence: 0.5,
                provenance: "t".into(),
            })
            .unwrap();
        let reader = store.reader();

        // rel filter: only the about edge.
        let only_about = reader.in_edges("estate", "c1", &["about"]).unwrap();
        assert_eq!(only_about.len(), 1);
        assert_eq!(only_about[0].rel, "about");

        // empty rels = any rel: both edges.
        let any = reader.in_edges("estate", "c1", &[]).unwrap();
        assert_eq!(any.len(), 2);

        // wrong endpoint: none.
        let none = reader.in_edges("estate", "c-other", &["about"]).unwrap();
        assert!(none.is_empty());

        // out_edges from the memory source side.
        let out = reader.out_edges("memory", "m1", &["about"]).unwrap();
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].target.stable_id, "c1");
    }

    /// The full PK includes the epoch, so the SAME (engine,id) at a DIFFERENT epoch is a distinct
    /// row — this is what lets a re-add (epoch bump) keep the old stale row addressable for
    /// fail-closed drop without clobbering it.
    #[test]
    fn epoch_is_part_of_the_key() {
        let store = XedgeStore::in_memory().unwrap();
        store.put_edge(&XEdge::about("m1", "c1", 0)).unwrap();
        store.put_edge(&XEdge::about("m1", "c1", 1)).unwrap();
        assert_eq!(
            store.len().unwrap(),
            2,
            "different tgt_epoch => distinct rows"
        );
    }

    /// `reader()` shares the store's database (the Arc<Mutex<Connection>> seam) — a reader minted
    /// before OR after a write sees the write. Guards against the `:memory:` "distinct empty DB"
    /// trap a second `Connection::open` would hit.
    #[test]
    fn reader_shares_the_store_database() {
        let store = XedgeStore::in_memory().unwrap();
        let reader = store.reader(); // minted BEFORE the write
        store.put_edge(&XEdge::about("m1", "c1", 0)).unwrap();
        let got = reader.in_edges("estate", "c1", &["about"]).unwrap();
        assert_eq!(got.len(), 1, "reader sees writes through the shared Arc");
    }
}
