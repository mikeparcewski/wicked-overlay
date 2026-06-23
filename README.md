```
          _      _            _                           _             
__      _(_) ___| | _____  __| |       _____   _____ _ __| | __ _ _   _ 
\ \ /\ / / |/ __| |/ / _ \/ _` |_____ / _ \ \ / / _ \ '__| |/ _` | | | |
 \ V  V /| | (__|   <  __/ (_| |_____| (_) \ V /  __/ |  | | (_| | |_| |
  \_/\_/ |_|\___|_|\_\___|\__,_|      \___/ \_/ \___|_|  |_|\__,_|\__, |
                                                                  |___/ 
```

**The cross-store overlay — the bridge that turns three separate engines into one graph at read time.**
`OverlayReader` unions a **home** engine (the [`wicked-estate`](https://github.com/mikeparcewski/wicked-estate)
code graph) with one or more **foreign** engines ([`wicked-memory`](https://github.com/mikeparcewski/wicked-memory),
[`wicked-knowledge`](https://github.com/mikeparcewski/wicked-knowledge)) over `about` cross-edges — so a
query can start at a **code symbol** and surface a grounding doc that lives in a *different* store, **with
zero lexical overlap** between the query and the doc. That's the differentiator a flat keyword/BM25 brain
structurally cannot do.

[![CI](https://github.com/mikeparcewski/wicked-overlay/actions/workflows/ci.yml/badge.svg)](https://github.com/mikeparcewski/wicked-overlay/actions/workflows/ci.yml)
[![License: MIT](https://img.shields.io/badge/license-MIT-blue.svg)](./LICENSE)

> **Status:** v0.12.0 — published on crates.io. estate-dep-only. Its own tests + the cross-store landing
> gate (in `wicked-memory`) are green; clippy `-D warnings` clean.

---

## What it brings together

```
                  wicked-estate  (code graph)        ← HOME: symbols · calls · blast-radius · scoped context
                         ▲
                         │   about   (xedge cross-edges — epoch-stamped, {confidence, provenance})
                         │
        wicked-memory / wicked-knowledge             ← FOREIGN: experiential memory · curated, citable docs
```

Each engine keeps its **own** store (DEC-1 — code, memory, and knowledge never co-mingle). The overlay
owns only the `about` edges *between* them (`xedge.db`) and folds the foreign side in when a `GraphRead`
traversal runs over the home. Recall, ranking, and search stay native to each engine — the overlay just
makes **"what memory/knowledge is this code about?"** a single graph hop.

## How it works

- **`OverlayReader<H: GraphRead>`** — implements estate's `GraphRead` over a home + foreign engines. Graph
  reads (`neighbors` / `traverse` / `get_node` / …) fold **epoch-validated** `about` edges and hydrate the
  matched nodes from the foreign engine. Keyword/vector/search stay **home-only**, so a foreign node is
  reachable *only* through the cross fold — it never leaks into the home's own ranking.
- **`XedgeStore` / `XEdge`** — the single-writer `xedge.db` of `about` cross-edges.
  `XEdge::about(mem_id, code_id, code_epoch)` stamps the code symbol's epoch, so a **stale** edge (the code
  changed underneath it) **fails closed** rather than surfacing a wrong link.
- **`ForeignEngine`** — the object-safe seam a foreign store implements (a `SqlitePool` does), so the reader
  can drive it from the home's thread without deadlock (`with_read_inline` / `Handle::block_on`).

## Who consumes it

- **[`wicked-memory`](https://github.com/mikeparcewski/wicked-memory)** (`--features cross`) —
  `cross::OverlayMemStore`: a `MemoryEngine` runs over the overlay for **ranked** cross-store recall.
- **[`wicked-knowledge`](https://github.com/mikeparcewski/wicked-knowledge)** — the agent-facing tools
  `knowledge.relate_code` (link a doc to code symbols) and `knowledge.recall_about_code` (recall knowledge
  **from a code seed**) are built directly on `XedgeStore`.

## Use as a library

```toml
wicked-overlay = "0.12"
```
```rust
use wicked_overlay::{OverlayReader, XEdge, XedgeStore, ForeignPools, CrossBudget};

// LINK: a knowledge/memory node is `about` a code symbol (no text change to either side).
xedge.put_edge(&XEdge::about(doc_id, code_symbol_id, code_epoch))?;

// READ: a GraphRead over home (code) + foreign (knowledge) that folds the `about` edge.
let reader = OverlayReader::new(
    &home, "estate", xedge.reader(), foreign_pools,
    vec!["about".into()],          // cross-edge kinds to fold ([] = home-only baseline)
    CrossBudget::default(),
);
// drive `reader` as a GraphRead: a traversal from the code seed hydrates the grounding doc.
```

## The wicked engines

| Engine | Role |
|---|---|
| [`wicked-estate`](https://github.com/mikeparcewski/wicked-estate) | the code graph (HOME) |
| [`wicked-memory`](https://github.com/mikeparcewski/wicked-memory) | 5-tier experiential memory |
| [`wicked-knowledge`](https://github.com/mikeparcewski/wicked-knowledge) | curated, citable knowledge |
| **wicked-overlay** | the cross-store bridge (**this crate**) |

## License

MIT — see [LICENSE](./LICENSE).
