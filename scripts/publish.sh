#!/usr/bin/env bash
# Publish wicked-overlay to crates.io (single crate).
#
# Prereq:
#   * `cargo login <token>` (a crates.io API token), or CARGO_REGISTRY_TOKEN in the env.
#   * wicked-estate-core AND wicked-estate-store must ALREADY be on crates.io at 0.12.0
#     (the versions this crate pins) — i.e. publish wicked-estate FIRST.
#
# crates.io publishes are IRREVERSIBLE (yank, never delete). Bump [package] version in Cargo.toml
# before re-publishing a changed crate.
#
# Usage:
#   ./scripts/publish.sh             # real publish (uploads to crates.io)
#   ./scripts/publish.sh --dry-run   # package without uploading
set -euo pipefail
cd "$(dirname "$0")/.."

DRY="${1:-}"
# --allow-dirty: cargo regenerates Cargo.lock during the verify build; the lock isn't part of a
# library's published package, so the upload still matches the tagged source.
PUB=(cargo publish --allow-dirty)

if [ "$DRY" = "--dry-run" ]; then
  "${PUB[@]}" --dry-run --no-verify 2>&1 ||
    echo "    (dry-run can't resolve not-yet-published estate deps — validated at real publish)"
  exit 0
fi

# Resumable + rate-limit-aware: a NEW crate hits crates.io's ~1-per-10-min new-crate limit; retry.
for attempt in $(seq 1 30); do
  if "${PUB[@]}" 2>/tmp/wo-publish.err; then echo "published wicked-overlay."; exit 0; fi
  if grep -qiE "already uploaded|already exists" /tmp/wo-publish.err; then
    echo "    already published — skipping"; exit 0
  fi
  if grep -qi "429 Too Many Requests" /tmp/wo-publish.err; then
    echo "    rate-limited (attempt $attempt) — waiting 120s"; sleep 120
  else
    echo "    ERROR publishing wicked-overlay:"; cat /tmp/wo-publish.err; exit 1
  fi
done
echo "    gave up after 30 retries"; exit 1
