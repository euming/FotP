//! P5 Search Cache — corpus version, query normalization, and cache key
//! construction.
//!
//! This module provides the building blocks for caching semantic search results
//! using GNUISNGNU Layer 4.  The cache key is derived from a **normalized query
//! string** combined with the **corpus version hash** so that:
//!
//! * Semantically equivalent queries (`"memory graph"` / `"graph memory"`) map to
//!   the same cache entry.
//! * Cached results are automatically invalidated when the corpus changes (new
//!   sessions ingested) because the corpus version hash changes.
//!
//! # Functions (A1)
//!
//! * [`compute_corpus_version`] — derives a short stable hash of the corpus state.
//!
//! # Functions (A2)
//!
//! * [`normalize_query`] — canonicalises a raw query string.
//! * [`search_cache_key`] — builds the source identity string used for Layer 4
//!   cache operations (`tool=semantic-search:v1`).

use sha2::{Digest, Sha256};

use crate::store::AmsStore;

// ── Corpus version (A1) ───────────────────────────────────────────────────────

/// Object kinds treated as session objects when computing the corpus version.
const SESSION_KINDS: &[&str] = &["session_ref", "session"];

/// Result of [`compute_corpus_version`].
pub struct CorpusVersionResult {
    /// First 12 hex characters of `SHA256("{count}:{max_created_at_unix_ms}")`.
    pub corpus_version: String,
    /// Total number of session objects found.
    pub session_count: usize,
}

/// Compute a short, stable corpus-version token.
///
/// The token changes whenever the set of session objects changes (new ingestion),
/// making it suitable as an invalidation key for semantic search caches.
///
/// Algorithm:
/// 1. Scan all `ObjectRecord`s for objects whose `object_kind` is `"session_ref"`
///    or `"session"`.
/// 2. Count them and find the maximum `created_at` value (as Unix milliseconds).
/// 3. Produce `corpus_version = first 12 hex chars of SHA256("{count}:{max_ms}")`.
///
/// Complexity: O(N) where N = total objects in the store.  Sessions are typically
/// a small fraction of the store.
pub fn compute_corpus_version(store: &AmsStore) -> CorpusVersionResult {
    let mut count: usize = 0;
    let mut max_ms: i64 = 0;

    for obj in store.objects().values() {
        if SESSION_KINDS.contains(&obj.object_kind.as_str()) {
            count += 1;
            let ms = obj.created_at.timestamp_millis();
            if ms > max_ms {
                max_ms = ms;
            }
        }
    }

    let input = format!("{count}:{max_ms}");
    let digest = Sha256::digest(input.as_bytes());
    // Each byte formats as two lowercase hex digits; take the first 12 chars.
    let hex: String = digest.iter().map(|b| format!("{b:02x}")).collect();
    let corpus_version = hex[..12].to_string();

    CorpusVersionResult { corpus_version, session_count: count }
}

// ── normalize_query ───────────────────────────────────────────────────────────

/// Normalise a raw query string so that equivalent queries produce the same
/// cache key.
///
/// Algorithm:
/// 1. Lowercase the entire string.
/// 2. Replace every non-alphanumeric character (except spaces) with a space.
/// 3. Split on whitespace.
/// 4. Filter out tokens shorter than 2 characters.
/// 5. Sort tokens alphabetically.
/// 6. Join with a single space.
///
/// # Examples
///
/// ```
/// use ams_core_kernel::search_cache::normalize_query;
///
/// assert_eq!(normalize_query("Dream Clustering!"), "clustering dream");
/// assert_eq!(normalize_query("AMS  memory  graph"), "ams graph memory");
/// assert_eq!(normalize_query("a the"), "the"); // "a" is 1 char → filtered; "the" is kept
/// assert_eq!(normalize_query(""), "");
/// ```
pub fn normalize_query(query: &str) -> String {
    // Step 1: lowercase.
    let lower = query.to_lowercase();

    // Step 2: replace non-alphanumeric (except spaces) with spaces.
    let sanitized: String = lower
        .chars()
        .map(|c| if c.is_alphanumeric() || c == ' ' { c } else { ' ' })
        .collect();

    // Steps 3–5: split, filter short tokens, sort.
    let mut tokens: Vec<&str> = sanitized
        .split_whitespace()
        .filter(|t| t.len() >= 2)
        .collect();
    tokens.sort_unstable();

    // Step 6: join.
    tokens.join(" ")
}

// ── search_cache_key ──────────────────────────────────────────────────────────

/// Build the source identity string used as the cache key for a semantic search
/// operation.
///
/// The key has the form `query:<normalized_query>:<corpus_version>`.
///
/// This string is used as `source_id` in GNUISNGNU Layer 4 cache operations with
/// `tool_id = "semantic-search:v1"`.
///
/// # Arguments
///
/// * `query` — the raw (un-normalised) query string from the caller.
/// * `corpus_version` — a short hex hash identifying the current state of the
///   corpus (see `search-corpus-version` command, implemented in A1).
///
/// # Examples
///
/// ```
/// use ams_core_kernel::search_cache::search_cache_key;
///
/// let key = search_cache_key("Dream Clustering!", "abc123def456");
/// assert!(key.starts_with("query:clustering dream:"));
/// assert!(key.ends_with(":abc123def456"));
/// ```
pub fn search_cache_key(query: &str, corpus_version: &str) -> String {
    format!("query:{}:{}", normalize_query(query), corpus_version)
}

// ── Tool ID constant ──────────────────────────────────────────────────────────

/// The stable tool identity used for all semantic search cache operations.
pub const SEMANTIC_SEARCH_TOOL_ID: &str = "semantic-search:v1";

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::model::ObjectRecord;

    // ── helpers ──────────────────────────────────────────────────────────────

    fn make_store_with_sessions(n: usize) -> AmsStore {
        let mut store = AmsStore::new();
        for i in 0..n {
            let ts = chrono::FixedOffset::east_opt(0)
                .unwrap()
                .with_ymd_and_hms(2024, 1, 1, 0, 0, i as u32)
                .unwrap();
            let obj = ObjectRecord::new(
                format!("session:{i}"),
                "session_ref".to_string(),
                None,
                None,
                Some(ts),
            );
            store.objects_mut().insert(obj.object_id.clone(), obj);
        }
        store
    }

    // --- corpus version (A1) ---

    #[test]
    fn corpus_version_is_12_hex_chars() {
        let store = make_store_with_sessions(3);
        let result = compute_corpus_version(&store);
        assert_eq!(result.session_count, 3);
        assert_eq!(result.corpus_version.len(), 12);
        assert!(
            result.corpus_version.chars().all(|c| c.is_ascii_hexdigit()),
            "version must be lowercase hex: got {}",
            result.corpus_version,
        );
    }

    #[test]
    fn corpus_version_changes_when_session_added() {
        let store3 = make_store_with_sessions(3);
        let store4 = make_store_with_sessions(4);
        let v3 = compute_corpus_version(&store3).corpus_version;
        let v4 = compute_corpus_version(&store4).corpus_version;
        assert_ne!(v3, v4, "version must change when a session is added");
    }

    #[test]
    fn corpus_version_is_deterministic() {
        let store = make_store_with_sessions(3);
        let v1 = compute_corpus_version(&store).corpus_version;
        let v2 = compute_corpus_version(&store).corpus_version;
        assert_eq!(v1, v2, "same store must produce the same version");
    }

    #[test]
    fn corpus_version_empty_store_is_12_hex_chars() {
        let store = AmsStore::new();
        let result = compute_corpus_version(&store);
        assert_eq!(result.session_count, 0);
        assert_eq!(result.corpus_version.len(), 12);
    }

    #[test]
    fn corpus_version_ignores_non_session_objects() {
        let mut store = make_store_with_sessions(2);
        // Add a non-session object — must not change session_count.
        let ts = chrono::FixedOffset::east_opt(0)
            .unwrap()
            .with_ymd_and_hms(2024, 2, 1, 0, 0, 0)
            .unwrap();
        let other = ObjectRecord::new(
            "other:1".to_string(),
            "smartlist".to_string(),
            None,
            None,
            Some(ts),
        );
        store.objects_mut().insert(other.object_id.clone(), other);
        let result = compute_corpus_version(&store);
        assert_eq!(result.session_count, 2, "non-session objects must not be counted");
    }

    // --- normalize_query ---

    #[test]
    fn normalize_lowercases_and_sorts() {
        assert_eq!(normalize_query("Dream Clustering!"), "clustering dream");
    }

    #[test]
    fn normalize_collapses_whitespace() {
        assert_eq!(normalize_query("AMS  memory  graph"), "ams graph memory");
    }

    #[test]
    fn normalize_filters_single_char_tokens() {
        // "a" is length 1 — filtered. "the" is length 3 — kept.
        assert_eq!(normalize_query("a the"), "the");
    }

    #[test]
    fn normalize_filters_all_short_tokens() {
        // All tokens are single characters; result is empty.
        assert_eq!(normalize_query("a b c"), "");
    }

    #[test]
    fn normalize_empty_string() {
        assert_eq!(normalize_query(""), "");
    }

    #[test]
    fn normalize_single_long_token() {
        assert_eq!(normalize_query("memory"), "memory");
    }

    #[test]
    fn normalize_replaces_punctuation_with_space() {
        // Punctuation becomes spaces, then whitespace is collapsed.
        assert_eq!(normalize_query("foo-bar_baz"), "bar baz foo");
    }

    #[test]
    fn normalize_order_independent() {
        // "memory graph" and "graph memory" should produce the same output.
        assert_eq!(normalize_query("memory graph"), normalize_query("graph memory"));
    }

    #[test]
    fn normalize_deduplicates_via_sort() {
        // Duplicated tokens still appear; we do not deduplicate, only sort.
        let result = normalize_query("ams ams memory");
        assert_eq!(result, "ams ams memory");
    }

    // --- search_cache_key ---

    #[test]
    fn cache_key_contains_normalized_query_and_version() {
        let key = search_cache_key("Dream Clustering!", "abc123def456");
        assert_eq!(key, "query:clustering dream:abc123def456");
    }

    #[test]
    fn cache_key_order_independent() {
        let k1 = search_cache_key("memory graph", "v1");
        let k2 = search_cache_key("graph memory", "v1");
        assert_eq!(k1, k2);
    }

    #[test]
    fn cache_key_empty_query() {
        let key = search_cache_key("", "deadbeef1234");
        assert_eq!(key, "query::deadbeef1234");
    }

    #[test]
    fn cache_key_all_single_char_tokens_filtered() {
        // All tokens are single characters; normalised query is empty.
        let key = search_cache_key("a b c", "v99");
        assert_eq!(key, "query::v99");
    }
}
