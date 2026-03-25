//! P7 FEP Cache Signal — emit cache hit/miss telemetry as `tool-call` Objects.
//!
//! When `semantic-query.py` hits or misses the search cache it calls the
//! `fep-cache-signal-emit` CLI command (provided by [`emit_cache_signal_command`]).
//! That command writes a `tool-call` Object whose provenance includes:
//!
//! | field            | meaning                                      |
//! |------------------|----------------------------------------------|
//! | `tool_name`      | `"semantic-search:cache-signal"`             |
//! | `signal_kind`    | `"search-cache-signal"`                      |
//! | `cache_status`   | `"hit"` or `"miss"`                          |
//! | `query_normalized` | The normalised query (from [`search_cache::normalize_query`]) |
//! | `corpus_version` | The 12-hex-char corpus-version token         |
//! | `ts`             | ISO 8601 timestamp                           |
//! | `actor_id`       | Actor that triggered the search              |
//! | `is_error`       | Always `false` — the signal itself is not an error |
//! | `result_preview` | Short human-readable summary line            |
//!
//! The dream-schedule (A2) reads these objects to compute per-cluster miss rates
//! and prioritises re-dreaming high-miss clusters, closing the P3<->P7 feedback loop.

use std::collections::BTreeMap;

use anyhow::Result;
use chrono::{DateTime, Duration, FixedOffset};
use serde::{Deserialize, Serialize};

use crate::model::{JsonMap, SemanticPayload};
use crate::search_cache::normalize_query;
use crate::store::AmsStore;

/// Default sliding window in hours for [`cache_signal_stats`].
pub const DEFAULT_WINDOW_HOURS: u32 = 24;

/// Tool name used for all cache signal `tool-call` records.
pub const CACHE_SIGNAL_TOOL_NAME: &str = "semantic-search:cache-signal";

/// Signal kind field value stored in provenance.
pub const CACHE_SIGNAL_KIND: &str = "search-cache-signal";

/// Emit a `tool-call` Object recording a cache hit or miss for a semantic search.
///
/// # Arguments
///
/// * `store`          - mutable AMS store (write transaction in progress)
/// * `query`          - raw (un-normalised) query string
/// * `corpus_version` - 12-hex-char corpus-version token (from [`compute_corpus_version`])
/// * `is_hit`         - `true` for a cache hit, `false` for a miss
/// * `actor_id`       - identity of the actor/agent that triggered the search
/// * `now`            - wall-clock time for the event (injected for determinism)
pub fn emit_cache_signal(
    store: &mut AmsStore,
    query: &str,
    corpus_version: &str,
    is_hit: bool,
    actor_id: &str,
    now: DateTime<FixedOffset>,
) -> Result<()> {
    use uuid::Uuid;

    let object_id = format!("tool-call:{}", Uuid::new_v4().simple());
    let norm = normalize_query(query);
    let status = if is_hit { "hit" } else { "miss" };
    let result_preview = format!(
        "cache_status={status} query_normalized={norm} corpus_version={corpus_version}"
    );

    let mut prov: JsonMap = JsonMap::new();
    prov.insert("tool_name".to_string(), serde_json::Value::String(CACHE_SIGNAL_TOOL_NAME.to_string()));
    prov.insert("signal_kind".to_string(), serde_json::Value::String(CACHE_SIGNAL_KIND.to_string()));
    prov.insert("cache_status".to_string(), serde_json::Value::String(status.to_string()));
    prov.insert("query_normalized".to_string(), serde_json::Value::String(norm));
    prov.insert("corpus_version".to_string(), serde_json::Value::String(corpus_version.to_string()));
    prov.insert("is_error".to_string(), serde_json::Value::Bool(false));
    prov.insert("result_preview".to_string(), serde_json::Value::String(result_preview));
    prov.insert("ts".to_string(), serde_json::Value::String(now.to_rfc3339()));
    prov.insert("actor_id".to_string(), serde_json::Value::String(actor_id.to_string()));

    let sp = SemanticPayload { provenance: Some(prov), ..Default::default() };
    store.upsert_object(object_id, "tool-call", None, Some(sp), Some(now))?;
    Ok(())
}

// ── Stats (A2) ────────────────────────────────────────────────────────────────

/// Per-tool statistics returned by [`cache_signal_stats`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ToolSignalStats {
    /// Tool name (e.g. `"semantic-search:cache-signal"`).
    pub tool_name: String,
    /// Hit signals within the window.
    pub hit_count: usize,
    /// Miss signals within the window.
    pub miss_count: usize,
    /// Total signals within the window (`hit_count + miss_count`).
    pub total: usize,
    /// Hit fraction: `hit_count / total`, or `0.0` when `total == 0`.
    pub hit_rate: f64,
    /// Consecutive misses at the head of the newest-first sorted sequence (i.e.
    /// the trailing miss streak).  Equals `total` when no hit is present.
    pub consecutive_misses: usize,
    /// Oldest signal timestamp in the window, `None` if empty.
    pub oldest_signal_utc: Option<DateTime<FixedOffset>>,
    /// Newest signal timestamp in the window, `None` if empty.
    pub newest_signal_utc: Option<DateTime<FixedOffset>>,
}

/// Result of [`cache_signal_stats`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct CacheSignalStatsResult {
    /// Per-tool stats, sorted alphabetically by `tool_name`.
    pub tools: Vec<ToolSignalStats>,
}

/// Aggregate cache signal statistics over a sliding time window.
///
/// Scans all `tool-call` objects whose provenance contains
/// `signal_kind = "search-cache-signal"`, filters to those created within
/// `now - window_hours`, groups by `tool_name`, and computes hit/miss
/// statistics for each group.
///
/// # Arguments
///
/// * `store`        — read-only AMS store.
/// * `tool_filter`  — if `Some`, only return stats for that tool name.
/// * `window_hours` — sliding window size; signals older than
///                    `now - window_hours` are excluded.
/// * `now`          — reference timestamp for the window boundary.
pub fn cache_signal_stats(
    store: &AmsStore,
    tool_filter: Option<&str>,
    window_hours: u32,
    now: DateTime<FixedOffset>,
) -> CacheSignalStatsResult {
    let cutoff = now - Duration::hours(i64::from(window_hours));

    // Collect (tool_name, created_at, cache_status) for in-window signals.
    let mut by_tool: BTreeMap<String, Vec<(DateTime<FixedOffset>, String)>> = BTreeMap::new();

    for obj in store.objects().values() {
        if obj.object_kind != "tool-call" {
            continue;
        }
        let Some(ref sp) = obj.semantic_payload else { continue };
        let Some(ref prov) = sp.provenance else { continue };

        // Must be a cache signal.
        let sig_kind = prov.get("signal_kind").and_then(|v| v.as_str()).unwrap_or("");
        if sig_kind != CACHE_SIGNAL_KIND {
            continue;
        }

        // Apply tool filter.
        let tool_name = prov
            .get("tool_name")
            .and_then(|v| v.as_str())
            .unwrap_or(CACHE_SIGNAL_TOOL_NAME);
        if let Some(filter) = tool_filter {
            if tool_name != filter {
                continue;
            }
        }

        // Apply window filter using object created_at.
        if obj.created_at < cutoff {
            continue;
        }

        let cache_status = prov
            .get("cache_status")
            .and_then(|v| v.as_str())
            .unwrap_or("")
            .to_string();

        by_tool
            .entry(tool_name.to_string())
            .or_default()
            .push((obj.created_at, cache_status));
    }

    // If a filter was requested but no signals exist, return a zero entry.
    if let Some(filter) = tool_filter {
        by_tool.entry(filter.to_string()).or_default();
    }

    let tools = by_tool
        .into_iter()
        .map(|(tool_name, mut signals)| {
            let total = signals.len();
            let hit_count = signals.iter().filter(|(_, s)| s == "hit").count();
            let miss_count = total - hit_count;
            let hit_rate = if total == 0 { 0.0 } else { hit_count as f64 / total as f64 };

            // Consecutive misses: sort newest-first, count leading misses.
            signals.sort_by(|a, b| b.0.cmp(&a.0));
            let consecutive_misses = {
                let mut streak = 0usize;
                let mut found_hit = false;
                for (_, status) in &signals {
                    if status != "miss" {
                        found_hit = true;
                        break;
                    }
                    streak += 1;
                }
                if found_hit { streak } else { total }
            };

            let oldest_signal_utc = signals.iter().map(|(ts, _)| *ts).min();
            let newest_signal_utc = signals.iter().map(|(ts, _)| *ts).max();

            ToolSignalStats {
                tool_name,
                hit_count,
                miss_count,
                total,
                hit_rate,
                consecutive_misses,
                oldest_signal_utc,
                newest_signal_utc,
            }
        })
        .collect();

    CacheSignalStatsResult { tools }
}

// ── Cluster Surprise (B1) ─────────────────────────────────────────────────────

/// Per-cluster surprise entry produced by [`cache_signal_cluster_surprise`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClusterSurpriseEntry {
    /// Cluster ID (e.g. `"cluster-0001"`).
    pub cluster_id: String,
    /// Number of sessions in the cluster SmartList.
    pub session_count: usize,
    /// Cache hit count for signals associated with this cluster.
    pub hit_count: usize,
    /// Cache miss count for signals associated with this cluster.
    pub miss_count: usize,
    /// Miss rate: `miss_count / total`, or `0.5` when no signals.
    pub miss_rate: f64,
    /// Surprise score: `(1.0 - hit_rate) × ln(1 + miss_count)`.
    pub surprise_score: f64,
}

/// Result of [`cache_signal_cluster_surprise`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct ClusterSurpriseResult {
    /// Entries sorted by `surprise_score` descending.
    pub clusters: Vec<ClusterSurpriseEntry>,
}

/// Root path for dream-topics SmartLists.
const DREAM_TOPICS_ROOT: &str = "smartlist/dream-topics";

/// Join cache signals to dream-topic cluster membership and rank clusters by
/// surprise score.
///
/// # Algorithm
///
/// 1. Enumerate all SmartList paths that start with `"smartlist/dream-topics/"`
///    (excluding the root index path) — each is a cluster.
/// 2. Load cache signals within `window_hours` from `store`.
/// 3. For each signal, tokenise `query_normalized` and check if any token
///    appears in the `in_situ_ref` of any session Object that is a member of
///    the cluster.
/// 4. Aggregate per-cluster hit/miss counts; compute
///    `surprise_score = (1.0 - hit_rate) × ln(1 + miss_count)`.
/// 5. Clusters with fewer than `min_signals` are included with `miss_rate=0.5`
///    and `surprise_score=0.0`.
///
/// # Arguments
///
/// * `store`        — read-only AMS store.
/// * `window_hours` — sliding window size for signals (default 24).
/// * `min_signals`  — if `Some(n)`, skip clusters with fewer than `n` signals.
/// * `now`          — reference timestamp for the window boundary.
pub fn cache_signal_cluster_surprise(
    store: &AmsStore,
    window_hours: u32,
    min_signals: Option<usize>,
    now: DateTime<FixedOffset>,
) -> ClusterSurpriseResult {
    // Members container ID prefix for dream-topics clusters.
    // Format: "smartlist-members:smartlist/dream-topics/<cluster-id>"
    let members_prefix = format!("smartlist-members:{}/", DREAM_TOPICS_ROOT);

    let cutoff = now - Duration::hours(i64::from(window_hours));

    // ── Step 1: enumerate clusters ──────────────────────────────────────────
    // Scan all containers for `smartlist-members:smartlist/dream-topics/<id>`.
    let mut cluster_ids: Vec<String> = Vec::new();
    for container_id in store.containers().keys() {
        if let Some(suffix) = container_id.strip_prefix(&members_prefix) {
            if !suffix.is_empty() && !suffix.contains('/') {
                cluster_ids.push(suffix.to_string());
            }
        }
    }
    cluster_ids.sort();

    // ── Step 2: load cache signals within window ────────────────────────────
    struct Signal {
        tokens: Vec<String>,
        is_hit: bool,
    }
    let mut signals: Vec<Signal> = Vec::new();
    for obj in store.objects().values() {
        if obj.object_kind != "tool-call" {
            continue;
        }
        let Some(ref sp) = obj.semantic_payload else { continue };
        let Some(ref prov) = sp.provenance else { continue };
        let sig_kind = prov.get("signal_kind").and_then(|v| v.as_str()).unwrap_or("");
        if sig_kind != CACHE_SIGNAL_KIND {
            continue;
        }
        if obj.created_at < cutoff {
            continue;
        }
        let is_hit = prov.get("cache_status").and_then(|v| v.as_str()).unwrap_or("") == "hit";
        let query_norm = prov
            .get("query_normalized")
            .and_then(|v| v.as_str())
            .unwrap_or("");
        let tokens: Vec<String> = query_norm
            .split_whitespace()
            .filter(|t| !t.is_empty())
            .map(|t| t.to_lowercase())
            .collect();
        signals.push(Signal { tokens, is_hit });
    }

    // ── Step 3–4: per-cluster aggregation ──────────────────────────────────
    let mut entries: Vec<ClusterSurpriseEntry> = Vec::new();

    for cluster_id in &cluster_ids {
        let container_id = format!("{}{}",  members_prefix, cluster_id);

        // Collect session member IDs for this cluster via link nodes.
        let member_ids: Vec<String> = store
            .iterate_forward(&container_id)
            .iter()
            .map(|node| node.object_id.clone())
            .collect();
        let session_count = member_ids.len();

        // Build a single bag of words from all member in_situ_ref strings.
        let mut cluster_text = String::new();
        for mid in &member_ids {
            if let Some(obj) = store.objects().get(mid) {
                if let Some(ref isr) = obj.in_situ_ref {
                    cluster_text.push(' ');
                    cluster_text.push_str(isr);
                }
                if let Some(ref sp) = obj.semantic_payload {
                    if let Some(ref summary) = sp.summary {
                        cluster_text.push(' ');
                        cluster_text.push_str(summary);
                    }
                }
            }
        }
        let cluster_text_lower = cluster_text.to_lowercase();

        // Count hits and misses for signals whose tokens match this cluster.
        let mut hit_count = 0usize;
        let mut miss_count = 0usize;
        for sig in &signals {
            let matches = !sig.tokens.is_empty()
                && sig.tokens.iter().any(|t| cluster_text_lower.contains(t.as_str()));
            if matches {
                if sig.is_hit {
                    hit_count += 1;
                } else {
                    miss_count += 1;
                }
            }
        }

        let total = hit_count + miss_count;

        // Apply min_signals filter.
        if let Some(min) = min_signals {
            if total < min {
                continue;
            }
        }

        let (miss_rate, surprise_score) = if total == 0 {
            (0.5, 0.0)
        } else {
            let mr = miss_count as f64 / total as f64;
            let ss = mr * (1.0 + miss_count as f64).ln();
            (mr, ss)
        };

        entries.push(ClusterSurpriseEntry {
            cluster_id: cluster_id.clone(),
            session_count,
            hit_count,
            miss_count,
            miss_rate,
            surprise_score,
        });
    }

    // Sort descending by surprise_score.
    entries.sort_by(|a, b| b.surprise_score.partial_cmp(&a.surprise_score).unwrap_or(std::cmp::Ordering::Equal));

    ClusterSurpriseResult { clusters: entries }
}

/// Return a `BTreeMap<cluster_id, surprise_score>` for use by the dream
/// scheduler.  Clusters with `total == 0` signals are omitted.
pub fn cache_signal_cluster_surprise_map(
    store: &AmsStore,
    window_hours: u32,
    now: DateTime<FixedOffset>,
) -> BTreeMap<String, f64> {
    let result = cache_signal_cluster_surprise(store, window_hours, None, now);
    result
        .clusters
        .into_iter()
        .filter(|e| e.hit_count + e.miss_count > 0)
        .map(|e| (e.cluster_id, e.surprise_score))
        .collect()
}

// ── Cache Report (C3) ─────────────────────────────────────────────────────────

/// A single line in the Dream Schedule Preview section of a cache report.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DreamSchedulePreviewEntry {
    /// Cluster ID.
    pub cluster_id: String,
    /// Session count in the cluster.
    pub session_count: usize,
    /// Signal-derived surprise score (from cache signals).
    pub signal_surprise: f64,
}

/// Recommendation kind produced by [`fep_cache_report`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub enum CacheReportRecommendation {
    /// A cluster has a high miss rate and should be re-dreamed.
    WarnHighMissRate {
        cluster_id: String,
        miss_rate: f64,
    },
    /// All clusters have a good hit rate.
    OkCacheIsWarm,
}

/// Full report produced by [`fep_cache_report`].
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct FepCacheReport {
    /// Per-tool signal stats (section 1).
    pub signal_stats: CacheSignalStatsResult,
    /// Top-10 clusters by surprise score (section 2).
    pub top_clusters: Vec<ClusterSurpriseEntry>,
    /// Clusters sorted by signal_surprise for dream schedule preview (section 3).
    pub dream_schedule_preview: Vec<DreamSchedulePreviewEntry>,
    /// Operator recommendations (section 4).
    pub recommendations: Vec<CacheReportRecommendation>,
}

/// Generate the full FEP cache report.
///
/// Combines [`cache_signal_stats`], [`cache_signal_cluster_surprise`], and a
/// lightweight dream-schedule preview into a single report struct.
///
/// # Arguments
///
/// * `store`        — read-only AMS store.
/// * `window_hours` — sliding window size for signals (default 24).
/// * `now`          — reference timestamp.
pub fn fep_cache_report(
    store: &AmsStore,
    window_hours: u32,
    now: DateTime<FixedOffset>,
) -> FepCacheReport {
    // Section 1: signal stats for the primary tool.
    let signal_stats =
        cache_signal_stats(store, Some(CACHE_SIGNAL_TOOL_NAME), window_hours, now);

    // Section 2: cluster surprise ranking (top 10).
    let surprise_result = cache_signal_cluster_surprise(store, window_hours, None, now);
    let top_clusters: Vec<ClusterSurpriseEntry> =
        surprise_result.clusters.iter().take(10).cloned().collect();

    // Section 3: dream schedule preview — clusters with signal data, sorted by
    // signal_surprise descending (mirrors what dream_schedule would use).
    let dream_schedule_preview: Vec<DreamSchedulePreviewEntry> = surprise_result
        .clusters
        .iter()
        .filter(|e| e.hit_count + e.miss_count > 0)
        .map(|e| DreamSchedulePreviewEntry {
            cluster_id: e.cluster_id.clone(),
            session_count: e.session_count,
            signal_surprise: e.surprise_score,
        })
        .collect();

    // Section 4: recommendations.
    let mut recommendations: Vec<CacheReportRecommendation> = Vec::new();
    for entry in &surprise_result.clusters {
        if entry.miss_rate > 0.7 && entry.session_count > 2 {
            recommendations.push(CacheReportRecommendation::WarnHighMissRate {
                cluster_id: entry.cluster_id.clone(),
                miss_rate: entry.miss_rate,
            });
        }
    }
    if recommendations.is_empty() {
        // Only emit OK if all clusters with signals have good hit rate.
        let all_warm = signal_stats.tools.iter().all(|s| s.hit_rate > 0.8 || s.total == 0);
        if all_warm {
            recommendations.push(CacheReportRecommendation::OkCacheIsWarm);
        }
    }

    FepCacheReport {
        signal_stats,
        top_clusters,
        dream_schedule_preview,
        recommendations,
    }
}

// -- Tests --------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use chrono::TimeZone;

    use super::*;
    use crate::smartlist_write::attach_member as smartlist_attach;

    fn fixed_now() -> DateTime<FixedOffset> {
        chrono::FixedOffset::east_opt(0)
            .unwrap()
            .with_ymd_and_hms(2026, 1, 1, 0, 0, 0)
            .unwrap()
    }

    #[test]
    fn emit_cache_hit_creates_tool_call_object() {
        let mut store = AmsStore::new();
        emit_cache_signal(&mut store, "dream clustering", "abc123def456", true, "test-actor", fixed_now()).unwrap();
        let objects: Vec<_> = store.objects().values()
            .filter(|o| o.object_kind == "tool-call")
            .collect();
        assert_eq!(objects.len(), 1);
        let prov = objects[0].semantic_payload.as_ref().unwrap().provenance.as_ref().unwrap();
        assert_eq!(prov["cache_status"], serde_json::Value::String("hit".to_string()));
        assert_eq!(prov["signal_kind"], serde_json::Value::String(CACHE_SIGNAL_KIND.to_string()));
        assert_eq!(prov["corpus_version"], serde_json::Value::String("abc123def456".to_string()));
    }

    #[test]
    fn emit_cache_miss_records_miss_status() {
        let mut store = AmsStore::new();
        emit_cache_signal(&mut store, "memory graph", "deadbeef0001", false, "agent-1", fixed_now()).unwrap();
        let objects: Vec<_> = store.objects().values()
            .filter(|o| o.object_kind == "tool-call")
            .collect();
        assert_eq!(objects.len(), 1);
        let prov = objects[0].semantic_payload.as_ref().unwrap().provenance.as_ref().unwrap();
        assert_eq!(prov["cache_status"], serde_json::Value::String("miss".to_string()));
    }

    #[test]
    fn emit_normalizes_query_in_provenance() {
        let mut store = AmsStore::new();
        emit_cache_signal(&mut store, "Dream Clustering!", "v1v1v1v1v1v1", false, "agent", fixed_now()).unwrap();
        let objects: Vec<_> = store.objects().values().collect();
        let prov = objects[0].semantic_payload.as_ref().unwrap().provenance.as_ref().unwrap();
        // normalize_query("Dream Clustering!") => "clustering dream"
        assert_eq!(prov["query_normalized"], serde_json::Value::String("clustering dream".to_string()));
    }

    #[test]
    fn multiple_signals_create_separate_objects() {
        let mut store = AmsStore::new();
        emit_cache_signal(&mut store, "query one", "corpus001", true, "a", fixed_now()).unwrap();
        emit_cache_signal(&mut store, "query two", "corpus001", false, "a", fixed_now()).unwrap();
        let count = store.objects().values()
            .filter(|o| o.object_kind == "tool-call")
            .count();
        assert_eq!(count, 2);
    }

    // ── cache_signal_stats tests ──────────────────────────────────────────────

    fn emit_signal_at(store: &mut AmsStore, is_hit: bool, offset_secs: i64) {
        let ts = fixed_now()
            + chrono::Duration::seconds(offset_secs);
        emit_cache_signal(store, "test query", "cv001", is_hit, "test", ts).unwrap();
    }

    #[test]
    fn stats_hit_rate_5_hits_2_misses() {
        let mut store = AmsStore::new();
        for i in 0..5i64 { emit_signal_at(&mut store, true, i); }
        for i in 5..7i64 { emit_signal_at(&mut store, false, i); }
        let now = fixed_now() + chrono::Duration::seconds(100);
        let result = cache_signal_stats(&store, None, 24, now);
        assert_eq!(result.tools.len(), 1);
        let s = &result.tools[0];
        assert_eq!(s.hit_count, 5);
        assert_eq!(s.miss_count, 2);
        assert_eq!(s.total, 7);
        assert!((s.hit_rate - 5.0 / 7.0).abs() < 0.001, "hit_rate={}", s.hit_rate);
    }

    #[test]
    fn stats_excludes_old_signals() {
        let mut store = AmsStore::new();
        // Signal 48h ago — outside 24h window.
        let old = fixed_now() - chrono::Duration::hours(48);
        emit_cache_signal(&mut store, "old query", "cv001", false, "test", old).unwrap();
        // Signal at now — inside window.
        emit_cache_signal(&mut store, "new query", "cv001", true, "test", fixed_now()).unwrap();
        let now = fixed_now() + chrono::Duration::seconds(1);
        let result = cache_signal_stats(&store, None, 24, now);
        let s = &result.tools[0];
        assert_eq!(s.total, 1, "old signal must be excluded");
        assert_eq!(s.hit_count, 1);
    }

    #[test]
    fn stats_no_signals_returns_zero_for_tool_filter() {
        let store = AmsStore::new();
        let result = cache_signal_stats(
            &store, Some(CACHE_SIGNAL_TOOL_NAME), 24, fixed_now(),
        );
        assert_eq!(result.tools.len(), 1);
        let s = &result.tools[0];
        assert_eq!(s.total, 0);
        assert_eq!(s.hit_rate, 0.0);
    }

    #[test]
    fn stats_tool_filter_restricts_output() {
        let mut store = AmsStore::new();
        emit_signal_at(&mut store, true, 0);
        let now = fixed_now() + chrono::Duration::seconds(100);
        // Filter to the actual tool name — should return 1 entry.
        let result = cache_signal_stats(&store, Some(CACHE_SIGNAL_TOOL_NAME), 24, now);
        assert_eq!(result.tools.len(), 1);
        assert_eq!(result.tools[0].tool_name, CACHE_SIGNAL_TOOL_NAME);
    }

    #[test]
    fn stats_consecutive_misses_trailing_streak() {
        let mut store = AmsStore::new();
        // Newest-first after sort: miss(+2), miss(+1), hit(+0) → streak=2
        emit_signal_at(&mut store, true, 0);
        emit_signal_at(&mut store, false, 1);
        emit_signal_at(&mut store, false, 2);
        let now = fixed_now() + chrono::Duration::seconds(100);
        let result = cache_signal_stats(&store, None, 24, now);
        assert_eq!(result.tools[0].consecutive_misses, 2);
    }

    #[test]
    fn stats_consecutive_misses_all_misses_equals_total() {
        let mut store = AmsStore::new();
        emit_signal_at(&mut store, false, 0);
        emit_signal_at(&mut store, false, 1);
        let now = fixed_now() + chrono::Duration::seconds(100);
        let result = cache_signal_stats(&store, None, 24, now);
        let s = &result.tools[0];
        assert_eq!(s.consecutive_misses, s.total);
    }

    // ── cache_signal_cluster_surprise tests ───────────────────────────────────

    fn make_session(store: &mut AmsStore, object_id: &str, text: &str) {
        let mut sp = crate::model::SemanticPayload::default();
        sp.summary = Some(text.to_string());
        store.upsert_object(
            object_id.to_string(),
            "session",
            Some(text.to_string()),
            Some(sp),
            Some(fixed_now()),
        ).unwrap();
    }

    #[test]
    fn cluster_surprise_two_clusters_high_miss_first() {
        let mut store = AmsStore::new();
        let now = fixed_now();
        let by = "test";

        // Create two clusters with sessions having distinct keywords.
        make_session(&mut store, "s1", "memory graph topology");
        smartlist_attach(&mut store, "dream-topics/cluster-0001", "s1", by, now).unwrap();

        make_session(&mut store, "s2", "route replay planning");
        smartlist_attach(&mut store, "dream-topics/cluster-0002", "s2", by, now).unwrap();

        // Emit signals for cluster-1 topic (80% miss rate: 1 hit, 4 misses).
        emit_cache_signal(&mut store, "memory graph", "cv001", true, "a", now).unwrap();
        for _ in 0..4 {
            emit_cache_signal(&mut store, "memory graph", "cv001", false, "a", now).unwrap();
        }

        let later = now + chrono::Duration::seconds(100);
        let result = cache_signal_cluster_surprise(&store, 24, None, later);

        // Both clusters should be present; cluster-0001 should have higher surprise.
        assert!(!result.clusters.is_empty(), "expected at least one cluster");
        let c1 = result.clusters.iter().find(|e| e.cluster_id == "cluster-0001");
        let c2 = result.clusters.iter().find(|e| e.cluster_id == "cluster-0002");
        assert!(c1.is_some(), "cluster-0001 not found");
        let c1 = c1.unwrap();
        // cluster-0001 matched signals → non-zero surprise
        assert!(c1.surprise_score > 0.0, "cluster-0001 should have positive surprise");
        // cluster-0002 had no matching signals → surprise=0.0
        if let Some(c2) = c2 {
            assert!(c1.surprise_score > c2.surprise_score,
                "cluster-0001 surprise ({}) should exceed cluster-0002 surprise ({})",
                c1.surprise_score, c2.surprise_score);
        }
        // First entry should be cluster-0001 (highest surprise).
        assert_eq!(result.clusters[0].cluster_id, "cluster-0001");
    }

    #[test]
    fn cluster_surprise_min_signals_filter() {
        let mut store = AmsStore::new();
        let now = fixed_now();
        let by = "test";

        make_session(&mut store, "s1", "dream clustering");
        smartlist_attach(&mut store, "dream-topics/cluster-0001", "s1", by, now).unwrap();

        // Only 1 signal — filter with min_signals=2 should exclude it.
        emit_cache_signal(&mut store, "dream clustering", "cv001", false, "a", now).unwrap();

        let later = now + chrono::Duration::seconds(10);
        let result = cache_signal_cluster_surprise(&store, 24, Some(2), later);
        assert!(result.clusters.is_empty(), "min_signals filter should exclude cluster with only 1 signal");
    }

    #[test]
    fn cluster_surprise_no_signals_returns_empty() {
        let store = AmsStore::new();
        let result = cache_signal_cluster_surprise(&store, 24, None, fixed_now());
        assert!(result.clusters.is_empty(), "no clusters, no output");
    }
}
