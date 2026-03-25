//! Generate CLAUDE.local.md from the topology-based dream cluster Atlas.
//!
//! ## What is dream-generate-md?
//!
//! `dream_generate_md` reads the topic cluster Atlas built by `dream_cluster`
//! and produces a human-readable Markdown file (typically `CLAUDE.local.md`)
//! that gives Claude immediate context at session start without re-reading
//! source files.
//!
//! ## Algorithm
//!
//! 1. Read `smartlist/dream-topics` (scale-0 root index) — get ordered topic
//!    Objects (ranked by cluster size, largest first).
//! 2. For each topic (up to 10), read its per-cluster SmartList
//!    (`smartlist/dream-topics/<cluster-id>`) to get member session GUIDs.
//! 3. Collect all sessions across all clusters; sort by `created_at`
//!    descending for the ## Recent Sessions section (top 20).
//! 4. Write the Markdown output with three sections:
//!    - `## Recent Sessions` — top-20 sessions newest-first
//!    - `## Key Topics` — top-10 clusters with label + top-3 session titles
//!    - `## Drill-Down` — standard AMS wrapper command reference

use anyhow::Result;
use chrono::{DateTime, FixedOffset};

use crate::dream_cluster::DREAM_TOPICS_ROOT;
use crate::store::AmsStore;

// ── Result type ───────────────────────────────────────────────────────────────

/// Output of `dream_generate_md`.
#[derive(Clone, Debug)]
pub struct DreamGenerateMdResult {
    /// Number of topic clusters included in the output.
    pub topics_written: usize,
    /// Number of sessions included in the Recent Sessions section.
    pub sessions_written: usize,
    /// The rendered Markdown text.
    pub markdown: String,
}

// ── Core function ─────────────────────────────────────────────────────────────

/// Generate a Markdown memory surface from the dream cluster Atlas.
///
/// This function is **read-only** — it does not mutate the store.
///
/// # Arguments
///
/// * `store`    — AMS store (read-only)
/// * `now_utc`  — current wall-clock time (used in the file header)
/// * `max_topics`   — maximum number of topic clusters to include (default: 10)
/// * `max_sessions` — maximum number of recent sessions to include (default: 20)
pub fn dream_generate_md(
    store: &AmsStore,
    now_utc: DateTime<FixedOffset>,
    max_topics: usize,
    max_sessions: usize,
) -> Result<DreamGenerateMdResult> {
    // ── Step 1: walk smartlist/dream-topics to get ordered topic object IDs ───
    // Members are stored under the "smartlist-members:" container, not the bucket itself.
    let root_members_container = format!("smartlist-members:{}", DREAM_TOPICS_ROOT);
    let topic_nodes = store.iterate_forward(&root_members_container);
    let topic_object_ids: Vec<String> = topic_nodes
        .iter()
        .map(|ln| ln.object_id.clone())
        .take(max_topics)
        .collect();

    // ── Step 2: for each topic, collect per-cluster sessions ──────────────────
    struct TopicInfo {
        label: String,
        cluster_id: String,
        sessions: Vec<SessionInfo>,
    }
    struct SessionInfo {
        object_id: String,
        in_situ_ref: Option<String>,
        created_at: DateTime<FixedOffset>,
    }

    let mut topics: Vec<TopicInfo> = Vec::new();
    let mut all_sessions: Vec<SessionInfo> = Vec::new();
    let mut seen_session_ids = std::collections::HashSet::new();

    for topic_object_id in &topic_object_ids {
        // Derive cluster_id from object ID: "topic:cluster-0001" → "cluster-0001"
        let cluster_id = topic_object_id
            .strip_prefix("topic:")
            .unwrap_or(topic_object_id.as_str())
            .to_string();

        // Get the human-readable label from the topic object's semantic payload.
        let label = store
            .objects()
            .get(topic_object_id.as_str())
            .and_then(|o| o.semantic_payload.as_ref())
            .and_then(|sp| sp.summary.as_deref())
            .unwrap_or(&cluster_id)
            .to_string();

        // Walk the per-cluster SmartList to get session GUIDs (ordered by link centrality).
        let cluster_smartlist = format!("smartlist-members:{}/{}", DREAM_TOPICS_ROOT, cluster_id);
        let session_nodes = store.iterate_forward(&cluster_smartlist);

        let mut cluster_sessions: Vec<SessionInfo> = Vec::new();
        for ln in &session_nodes {
            let session_id = &ln.object_id;
            let obj = store.objects().get(session_id.as_str());
            let in_situ_ref = obj.and_then(|o| o.in_situ_ref.clone());
            let created_at = obj
                .map(|o| o.created_at)
                .unwrap_or(now_utc);

            cluster_sessions.push(SessionInfo {
                object_id: session_id.clone(),
                in_situ_ref: in_situ_ref.clone(),
                created_at,
            });

            // Also accumulate into global session list (deduplicated).
            if seen_session_ids.insert(session_id.clone()) {
                all_sessions.push(SessionInfo {
                    object_id: session_id.clone(),
                    in_situ_ref,
                    created_at,
                });
            }
        }

        topics.push(TopicInfo {
            label,
            cluster_id,
            sessions: cluster_sessions,
        });
    }

    // ── Step 3: sort all sessions by created_at descending ───────────────────
    all_sessions.sort_by(|a, b| b.created_at.cmp(&a.created_at));
    all_sessions.truncate(max_sessions);

    // ── Step 4: render Markdown ───────────────────────────────────────────────
    let now_str = now_utc.format("%Y-%m-%d %H:%M").to_string();
    let mut md = String::new();

    md.push_str(&format!(
        "<!--\n  AUTO-GENERATED -- do not edit by hand.\n  Tool   : ams-core-kernel dream-generate-md\n  Updated: {}\n-->\n\n",
        now_str
    ));
    md.push_str("# AI Memory: NetworkGraphMemory\n\n");
    md.push_str("This file is auto-generated from Claude Code session history using AMS Dreaming. ");
    md.push_str("It gives Claude immediate, token-efficient context at the start of each session ");
    md.push_str("without re-reading source files.\n\n");

    // ## Recent Sessions
    md.push_str("## Recent Sessions\n");
    md.push_str("Most recent development sessions (newest first):\n");
    for s in &all_sessions {
        let date_str = s.created_at.format("%Y-%m-%d").to_string();
        let title = s
            .in_situ_ref
            .as_deref()
            .unwrap_or(&s.object_id);
        // Truncate very long titles to ~120 chars for readability.
        let truncated = if title.len() > 120 {
            format!("{}…", &title[..120])
        } else {
            title.to_string()
        };
        md.push_str(&format!("- **{}**: {}\n", date_str, truncated));
    }
    md.push('\n');

    // ## Key Topics
    md.push_str("## Key Topics\n");
    md.push_str("Recurring themes (use `/memory-search <topic>` for details):\n");
    for topic in &topics {
        // Show top-3 session titles under each topic.
        let top_sessions: Vec<&str> = topic
            .sessions
            .iter()
            .take(3)
            .map(|s| {
                s.in_situ_ref
                    .as_deref()
                    .unwrap_or(s.object_id.as_str())
            })
            .collect();

        md.push_str(&format!("- **{}** (`{}`)", topic.label, topic.cluster_id));
        if !top_sessions.is_empty() {
            md.push_str(": ");
            let snippets: Vec<String> = top_sessions
                .iter()
                .map(|t| {
                    let trimmed = if t.len() > 60 {
                        format!("{}…", &t[..60])
                    } else {
                        t.to_string()
                    };
                    format!("_{}_", trimmed)
                })
                .collect();
            md.push_str(&snippets.join("; "));
        }
        md.push('\n');
    }
    md.push('\n');

    // ## Drill-Down
    md.push_str("## Drill-Down\n");
    md.push_str("To inspect memory, use the AMS wrapper contract:\n");
    md.push_str("```\n");
    md.push_str("  scripts\\ams.bat thread                         # current task graph / active thread\n");
    md.push_str("  scripts\\ams.bat handoff                        # cross-agent handoff memory\n");
    md.push_str("  scripts\\ams.bat search \"<task keywords>\"      # normal front-path retrieval\n");
    md.push_str("  scripts\\ams.bat recall \"<latent keywords>\"    # latent/background-memory-inclusive retrieval\n");
    md.push_str("  scripts\\ams.bat sessions --n 20               # recent sessions\n");
    md.push_str("  scripts\\ams.bat read <guid-prefix>            # full session text\n");
    md.push_str("```\n");
    md.push_str("Full reference: `docs/agent-memory-tools.md`\n");

    let topics_written = topics.len();
    let sessions_written = all_sessions.len();

    Ok(DreamGenerateMdResult {
        topics_written,
        sessions_written,
        markdown: md,
    })
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::dream_cluster::{dream_cluster, DEFAULT_MAX_CLUSTERS, DEFAULT_MIN_JACCARD};
    use crate::model::now_fixed;
    use crate::smartlist_write::attach_member;
    use crate::store::AmsStore;

    fn make_store_with_clusters() -> AmsStore {
        let mut store = AmsStore::new();
        let now = now_fixed();
        let by = "test";

        // Create 6 sessions with overlapping containers → 3 clusters.
        // Sessions A+B share smartlist list-x
        // Sessions C+D share smartlist list-y
        // Sessions E+F share smartlist list-z
        // Use attach_member (smartlist-members: prefix) so dream_cluster can detect them.
        let sessions = [
            ("sess-a", "topics/list-x"),
            ("sess-b", "topics/list-x"),
            ("sess-c", "topics/list-y"),
            ("sess-d", "topics/list-y"),
            ("sess-e", "topics/list-z"),
            ("sess-f", "topics/list-z"),
        ];
        for (sess_id, list_path) in &sessions {
            store
                .upsert_object(*sess_id, "session", Some(format!("Title for {}", sess_id)), None, Some(now))
                .unwrap();
            attach_member(&mut store, list_path, sess_id, by, now).unwrap();
        }

        // Run dream_cluster to materialise topic SmartLists.
        dream_cluster(&mut store, DEFAULT_MIN_JACCARD, DEFAULT_MAX_CLUSTERS, by, now).unwrap();

        store
    }

    #[test]
    fn dream_generate_md_produces_key_topics_section() {
        let store = make_store_with_clusters();
        let now = now_fixed();
        let result = dream_generate_md(&store, now, 10, 20).unwrap();

        assert!(
            result.markdown.contains("## Key Topics"),
            "output must contain '## Key Topics'"
        );
        assert!(
            result.topics_written >= 1,
            "at least one topic cluster must be written"
        );
    }

    #[test]
    fn dream_generate_md_produces_recent_sessions_section() {
        let store = make_store_with_clusters();
        let now = now_fixed();
        let result = dream_generate_md(&store, now, 10, 20).unwrap();

        assert!(
            result.markdown.contains("## Recent Sessions"),
            "output must contain '## Recent Sessions'"
        );
        assert!(
            result.sessions_written >= 1,
            "at least one session must be written"
        );
    }

    #[test]
    fn dream_generate_md_is_valid_utf8() {
        let store = make_store_with_clusters();
        let now = now_fixed();
        let result = dream_generate_md(&store, now, 10, 20).unwrap();

        // String::from_utf8 is implicit since Rust Strings are always UTF-8.
        // We just verify it round-trips through bytes without loss.
        let bytes = result.markdown.as_bytes();
        let reparsed = std::str::from_utf8(bytes).unwrap();
        assert_eq!(result.markdown, reparsed);
    }
}
