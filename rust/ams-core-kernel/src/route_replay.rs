use std::fs;
use std::path::Path;

use anyhow::{Context, Result};
use serde::Serialize;

use crate::context::{build_query_context, QueryContextOptions};
use crate::corpus::MaterializedCorpus;
use crate::retrieval::{query_cards, QueryOptions};
use crate::route_memory::{
    canonical_target_ref, load_route_replay_records, RouteMemoryBiasOptions, RouteMemoryStore, RouteReplayRecord,
};

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct RouteReplayOutput {
    pub case_index: usize,
    pub query: String,
    pub baseline_hits: Vec<String>,
    pub replay_hits: Vec<String>,
    pub replay_surface: Vec<String>,
    pub baseline_weak: bool,
    pub replay_weak: bool,
    pub baseline_scope_lens: String,
    pub replay_scope_lens: String,
    pub top1_changed: bool,
    pub top1_baseline: Option<String>,
    pub top1_replay: Option<String>,
    pub delta: String,
    pub expected_refs_hit: Option<bool>,
    pub replay_explain_paths: Vec<String>,
    pub route_memory_signal_present: bool,
}

pub fn run_route_replay(
    corpus: &MaterializedCorpus,
    records: &[RouteReplayRecord],
    default_top: usize,
    bias_options: &RouteMemoryBiasOptions,
) -> Result<Vec<RouteReplayOutput>> {
    let mut outputs = Vec::with_capacity(records.len());
    for (index, record) in records.iter().enumerate() {
        let context = build_query_context(
            corpus,
            &QueryContextOptions {
                current_node_id: record.current_node.clone(),
                parent_node_id: record.parent_node.clone(),
                grandparent_node_id: record.grandparent_node.clone(),
                agent_role: record.role.clone(),
                mode: record.mode.clone(),
                failure_bucket: None,
                active_artifacts: Vec::new(),
                traversal_budget: 3,
                no_active_thread_context: record.no_active_thread_context,
            },
        )?;

        let query_options = QueryOptions {
            top: if record.top > 0 { record.top } else { default_top },
            ..QueryOptions::default()
        };
        let baseline = query_cards(corpus, &record.query, &query_options, context.as_ref(), None, None);
        let route_memory = RouteMemoryStore::from_episodes(record.episodes.clone());
        let replay = query_cards(
            corpus,
            &record.query,
            &query_options,
            context.as_ref(),
            Some(&route_memory),
            Some(bias_options),
        );

        let baseline_hits = baseline.iter().map(|hit| hit.card_id.to_string()).collect::<Vec<_>>();
        let replay_hits = replay.iter().map(|hit| hit.card_id.to_string()).collect::<Vec<_>>();
        let replay_surface = replay_hits.clone();
        let top1_baseline = baseline_hits.first().cloned();
        let top1_replay = replay_hits.first().cloned();
        let top1_changed = top1_baseline != top1_replay;

        let expected_refs = record
            .expected_refs
            .as_ref()
            .map(|refs| refs.iter().map(|value| canonical_target_ref(value)).collect::<Vec<_>>());
        let expected_refs_hit = expected_refs.as_ref().map(|expected_refs| {
            replay_surface
                .iter()
                .map(|value| canonical_target_ref(value))
                .any(|value| expected_refs.contains(&value))
        });

        let replay_explain_paths = replay.iter().map(|hit| hit.scope_reason.clone()).collect::<Vec<_>>();
        let route_memory_signal_present = replay_explain_paths
            .iter()
            .any(|path| path.contains("route-memory:reuse") || path.contains("route-memory:suppress"));

        outputs.push(RouteReplayOutput {
            case_index: index,
            query: record.query.clone(),
            baseline_hits,
            replay_hits,
            replay_surface,
            baseline_weak: baseline.is_empty(),
            replay_weak: replay.is_empty(),
            baseline_scope_lens: context
                .as_ref()
                .map(|value| value.scope_lens().to_string())
                .unwrap_or_else(|| "global".to_string()),
            replay_scope_lens: context
                .as_ref()
                .map(|value| value.scope_lens().to_string())
                .unwrap_or_else(|| "global".to_string()),
            top1_changed,
            top1_baseline: top1_baseline.clone(),
            top1_replay: top1_replay.clone(),
            delta: compute_replay_delta(
                baseline.is_empty(),
                replay.is_empty(),
                top1_baseline.as_deref(),
                top1_replay.as_deref(),
                expected_refs.as_deref(),
            ),
            expected_refs_hit,
            replay_explain_paths,
            route_memory_signal_present,
        });
    }
    Ok(outputs)
}

pub fn write_route_replay_outputs(path: &Path, outputs: &[RouteReplayOutput]) -> Result<()> {
    let mut lines = String::new();
    for output in outputs {
        lines.push_str(&serde_json::to_string(output)?);
        lines.push('\n');
    }
    fs::write(path, lines).with_context(|| format!("failed to write route-replay output '{}'", path.display()))
}

pub fn load_and_run_route_replay(
    corpus: &MaterializedCorpus,
    replay_path: &Path,
    out_path: &Path,
    default_top: usize,
    bias_options: &RouteMemoryBiasOptions,
) -> Result<Vec<RouteReplayOutput>> {
    let records = load_route_replay_records(replay_path)?;
    let outputs = run_route_replay(corpus, &records, default_top, bias_options)?;
    write_route_replay_outputs(out_path, &outputs)?;
    Ok(outputs)
}

fn compute_replay_delta(
    baseline_weak: bool,
    replay_weak: bool,
    top1_baseline: Option<&str>,
    top1_replay: Option<&str>,
    expected_refs: Option<&[String]>,
) -> String {
    let top1_changed = top1_baseline != top1_replay;
    if baseline_weak && !replay_weak {
        return "improved-weak".to_string();
    }
    if !baseline_weak && replay_weak {
        return "regressed-weak".to_string();
    }

    if top1_changed {
        if let (Some(top1_baseline), Some(top1_replay), Some(expected_refs)) = (top1_baseline, top1_replay, expected_refs)
        {
            let baseline_hit = expected_refs.contains(&canonical_target_ref(top1_baseline));
            let replay_hit = expected_refs.contains(&canonical_target_ref(top1_replay));
            if replay_hit && !baseline_hit {
                return "top1-promoted".to_string();
            }
            if !replay_hit && baseline_hit {
                return "top1-demoted".to_string();
            }
        }
        return "reorder".to_string();
    }

    "no-change".to_string()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use uuid::Uuid;

    use crate::corpus::{BinderRecord, CardPayloadRecord, CardRecord, CardState, MaterializedCorpus, TagLinkMeta, TagLinkRecord};
    use crate::route_memory::{RouteReplayEpisodeEntry, RouteReplayEpisodeInput, RouteReplayFrameInput, RouteReplayRouteInput};

    use super::*;

    fn make_corpus() -> MaterializedCorpus {
        let local = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap();
        let broad = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2").unwrap();
        let binder = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        MaterializedCorpus {
            db_path: PathBuf::from("fixture.memory.jsonl"),
            snapshot_path: None,
            snapshot: None,
            cards: BTreeMap::from([
                (
                    local,
                    CardRecord {
                        card_id: local,
                        state: CardState::Active,
                        state_reason: None,
                    },
                ),
                (
                    broad,
                    CardRecord {
                        card_id: broad,
                        state: CardState::Active,
                        state_reason: None,
                    },
                ),
            ]),
            binders: BTreeMap::from([(
                binder,
                BinderRecord {
                    binder_id: binder,
                    name: "Topic: Retrieval".to_string(),
                },
            )]),
            tag_links: BTreeMap::from([
                (
                    (local, binder),
                    TagLinkRecord {
                        card_id: local,
                        binder_id: binder,
                        meta: TagLinkMeta {
                            relevance: 0.61,
                            reason: None,
                            added_by: None,
                            created_at: None,
                        },
                    },
                ),
                (
                    (broad, binder),
                    TagLinkRecord {
                        card_id: broad,
                        binder_id: binder,
                        meta: TagLinkMeta {
                            relevance: 0.8,
                            reason: None,
                            added_by: None,
                            created_at: None,
                        },
                    },
                ),
            ]),
            payloads: BTreeMap::from([
                (
                    local,
                    CardPayloadRecord {
                        card_id: local,
                        title: Some("Local retrieval cache contract".to_string()),
                        text: Some("Exact lineage route memory should win.".to_string()),
                        source: Some("fixture".to_string()),
                        updated_at: None,
                    },
                ),
                (
                    broad,
                    CardPayloadRecord {
                        card_id: broad,
                        title: Some("Broad retrieval cache contract".to_string()),
                        text: Some("Project-wide route memory often over-matches.".to_string()),
                        source: Some("fixture".to_string()),
                        updated_at: None,
                    },
                ),
            ]),
            unknown_record_types: BTreeMap::new(),
        }
    }

    #[test]
    fn run_route_replay_promotes_expected_top1_and_exposes_signal() {
        let local = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap();
        let broad = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2").unwrap();
        let record = RouteReplayRecord {
            query: "retrieval cache contract".to_string(),
            top: 2,
            current_node: Some("child-thread".to_string()),
            parent_node: None,
            grandparent_node: None,
            role: Some("implementer".to_string()),
            mode: Some("build".to_string()),
            no_active_thread_context: true,
            episodes: vec![RouteReplayEpisodeEntry {
                frame: RouteReplayFrameInput {
                    scope_lens: "local-first-lineage".to_string(),
                    agent_role: "implementer".to_string(),
                    mode: "build".to_string(),
                    lineage_node_ids: vec!["child-thread".to_string()],
                    artifact_refs: None,
                    failure_bucket: None,
                },
                route: RouteReplayRouteInput {
                    ranking_source: "raw-lesson".to_string(),
                    path: "retrieval-graph:self-thread -> in-bucket".to_string(),
                    cost: 0.5,
                    risk_flags: Some(vec![]),
                },
                episode: RouteReplayEpisodeInput {
                    query_text: "retrieval cache contract".to_string(),
                    occurred_at: chrono::DateTime::parse_from_rfc3339("2026-03-10T08:00:00+00:00").unwrap(),
                    weak_result: false,
                    used_fallback: false,
                    winning_target_ref: local.to_string(),
                    top_target_refs: vec![local.to_string(), broad.to_string()],
                    user_feedback: None,
                    tool_outcome: None,
                },
                candidate_target_refs: vec![local.to_string(), broad.to_string()],
                winning_target_ref: local.to_string(),
            }],
            expected_refs: Some(vec![local.to_string()]),
        };

        let outputs = run_route_replay(
            &make_corpus(),
            &[record],
            2,
            &RouteMemoryBiasOptions::default(),
        )
        .unwrap();

        assert_eq!(outputs.len(), 1);
        assert_eq!(outputs[0].delta, "top1-promoted");
        let local_ref = local.to_string();
        assert_eq!(outputs[0].top1_replay.as_deref(), Some(local_ref.as_str()));
        assert!(outputs[0].route_memory_signal_present);
    }
}
