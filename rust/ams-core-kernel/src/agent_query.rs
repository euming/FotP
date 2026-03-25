use anyhow::Result;
use chrono::Utc;
use serde::Serialize;

use crate::context::{build_query_context, QueryContextOptions};
use crate::corpus::{CardState, MaterializedCorpus};
use crate::freshness::{prepare_snapshot_freshness, touch_snapshot_freshness, FreshnessWriteAction};
use crate::lesson_retrieval::rank_snapshot_lessons;
use crate::retrieval::{parse_seed_card, query_cards, QueryOptions, RetrievalHit};
use crate::route_memory::{
    build_frame_fingerprint, classify_tool_outcome, RouteMemoryBiasOptions, RouteMemoryStore,
    RouteReplayEpisodeEntry, RouteReplayEpisodeInput, RouteReplayFrameInput, RouteReplayRouteInput,
};
use crate::short_term::{select_manual_smartlist_docs, select_short_term_hits, AgentShortTermHit};

#[derive(Clone, Debug)]
pub struct AgentQueryRequest {
    pub query: String,
    pub top: usize,
    pub binder_filters: Vec<String>,
    pub seed_card: Option<String>,
    pub state_filter: Option<CardState>,
    pub include_retracted: bool,
    pub explain: bool,
    pub context_options: QueryContextOptions,
    pub route_memory: Option<RouteMemoryStore>,
    pub route_memory_bias_options: RouteMemoryBiasOptions,
    pub include_latent: bool,
    pub touch: bool,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AgentQueryDiagnostics {
    pub scoring_lane: String,
    pub routing_decision: String,
    pub scope_lens: String,
    pub routing_flags: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AgentQueryExplain {
    pub ranking_source: String,
    pub matched_tokens: Vec<String>,
    pub score_breakdown: AgentQueryScoreBreakdown,
    pub path: String,
    pub why_won: String,
    pub risk_flags: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AgentQueryScoreBreakdown {
    pub text: f64,
    pub binder: f64,
    pub meta: f64,
    pub context: f64,
    pub route_memory: f64,
    pub free_energy: Option<f64>,
    pub final_score: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AgentQueryHit {
    pub title: String,
    pub score: f64,
    pub snippet: Option<String>,
    pub source_ref: String,
    pub binder_names: Vec<String>,
    pub explain: Option<AgentQueryExplain>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AgentFallbackSummary {
    pub title: String,
    pub snippet: String,
    pub source_kind: String,
    pub source_ref: String,
    pub score: f64,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct AgentQueryResult {
    pub markdown: String,
    pub hits: Vec<AgentQueryHit>,
    pub weak_result: bool,
    pub touched_lessons: usize,
    pub freshness_admissions: usize,
    pub short_term_hits: usize,
    pub short_term: Vec<AgentShortTermHit>,
    pub fallback: Vec<AgentFallbackSummary>,
    pub diagnostics: AgentQueryDiagnostics,
    pub route_episode: Option<RouteReplayEpisodeEntry>,
    pub freshness_actions: Vec<FreshnessWriteAction>,
}

pub fn run_agent_query(corpus: &mut MaterializedCorpus, request: &AgentQueryRequest) -> Result<AgentQueryResult> {
    let context = build_query_context(corpus, &request.context_options)?;
    let now_utc = Utc::now().fixed_offset();
    let freshness_prepare = if let Some(snapshot) = corpus.snapshot.as_mut() {
        Some(prepare_snapshot_freshness(snapshot, &request.query, now_utc)?)
    } else {
        None
    };
    let freshness_positions = freshness_prepare
        .as_ref()
        .map(|value| value.positions.clone())
        .unwrap_or_default();
    let frozen_exclusions = freshness_prepare
        .as_ref()
        .map(|value| value.frozen_exclusions.clone())
        .unwrap_or_default();
    let snapshot_lesson_surface = corpus
        .snapshot
        .as_ref()
        .and_then(|snapshot| {
            rank_snapshot_lessons(
                snapshot,
                &request.query,
                request.top,
                context.as_ref(),
                &freshness_positions,
                &frozen_exclusions,
            )
        });

    let (rendered_hits, diagnostics) = if let Some(surface) = snapshot_lesson_surface {
        (surface.hits, surface.diagnostics)
    } else {
        let query_options = QueryOptions {
            top: request.top,
            binder_filters: request.binder_filters.clone(),
            seed_card: parse_seed_card(request.seed_card.as_deref())?,
            state_filter: request.state_filter,
            include_retracted: request.include_retracted,
            ..QueryOptions::default()
        };
        let hits = query_cards(
            corpus,
            &request.query,
            &query_options,
            context.as_ref(),
            request.route_memory.as_ref(),
            Some(&request.route_memory_bias_options),
        );
        let diagnostics = AgentQueryDiagnostics {
            scoring_lane: "raw-lesson".to_string(),
            routing_decision: if hits.is_empty() {
                "semantic-empty".to_string()
            } else {
                "direct".to_string()
            },
            scope_lens: context
                .as_ref()
                .map(|context| context.scope_lens().to_string())
                .unwrap_or_else(|| "global".to_string()),
            routing_flags: Vec::new(),
        };
        let rendered_hits = hits
            .iter()
            .map(|hit| render_hit(hit, request.explain))
            .collect::<Vec<_>>();
        (rendered_hits, diagnostics)
    };
    let short_term_hits = select_short_term_hits(
        corpus,
        &request.query,
        request.top,
        context.as_ref(),
        request.include_latent,
        &freshness_positions,
        &frozen_exclusions,
    );
    let mut freshness_actions = freshness_prepare
        .as_ref()
        .map(|value| value.actions.clone())
        .unwrap_or_default();
    let mut freshness_admissions = freshness_prepare
        .as_ref()
        .map(|value| value.admissions)
        .unwrap_or(0);
    if request.touch {
        let touch_refs = rendered_hits
            .iter()
            .map(|hit| hit.source_ref.clone())
            .chain(short_term_hits.iter().map(|hit| hit.source_ref.clone()))
            .collect::<Vec<_>>();
        if let Some(snapshot) = corpus.snapshot.as_mut() {
            let touched = touch_snapshot_freshness(snapshot, &touch_refs, &request.query, now_utc)?;
            freshness_admissions += touched.admissions;
            freshness_actions.extend(touched.actions);
        }
    }
    let weak_result = rendered_hits.is_empty() && short_term_hits.is_empty();
    let fallback = if rendered_hits.is_empty() {
        select_fallback_summaries(
            corpus,
            &request.query,
            request.top.max(5),
            context.as_ref(),
            request.include_latent,
        )
    } else {
        Vec::new()
    };
    let route_episode = build_route_episode(&request.query, &rendered_hits, &diagnostics, weak_result, context.as_ref());
    let markdown = render_markdown(
        &request.query,
        &rendered_hits,
        &short_term_hits,
        &fallback,
        &diagnostics,
        weak_result,
        freshness_admissions,
    );
    Ok(AgentQueryResult {
        markdown,
        hits: rendered_hits,
        weak_result,
        touched_lessons: 0,
        freshness_admissions,
        short_term_hits: short_term_hits.len(),
        short_term: short_term_hits,
        fallback,
        diagnostics,
        route_episode,
        freshness_actions,
    })
}

fn build_route_episode(
    query: &str,
    hits: &[AgentQueryHit],
    diagnostics: &AgentQueryDiagnostics,
    weak_result: bool,
    context: Option<&crate::context::QueryContext>,
) -> Option<RouteReplayEpisodeEntry> {
    let top_hit = hits.first()?;
    let frame = build_frame_fingerprint(context?)?;
    let winning_target_ref = top_hit.source_ref.clone();
    let risk_flags = top_hit
        .explain
        .as_ref()
        .map(|value| value.risk_flags.clone())
        .unwrap_or_default();
    let mut entry = RouteReplayEpisodeEntry {
        frame: RouteReplayFrameInput {
            scope_lens: frame.scope_lens,
            agent_role: frame.agent_role,
            mode: frame.mode,
            lineage_node_ids: frame.lineage_node_ids,
            artifact_refs: Some(frame.artifact_refs).filter(|items| !items.is_empty()),
            failure_bucket: frame.failure_bucket,
        },
        route: RouteReplayRouteInput {
            ranking_source: top_hit
                .explain
                .as_ref()
                .map(|value| value.ranking_source.clone())
                .unwrap_or_else(|| diagnostics.scoring_lane.clone()),
            path: top_hit
                .explain
                .as_ref()
                .map(|value| value.path.clone())
                .unwrap_or_else(|| diagnostics.routing_decision.clone()),
            cost: (1.0 / (top_hit.score + 1.0)).clamp(0.0, 1.0),
            risk_flags: Some(risk_flags).filter(|items| !items.is_empty()),
        },
        episode: RouteReplayEpisodeInput {
            query_text: query.to_string(),
            occurred_at: Utc::now().fixed_offset(),
            weak_result,
            used_fallback: false,
            winning_target_ref: winning_target_ref.clone(),
            top_target_refs: hits.iter().map(|hit| hit.source_ref.clone()).collect(),
            user_feedback: None,
            tool_outcome: None,
        },
        candidate_target_refs: hits.iter().map(|hit| hit.source_ref.clone()).collect(),
        winning_target_ref,
    };
    entry.episode.tool_outcome = Some(classify_tool_outcome(&entry));
    Some(entry)
}

fn render_hit(hit: &RetrievalHit, include_explain: bool) -> AgentQueryHit {
    let title = hit
        .payload
        .as_ref()
        .and_then(|payload| payload.title.clone())
        .unwrap_or_else(|| hit.card_id.to_string());
    let snippet = hit
        .payload
        .as_ref()
        .and_then(|payload| payload.text.as_deref())
        .map(snippet_text)
        .filter(|text| !text.is_empty());
    let explain = include_explain.then(|| AgentQueryExplain {
        ranking_source: "raw-lesson".to_string(),
        matched_tokens: hit.matched_tokens.clone(),
        score_breakdown: AgentQueryScoreBreakdown {
            text: hit.text_score,
            binder: hit.binder_score,
            meta: hit.meta_score,
            context: hit.context_score,
            route_memory: hit.route_memory_bias,
            free_energy: hit.free_energy_score,
            final_score: hit.total_score,
        },
        path: hit.scope_reason.clone(),
        why_won: build_why_won(hit),
        risk_flags: build_risk_flags(hit),
    });
    AgentQueryHit {
        title,
        score: hit.total_score,
        snippet,
        source_ref: hit.card_id.to_string(),
        binder_names: hit.binder_names.clone(),
        explain,
    }
}

fn render_markdown(
    query: &str,
    hits: &[AgentQueryHit],
    short_term_hits: &[AgentShortTermHit],
    fallback: &[AgentFallbackSummary],
    diagnostics: &AgentQueryDiagnostics,
    weak_result: bool,
    freshness_admissions: usize,
) -> String {
    let mut out = String::new();
    out.push_str("# AGENT MEMORY\n\n");
    out.push_str(&format!("Query: {query}\n\n"));
    out.push_str("## Lessons\n");
    if hits.is_empty() {
        out.push_str("- No lesson hits.\n");
    } else {
        for (index, hit) in hits.iter().enumerate() {
            out.push_str(&format!(
                "{}. [card] {} (score={:.2})\n",
                index + 1,
                hit.title,
                hit.score
            ));
            if let Some(snippet) = hit.snippet.as_deref() {
                out.push_str(&format!("- snippet: {snippet}\n"));
            }
            out.push_str(&format!("- ref: card:{}\n", hit.source_ref));
            if !hit.binder_names.is_empty() {
                out.push_str(&format!("- binders: {}\n", hit.binder_names.join(", ")));
            }
        }
    }

    if !short_term_hits.is_empty() {
        out.push_str("\n## Short-Term Memory\n");
        for (index, hit) in short_term_hits.iter().enumerate() {
            let timestamp = hit
                .timestamp
                .map(|value| format!(" @ {}", value.format("%Y-%m-%d %H:%M")))
                .unwrap_or_default();
            out.push_str(&format!(
                "{}. [{}] {} (score={:.2}, recency={:.2}, matched={}){}\n",
                index + 1,
                hit.source_kind,
                hit.session_title,
                hit.score,
                hit.recency,
                hit.matched_tokens.len(),
                timestamp
            ));
            out.push_str(&format!("- snippet: {}\n", hit.snippet));
            out.push_str(&format!("- ref: {} (session={})\n", hit.source_ref, hit.session_ref));
        }
    }

    if hits.iter().any(|hit| hit.explain.is_some()) || !short_term_hits.is_empty() {
        out.push_str("\n## Explain\n");
        for (index, hit) in hits.iter().enumerate() {
            let Some(explain) = hit.explain.as_ref() else {
                continue;
            };
            out.push_str(&format!("{}. {}\n", index + 1, hit.title));
            out.push_str(&format!("- source={}\n", explain.ranking_source));
            out.push_str(&format!(
                "- matched_tokens: {}\n",
                if explain.matched_tokens.is_empty() {
                    "(none)".to_string()
                } else {
                    explain.matched_tokens.join(", ")
                }
            ));
            let fe_part = explain
                .score_breakdown
                .free_energy
                .map(|fe| format!(" free_energy={:.2}", fe))
                .unwrap_or_default();
            out.push_str(&format!(
                "- score_breakdown: text={:.2} binder={:.2} meta={:.2} context={:.2} route_memory={:.2}{} final={:.2}\n",
                explain.score_breakdown.text,
                explain.score_breakdown.binder,
                explain.score_breakdown.meta,
                explain.score_breakdown.context,
                explain.score_breakdown.route_memory,
                fe_part,
                explain.score_breakdown.final_score
            ));
            out.push_str(&format!("- path: {}\n", explain.path));
            out.push_str(&format!("- why: {}\n", explain.why_won));
            out.push_str(&format!(
                "- risk: {}\n",
                if explain.risk_flags.is_empty() {
                    "none".to_string()
                } else {
                    explain.risk_flags.join(", ")
                }
            ));
        }
        if !short_term_hits.is_empty() {
            for (index, hit) in short_term_hits.iter().enumerate() {
                out.push_str(&format!("{}. [short-term] {}\n", hits.len() + index + 1, hit.session_title));
                out.push_str(&format!("- source={}\n", hit.source_kind));
                out.push_str(&format!(
                    "- matched_tokens: {}\n",
                    if hit.matched_tokens.is_empty() {
                        "(none)".to_string()
                    } else {
                        hit.matched_tokens.join(", ")
                    }
                ));
                out.push_str(&format!(
                    "- score_breakdown: recency={:.2} final={:.2}\n",
                    hit.recency, hit.score
                ));
                out.push_str(&format!("- path: {}\n", hit.path));
            }
        }
    }

    if weak_result || !fallback.is_empty() {
        out.push_str("\n## Cross-Agent Summaries (fallback)\n");
        if fallback.is_empty() {
            out.push_str("- No fallback summaries available.\n");
        } else {
            for (index, summary) in fallback.iter().enumerate() {
                out.push_str(&format!(
                    "{}. [{}] {} (score={:.2})\n",
                    index + 1,
                    summary.source_kind,
                    summary.title,
                    summary.score
                ));
                out.push_str(&format!("- snippet: {}\n", summary.snippet));
                out.push_str(&format!("- ref: {}\n", summary.source_ref));
            }
        }
    }

    out.push_str("\n# Diagnostics\n");
    out.push_str(&format!(
        "weak_result={} touched=0 freshness_admissions={} lesson_hits={} short_term_hits={} lane={} reroute={} scope_lens={}",
        if weak_result { "true" } else { "false" },
        freshness_admissions,
        hits.len(),
        short_term_hits.len(),
        diagnostics.scoring_lane,
        diagnostics.routing_decision,
        diagnostics.scope_lens
    ));
    if !diagnostics.routing_flags.is_empty() {
        out.push_str(&format!("\nrouting_flags={}", diagnostics.routing_flags.join(", ")));
    }
    out.push('\n');
    out
}

fn build_why_won(hit: &RetrievalHit) -> String {
    let mut reasons = Vec::new();
    if !hit.matched_tokens.is_empty() {
        reasons.push(format!("matched {}", hit.matched_tokens.join(", ")));
    }
    if hit.context_score > 0.0 {
        reasons.push(format!("context +{:.2}", hit.context_score));
    }
    if hit.route_memory_bias.abs() >= 0.0001 {
        reasons.push(format!("route-memory {:+.2}", hit.route_memory_bias));
    }
    if reasons.is_empty() {
        "non-zero retrieval score".to_string()
    } else {
        reasons.join("; ")
    }
}

fn build_risk_flags(hit: &RetrievalHit) -> Vec<String> {
    let mut flags = Vec::new();
    if hit.route_memory_bias < 0.0 {
        flags.push("route-memory-suppressed".to_string());
    }
    if hit.binder_names.is_empty() {
        flags.push("untagged".to_string());
    }
    flags
}

fn select_fallback_summaries(
    corpus: &MaterializedCorpus,
    query: &str,
    top: usize,
    context: Option<&crate::context::QueryContext>,
    include_latent: bool,
) -> Vec<AgentFallbackSummary> {
    let Some(snapshot) = corpus.snapshot.as_ref() else {
        return Vec::new();
    };

    let tokens = crate::retrieval::tokenize(query);
    if tokens.is_empty() {
        return Vec::new();
    }

    let mut hits = select_agent_summary_fallbacks(snapshot, &tokens, top);
    if hits.is_empty()
        && !snapshot
            .objects()
            .values()
            .any(|object| object.object_kind == "agent_summary")
    {
        hits = select_manual_smartlist_docs(snapshot, context, include_latent)
            .into_iter()
            .filter(|doc| {
                snapshot
                    .objects()
                    .get(&doc.source_ref)
                    .and_then(|object| object.semantic_payload.as_ref())
                    .and_then(|payload| payload.provenance.as_ref())
                    .and_then(|provenance| provenance.get("retrieval_visibility"))
                    .and_then(|value| value.as_str())
                    .map(|value| include_latent || value == "default")
                    .unwrap_or(true)
            })
            .filter_map(|doc| {
                let haystack = format!("{}\n{}", doc.title, doc.snippet).to_ascii_lowercase();
                let matched = tokens
                    .iter()
                    .filter(|token| haystack.contains(token.as_str()))
                    .count();
                if matched == 0 {
                    return None;
                }

                Some(AgentFallbackSummary {
                    title: doc.title,
                    snippet: snippet_text(&doc.snippet),
                    source_kind: doc.source_kind,
                    source_ref: doc.source_ref,
                    score: matched as f64 / tokens.len().max(1) as f64,
                })
            })
            .collect::<Vec<_>>();
    }

    hits.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.source_ref.cmp(&right.source_ref))
    });
    hits.truncate(top);
    hits
}

fn select_agent_summary_fallbacks(
    snapshot: &crate::store::AmsStore,
    tokens: &[String],
    top: usize,
) -> Vec<AgentFallbackSummary> {
    let mut hits = snapshot
        .objects()
        .values()
        .filter(|object| object.object_kind == "agent_summary")
        .filter_map(|object| {
            let summary = object
                .semantic_payload
                .as_ref()
                .and_then(|payload| payload.summary.as_deref())
                .unwrap_or_default();
            let matched = tokens
                .iter()
                .filter(|token| summary.to_ascii_lowercase().contains(token.as_str()))
                .count();
            if matched == 0 {
                return None;
            }

            Some(AgentFallbackSummary {
                title: object.object_id.clone(),
                snippet: snippet_text(summary),
                source_kind: "agent_summary".to_string(),
                source_ref: object.object_id.clone(),
                score: matched as f64 + if object.object_id == "agent_summary:shared" { 1.0 } else { 0.0 },
            })
        })
        .collect::<Vec<_>>();

    hits.sort_by(|left, right| {
        right
            .score
            .partial_cmp(&left.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.source_ref.cmp(&right.source_ref))
    });
    hits.truncate(top.min(5));
    hits
}

fn snippet_text(text: &str) -> String {
    const MAX: usize = 160;
    let trimmed = text.trim();
    if trimmed.len() <= MAX {
        return trimmed.to_string();
    }
    let mut out = trimmed[..MAX].trim_end().to_string();
    out.push_str("...");
    out
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use uuid::Uuid;

    use crate::corpus::{BinderRecord, CardPayloadRecord, CardRecord, MaterializedCorpus, TagLinkMeta, TagLinkRecord};

    use super::*;

    fn make_corpus() -> MaterializedCorpus {
        let card_a = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap();
        let binder = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        MaterializedCorpus {
            db_path: PathBuf::from("fixture.memory.jsonl"),
            snapshot_path: None,
            snapshot: None,
            cards: BTreeMap::from([(
                card_a,
                CardRecord {
                    card_id: card_a,
                    state: CardState::Active,
                    state_reason: None,
                },
            )]),
            binders: BTreeMap::from([(
                binder,
                BinderRecord {
                    binder_id: binder,
                    name: "Topic: Search".to_string(),
                },
            )]),
            tag_links: BTreeMap::from([(
                (card_a, binder),
                TagLinkRecord {
                    card_id: card_a,
                    binder_id: binder,
                    meta: TagLinkMeta {
                        relevance: 0.9,
                        reason: None,
                        added_by: None,
                        created_at: None,
                    },
                },
            )]),
            payloads: BTreeMap::from([(
                card_a,
                CardPayloadRecord {
                    card_id: card_a,
                    title: Some("Search cache invalidation fix".to_string()),
                    text: Some("Cache key normalization prevents stale results.".to_string()),
                    source: Some("fixture".to_string()),
                    updated_at: None,
                },
            )]),
            unknown_record_types: BTreeMap::new(),
        }
    }

    #[test]
    fn run_agent_query_renders_markdown_and_diagnostics() {
        let result = run_agent_query(
            &mut make_corpus(),
            &AgentQueryRequest {
                query: "search cache".to_string(),
                top: 5,
                binder_filters: Vec::new(),
                seed_card: None,
                state_filter: None,
                include_retracted: false,
                explain: true,
                context_options: QueryContextOptions::default(),
                route_memory: None,
                route_memory_bias_options: RouteMemoryBiasOptions::default(),
                include_latent: false,
                touch: true,
            },
        )
        .unwrap();

        assert_eq!(result.hits.len(), 1);
        assert!(result.markdown.contains("# AGENT MEMORY"));
        assert!(result.markdown.contains("## Explain"));
        assert!(result.markdown.contains("# Diagnostics"));
        assert_eq!(result.diagnostics.scope_lens, "global");
    }

    fn make_contextful_corpus() -> MaterializedCorpus {
        let mut snapshot = crate::store::AmsStore::new();
        snapshot
            .upsert_object("task-thread:child-thread", "task_thread", None, None, None)
            .unwrap();
        snapshot.objects_mut().get_mut("task-thread:child-thread").unwrap().semantic_payload =
            Some(crate::model::SemanticPayload {
                embedding: None,
                tags: None,
                summary: Some("Child search cache thread".to_string()),
                provenance: Some(BTreeMap::from([
                    ("thread_id".to_string(), serde_json::json!("child-thread")),
                    ("current_step".to_string(), serde_json::json!("Search cache implementation")),
                    ("next_command".to_string(), serde_json::json!("cargo test")),
                ])),
            });
        let mut corpus = make_corpus();
        corpus.snapshot_path = Some(PathBuf::from("fixture.memory.ams.json"));
        corpus.snapshot = Some(snapshot);
        corpus
    }

    fn make_smartlist_only_corpus() -> MaterializedCorpus {
        let mut snapshot = crate::store::AmsStore::new();
        snapshot
            .upsert_object(
                "smartlist-note:1",
                "smartlist_note",
                None,
                Some(crate::model::SemanticPayload {
                    embedding: None,
                    tags: None,
                    summary: Some("Search cache note".to_string()),
                    provenance: Some(BTreeMap::from([
                        ("title".to_string(), serde_json::json!("Search cache note")),
                        (
                            "text".to_string(),
                            serde_json::json!("Use manual invalidation for the search cache lane."),
                        ),
                        ("updated_at".to_string(), serde_json::json!("2026-03-13T20:00:00Z")),
                    ])),
                }),
                None,
            )
            .unwrap();
        snapshot
            .create_container(
                "smartlist-members:smartlist/architecture/incubation/rust-replatform",
                "container",
                "smartlist_members",
            )
            .unwrap();
        snapshot
            .add_object(
                "smartlist-members:smartlist/architecture/incubation/rust-replatform",
                "smartlist-note:1",
                None,
                None,
            )
            .unwrap();

        MaterializedCorpus {
            db_path: PathBuf::from("fixture.memory.jsonl"),
            snapshot_path: Some(PathBuf::from("fixture.memory.ams.json")),
            snapshot: Some(snapshot),
            cards: BTreeMap::new(),
            binders: BTreeMap::new(),
            tag_links: BTreeMap::new(),
            payloads: BTreeMap::new(),
            unknown_record_types: BTreeMap::new(),
        }
    }

    #[test]
    fn run_agent_query_renders_short_term_memory_from_snapshot() {
        let mut snapshot = crate::store::AmsStore::new();
        snapshot.create_container("chat-session:s1", "chat_session", "chat_session").unwrap();
        snapshot
            .upsert_object(
                "chat-msg:s1-0",
                "chat_message",
                None,
                Some(crate::model::SemanticPayload {
                    embedding: None,
                    tags: None,
                    summary: Some("Use double buffering for swarm agents".to_string()),
                    provenance: Some(BTreeMap::from([
                        ("text".to_string(), serde_json::json!("Use double buffering for swarm agents with shared memory.")),
                        ("ts".to_string(), serde_json::json!("2026-03-13T19:00:00Z")),
                    ])),
                }),
                None,
            )
            .unwrap();
        snapshot.containers_mut().get_mut("chat-session:s1").unwrap().metadata = Some(BTreeMap::from([
            ("title".to_string(), serde_json::json!("Session s1")),
            ("started_at".to_string(), serde_json::json!("2026-03-13T18:00:00Z")),
        ]));
        snapshot.add_object("chat-session:s1", "chat-msg:s1-0", None, None).unwrap();

        let mut corpus = MaterializedCorpus {
            db_path: PathBuf::from("fixture.memory.jsonl"),
            snapshot_path: Some(PathBuf::from("fixture.memory.ams.json")),
            snapshot: Some(snapshot),
            cards: BTreeMap::new(),
            binders: BTreeMap::new(),
            tag_links: BTreeMap::new(),
            payloads: BTreeMap::new(),
            unknown_record_types: BTreeMap::new(),
        };

        let result = run_agent_query(
            &mut corpus,
            &AgentQueryRequest {
                query: "swarm double buffering shared memory".to_string(),
                top: 5,
                binder_filters: Vec::new(),
                seed_card: None,
                state_filter: None,
                include_retracted: false,
                explain: true,
                context_options: QueryContextOptions::default(),
                route_memory: None,
                route_memory_bias_options: RouteMemoryBiasOptions::default(),
                include_latent: false,
                touch: true,
            },
        )
        .unwrap();

        assert_eq!(result.hits.len(), 0);
        assert_eq!(result.short_term_hits, 1);
        assert!(result.markdown.contains("## Short-Term Memory"));
        assert!(result.markdown.contains("chat-msg:s1-0"));
        assert!(result.markdown.contains("[short-term]"));
    }

    #[test]
    fn run_agent_query_builds_route_episode_when_context_and_hits_exist() {
        let result = run_agent_query(
            &mut make_contextful_corpus(),
            &AgentQueryRequest {
                query: "search cache".to_string(),
                top: 5,
                binder_filters: Vec::new(),
                seed_card: None,
                state_filter: None,
                include_retracted: false,
                explain: true,
                context_options: QueryContextOptions {
                    current_node_id: Some("child-thread".to_string()),
                    no_active_thread_context: true,
                    ..QueryContextOptions::default()
                },
                route_memory: None,
                route_memory_bias_options: RouteMemoryBiasOptions::default(),
                include_latent: false,
                touch: true,
            },
        )
        .unwrap();

        let episode = result.route_episode.expect("route episode");
        assert_eq!(episode.frame.lineage_node_ids, vec!["child-thread".to_string()]);
        assert_eq!(episode.episode.query_text, "search cache");
        assert_eq!(episode.candidate_target_refs.len(), 1);
    }

    #[test]
    fn run_agent_query_surfaces_smartlist_fallback_when_no_lesson_hits_exist() {
        let result = run_agent_query(
            &mut make_smartlist_only_corpus(),
            &AgentQueryRequest {
                query: "search cache invalidation".to_string(),
                top: 5,
                binder_filters: Vec::new(),
                seed_card: None,
                state_filter: None,
                include_retracted: false,
                explain: false,
                context_options: QueryContextOptions::default(),
                route_memory: None,
                route_memory_bias_options: RouteMemoryBiasOptions::default(),
                include_latent: false,
                touch: true,
            },
        )
        .unwrap();

        assert!(result.hits.is_empty());
        assert_eq!(result.short_term_hits, 1);
        assert_eq!(result.fallback.len(), 1);
        assert!(result.markdown.contains("## Cross-Agent Summaries (fallback)"));
        assert!(result.markdown.contains("Search cache note"));
    }

    #[test]
    fn run_agent_query_recall_surfaces_suppressed_latent_smartlist_memory() {
        let mut snapshot = crate::store::AmsStore::new();
        snapshot
            .upsert_object(
                "smartlist-note:latent",
                "smartlist_note",
                None,
                Some(crate::model::SemanticPayload {
                    embedding: None,
                    tags: None,
                    summary: Some("3-agent basic shared swarm smoke handoff".to_string()),
                    provenance: Some(BTreeMap::from([
                        ("title".to_string(), serde_json::json!("3-agent basic shared swarm smoke handoff")),
                        (
                            "text".to_string(),
                            serde_json::json!("orchestrator root plus two worker tangent threads"),
                        ),
                        ("retrieval_visibility".to_string(), serde_json::json!("suppressed")),
                        ("updated_at".to_string(), serde_json::json!("2026-03-15T18:55:00Z")),
                    ])),
                }),
                None,
            )
            .unwrap();
        snapshot
            .create_container(
                "smartlist-members:smartlist/architecture/incubation/rust-replatform/latent",
                "container",
                "smartlist_members",
            )
            .unwrap();
        snapshot
            .add_object(
                "smartlist-members:smartlist/architecture/incubation/rust-replatform/latent",
                "smartlist-note:latent",
                None,
                None,
            )
            .unwrap();

        let mut corpus = MaterializedCorpus {
            db_path: PathBuf::from("fixture.memory.jsonl"),
            snapshot_path: Some(PathBuf::from("fixture.memory.ams.json")),
            snapshot: Some(snapshot),
            cards: BTreeMap::new(),
            binders: BTreeMap::new(),
            tag_links: BTreeMap::new(),
            payloads: BTreeMap::new(),
            unknown_record_types: BTreeMap::new(),
        };

        let default_result = run_agent_query(
            &mut corpus,
            &AgentQueryRequest {
                query: "3-agent shared swarm handoff".to_string(),
                top: 5,
                binder_filters: Vec::new(),
                seed_card: None,
                state_filter: None,
                include_retracted: false,
                explain: false,
                context_options: QueryContextOptions::default(),
                route_memory: None,
                route_memory_bias_options: RouteMemoryBiasOptions::default(),
                include_latent: false,
                touch: true,
            },
        )
        .unwrap();
        assert_eq!(default_result.short_term_hits, 0);

        let recall_result = run_agent_query(
            &mut corpus,
            &AgentQueryRequest {
                query: "3-agent shared swarm handoff".to_string(),
                top: 5,
                binder_filters: Vec::new(),
                seed_card: None,
                state_filter: None,
                include_retracted: false,
                explain: false,
                context_options: QueryContextOptions::default(),
                route_memory: None,
                route_memory_bias_options: RouteMemoryBiasOptions::default(),
                include_latent: true,
                touch: true,
            },
        )
        .unwrap();
        assert_eq!(recall_result.short_term_hits, 1);
        assert!(recall_result.markdown.contains("3-agent basic shared swarm smoke handoff"));
    }
}
