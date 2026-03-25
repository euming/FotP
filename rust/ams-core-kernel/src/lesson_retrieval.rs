use std::collections::{BTreeMap, HashMap, HashSet};

use serde_json::Value;

use crate::agent_query::{AgentQueryDiagnostics, AgentQueryExplain, AgentQueryHit, AgentQueryScoreBreakdown};
use crate::context::QueryContext;
use crate::freshness::{freshness_lane_boost, FreshnessObjectPosition};
use crate::model::{JsonMap, ObjectRecord};
use crate::retrieval::tokenize;
use crate::store::AmsStore;

#[derive(Clone, Debug)]
pub struct RankedLessonSurface {
    pub hits: Vec<AgentQueryHit>,
    pub diagnostics: AgentQueryDiagnostics,
}

#[derive(Clone, Debug)]
struct LessonEvidenceSnapshot {
    snippet: String,
    source_ref: String,
}

#[derive(Clone, Debug)]
struct LessonScore {
    total: f64,
    semantic: f64,
    evidence: f64,
    confidence: f64,
    decay: f64,
    tier: String,
    snapshots: Vec<LessonEvidenceSnapshot>,
    semantic_contribution: f64,
    freshness_contribution: f64,
    evidence_contribution: f64,
}

#[derive(Clone, Debug)]
struct RankedLesson {
    hit: AgentQueryHit,
}

pub fn rank_snapshot_lessons(
    snapshot: &AmsStore,
    query: &str,
    top: usize,
    context: Option<&QueryContext>,
    freshness_positions: &HashMap<String, FreshnessObjectPosition>,
    excluded_object_ids: &HashSet<String>,
) -> Option<RankedLessonSurface> {
    let has_lessons = snapshot
        .objects()
        .values()
        .any(|object| matches!(object.object_kind.as_str(), "lesson" | "lesson_semantic_node"));
    if !has_lessons {
        return None;
    }

    let tokens = tokenize(query);
    let scope_lens = context
        .map(|active| active.scope_lens().to_string())
        .unwrap_or_else(|| "global".to_string());
    if tokens.is_empty() {
        return Some(RankedLessonSurface {
            hits: Vec::new(),
            diagnostics: AgentQueryDiagnostics {
                scoring_lane: "none".to_string(),
                routing_decision: "none".to_string(),
                scope_lens,
                routing_flags: Vec::new(),
            },
        });
    }

    let raw_hits = rank_lessons_raw(snapshot, &tokens, top, freshness_positions, excluded_object_ids);
    let semantic_hits =
        rank_lessons_from_semantic_nodes(snapshot, &tokens, top, freshness_positions, excluded_object_ids);

    if semantic_hits.is_empty() {
        return Some(RankedLessonSurface {
            hits: raw_hits.into_iter().map(|item| item.hit).collect(),
            diagnostics: AgentQueryDiagnostics {
                scoring_lane: "raw-lesson".to_string(),
                routing_decision: "semantic-empty".to_string(),
                scope_lens,
                routing_flags: Vec::new(),
            },
        });
    }

    let best_semantic = &semantic_hits[0];
    let best_raw = raw_hits.first();
    let weak_semantic = is_weak_semantic_hit(best_semantic, tokens.len());
    let raw_more_specific = best_raw
        .as_ref()
        .map(|raw| matched_token_count(&raw.hit) > matched_token_count(&best_semantic.hit))
        .unwrap_or(false);
    let raw_materially_better = best_raw
        .as_ref()
        .map(|raw| raw.hit.score >= (best_semantic.hit.score + 0.05))
        .unwrap_or(false);

    if weak_semantic && best_raw.is_some() && (raw_more_specific || raw_materially_better) {
        let mut flags = best_semantic
            .hit
            .explain
            .as_ref()
            .map(|explain| explain.risk_flags.clone())
            .unwrap_or_default();
        if raw_more_specific {
            flags.push("raw-more-specific".to_string());
        }
        if raw_materially_better {
            flags.push("raw-score-stronger".to_string());
        }
        flags.sort();
        flags.dedup();

        return Some(RankedLessonSurface {
            hits: raw_hits.into_iter().map(|item| item.hit).collect(),
            diagnostics: AgentQueryDiagnostics {
                scoring_lane: "raw-lesson".to_string(),
                routing_decision: "rerouted-from-semantic-node-first".to_string(),
                scope_lens,
                routing_flags: flags,
            },
        });
    }

    Some(RankedLessonSurface {
        hits: semantic_hits.into_iter().map(|item| item.hit).collect(),
        diagnostics: AgentQueryDiagnostics {
            scoring_lane: "semantic-node-first".to_string(),
            routing_decision: "semantic-node-first".to_string(),
            scope_lens,
            routing_flags: Vec::new(),
        },
    })
}

fn rank_lessons_raw(
    snapshot: &AmsStore,
    tokens: &[String],
    top: usize,
    freshness_positions: &HashMap<String, FreshnessObjectPosition>,
    excluded_object_ids: &HashSet<String>,
) -> Vec<RankedLesson> {
    let mut hits = snapshot
        .objects()
        .values()
        .filter(|object| object.object_kind == "lesson")
        .filter(|object| !excluded_object_ids.contains(&object.object_id))
        .filter_map(|lesson| {
            let score = score_lesson(lesson, tokens, freshness_positions.get(&lesson.object_id));
            let matched_tokens = match_tokens(
                &format!(
                    "{}\n{}",
                    lesson_summary(lesson),
                    score
                        .snapshots
                        .iter()
                        .map(|snapshot| format!("{}\n{}", snapshot.snippet, snapshot.source_ref))
                        .collect::<Vec<_>>()
                        .join("\n")
                ),
                tokens,
            );
            if matched_tokens.is_empty() {
                return None;
            }

            let path = freshness_positions
                .get(&lesson.object_id)
                .map(|position| format!("raw-lesson -> {} -> freshness:{}", lesson.object_id, position.temperature_label))
                .unwrap_or_else(|| format!("raw-lesson -> {}", lesson.object_id));
            let risk_flags = raw_risk_flags(tokens.len(), &matched_tokens, &score);
            Some(RankedLesson {
                hit: AgentQueryHit {
                    title: lesson_summary(lesson),
                    score: score.total,
                    snippet: best_snippet(lesson, &score.snapshots),
                    source_ref: lesson.object_id.clone(),
                    binder_names: Vec::new(),
                    explain: Some(AgentQueryExplain {
                        ranking_source: "raw-lesson".to_string(),
                        matched_tokens,
                        score_breakdown: AgentQueryScoreBreakdown {
                            text: score.semantic_contribution,
                            binder: score.freshness_contribution,
                            meta: score.evidence_contribution,
                            context: 0.0,
                            route_memory: 0.0,
                            free_energy: None,
                            final_score: score.total,
                        },
                        path,
                        why_won: format!(
                            "lesson matched {} token(s); freshness_tier={} confidence={:.2} evidence={:.2}",
                            count_tokens(lesson, &score.snapshots, tokens),
                            score.tier,
                            score.confidence,
                            score.evidence
                        ),
                        risk_flags,
                    }),
                },
            })
        })
        .collect::<Vec<_>>();

    hits.sort_by(|left, right| {
        let left_position = freshness_positions.get(&left.hit.source_ref);
        let right_position = freshness_positions.get(&right.hit.source_ref);
        freshness_ordering(left_position, right_position)
            .then_with(|| {
        right
            .hit
            .score
            .partial_cmp(&left.hit.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.hit.source_ref.cmp(&right.hit.source_ref))
            })
    });
    hits.truncate(top);
    hits
}

fn rank_lessons_from_semantic_nodes(
    snapshot: &AmsStore,
    tokens: &[String],
    top: usize,
    freshness_positions: &HashMap<String, FreshnessObjectPosition>,
    excluded_object_ids: &HashSet<String>,
) -> Vec<RankedLesson> {
    let mut hits = Vec::new();
    let mut nodes = snapshot
        .objects()
        .values()
        .filter(|object| object.object_kind == "lesson_semantic_node")
        .collect::<Vec<_>>();
    nodes.sort_by(|left, right| left.object_id.cmp(&right.object_id));

    for node in nodes {
        let node_prov = node.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref());
        let Some(members_container_id) = read_string(node_prov, "members_container_id") else {
            continue;
        };
        if !snapshot.containers().contains_key(&members_container_id) {
            continue;
        }

        let mut members = snapshot
            .iterate_forward(&members_container_id)
            .into_iter()
            .filter_map(|link| snapshot.objects().get(&link.object_id))
            .filter(|object| object.object_kind == "lesson")
            .filter(|object| !excluded_object_ids.contains(&object.object_id))
            .collect::<Vec<_>>();
        members.sort_by(|left, right| left.object_id.cmp(&right.object_id));
        if members.is_empty() {
            continue;
        }

        let mut ranked_members = members
            .iter()
            .map(|lesson| (*lesson, score_lesson(lesson, tokens, freshness_positions.get(&lesson.object_id))))
            .collect::<Vec<_>>();
        ranked_members.sort_by(|left, right| {
            freshness_ordering(
                freshness_positions.get(&left.0.object_id),
                freshness_positions.get(&right.0.object_id),
            )
            .then_with(|| {
            right
                .1
                .total
                .partial_cmp(&left.1.total)
                .unwrap_or(std::cmp::Ordering::Equal)
                .then_with(|| left.0.object_id.cmp(&right.0.object_id))
            })
        });
        let (best_lesson, best_score) = ranked_members[0].clone();

        let node_haystack = format!(
            "{}\n{}",
            lesson_summary(node),
            members
                .iter()
                .map(|lesson| lesson_summary(lesson))
                .collect::<Vec<_>>()
                .join("\n")
        );
        let node_matched_tokens = match_tokens(&node_haystack, tokens);
        let semantic = (node_matched_tokens.len() as f64 / tokens.len().max(1) as f64).max(best_score.semantic);
        let member_count = members.len();
        let representative_mismatch = titles_materially_differ(node.semantic_payload.as_ref().and_then(|payload| payload.summary.as_deref()), best_lesson.semantic_payload.as_ref().and_then(|payload| payload.summary.as_deref()));
        let semantic_contribution = 0.80 * semantic;
        let freshness_contribution = 0.10 * best_score.freshness_contribution;
        let evidence_contribution = 0.10 * best_score.evidence;
        let broad_penalty = if member_count >= 4 {
            0.12
        } else if member_count == 3 {
            0.06
        } else {
            0.0
        };
        let low_specificity_penalty = if node_matched_tokens.len() <= std::cmp::max(1, tokens.len() / 4) {
            0.06
        } else {
            0.0
        };
        let representative_penalty = if representative_mismatch { 0.08 } else { 0.0 };
        let pre_decay = (semantic_contribution
            + freshness_contribution
            + evidence_contribution
            - broad_penalty
            - low_specificity_penalty
            - representative_penalty)
            .max(0.0);
        let evidence_text = best_score
            .snapshots
            .iter()
            .map(|snapshot| format!("{}\n{}", snapshot.snippet, snapshot.source_ref))
            .collect::<Vec<_>>()
            .join("\n");
        let matched_tokens = match_tokens(
            &format!("{}\n{}\n{}", lesson_summary(node), lesson_summary(best_lesson), evidence_text),
            tokens,
        );
        if matched_tokens.is_empty() {
            continue;
        }

        let score = pre_decay / best_score.decay.max(1.0);
        if score <= 0.0 {
            continue;
        }

        let mut risk_flags = Vec::new();
        if member_count >= 3 {
            risk_flags.push("broad-node".to_string());
        }
        if matched_tokens.len() <= std::cmp::max(1, tokens.len() / 4) {
            risk_flags.push("low-specificity".to_string());
        }
        if representative_mismatch {
            risk_flags.push("representative-mismatch".to_string());
        }
        if semantic < 0.35 && (freshness_contribution + evidence_contribution) > semantic_contribution {
            risk_flags.push("freshness-evidence-boosted".to_string());
        }

        hits.push(RankedLesson {
            hit: AgentQueryHit {
                title: lesson_summary(node),
                score,
                snippet: best_snippet(best_lesson, &best_score.snapshots),
                source_ref: best_lesson.object_id.clone(),
                binder_names: Vec::new(),
                explain: Some(AgentQueryExplain {
                    ranking_source: "semantic-node-first".to_string(),
                    matched_tokens,
                    score_breakdown: AgentQueryScoreBreakdown {
                        text: semantic_contribution,
                        binder: freshness_contribution,
                        meta: evidence_contribution,
                        context: 0.0,
                        route_memory: 0.0,
                        free_energy: None,
                        final_score: score,
                    },
                    path: freshness_positions
                        .get(&best_lesson.object_id)
                        .map(|position| {
                            format!(
                                "{} -> {} -> freshness:{}",
                                node.object_id, best_lesson.object_id, position.temperature_label
                            )
                        })
                        .unwrap_or_else(|| format!("{} -> {}", node.object_id, best_lesson.object_id)),
                    why_won: format!(
                        "semantic node matched {} token(s); representative={} member_count={} confidence={:.2}",
                        count_tokens(best_lesson, &best_score.snapshots, tokens).max(node_matched_tokens.len()),
                        if representative_mismatch { "mismatch" } else { "aligned" },
                        member_count,
                        best_score.confidence
                    ),
                    risk_flags,
                }),
            },
        });
    }

    hits.sort_by(|left, right| {
        let left_position = freshness_positions.get(&left.hit.source_ref);
        let right_position = freshness_positions.get(&right.hit.source_ref);
        freshness_ordering(left_position, right_position)
            .then_with(|| {
        right
            .hit
            .score
            .partial_cmp(&left.hit.score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| left.hit.source_ref.cmp(&right.hit.source_ref))
            })
    });
    hits.truncate(top);
    hits
}

fn score_lesson(
    lesson: &ObjectRecord,
    tokens: &[String],
    freshness_position: Option<&FreshnessObjectPosition>,
) -> LessonScore {
    let snapshots = read_snapshots(lesson.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref()));
    let haystack = format!(
        "{}\n{}",
        lesson_summary(lesson),
        snapshots
            .iter()
            .map(|snapshot| format!("{}\n{}", snapshot.snippet, snapshot.source_ref))
            .collect::<Vec<_>>()
            .join("\n")
    )
    .to_ascii_lowercase();
    let semantic = tokens
        .iter()
        .filter(|token| haystack.contains(token.as_str()))
        .count() as f64
        / tokens.len().max(1) as f64;
    let tier = read_string(lesson.semantic_payload.as_ref().and_then(|payload| payload.provenance.as_ref()), "freshness_tier")
        .unwrap_or_else(|| "yearly".to_string());
    let freshness = (tier_weight(&tier) + freshness_lane_boost(freshness_position)).min(1.25);
    let confidence = read_f64(
        lesson
            .semantic_payload
            .as_ref()
            .and_then(|payload| payload.provenance.as_ref()),
        "confidence",
        0.0,
    );
    let evidence = read_f64(
        lesson
            .semantic_payload
            .as_ref()
            .and_then(|payload| payload.provenance.as_ref()),
        "evidence_health",
        0.0,
    );
    let decay = read_f64(
        lesson
            .semantic_payload
            .as_ref()
            .and_then(|payload| payload.provenance.as_ref()),
        "decay_multiplier",
        1.0,
    )
    .max(1.0);
    let semantic_contribution = 0.65 * semantic;
    let freshness_contribution = 0.20 * freshness;
    let evidence_contribution = 0.15 * evidence;
    let total = (semantic_contribution + freshness_contribution + evidence_contribution) / decay;
    LessonScore {
        total,
        semantic,
        evidence,
        confidence,
        decay,
        tier,
        snapshots,
        semantic_contribution,
        freshness_contribution,
        evidence_contribution,
    }
}

fn freshness_ordering(
    left: Option<&FreshnessObjectPosition>,
    right: Option<&FreshnessObjectPosition>,
) -> std::cmp::Ordering {
    match (left, right) {
        (Some(left), Some(right)) => left
            .lane_index
            .cmp(&right.lane_index)
            .then_with(|| left.lane_path.cmp(&right.lane_path)),
        (Some(_), None) => std::cmp::Ordering::Less,
        (None, Some(_)) => std::cmp::Ordering::Greater,
        (None, None) => std::cmp::Ordering::Equal,
    }
}

fn lesson_summary(object: &ObjectRecord) -> String {
    object
        .semantic_payload
        .as_ref()
        .and_then(|payload| payload.summary.clone())
        .unwrap_or_else(|| object.object_id.clone())
}

fn read_snapshots(provenance: Option<&JsonMap>) -> Vec<LessonEvidenceSnapshot> {
    let Some(Value::Array(items)) = provenance.and_then(|map| map.get("evidence_snapshots")) else {
        return Vec::new();
    };
    items
        .iter()
        .filter_map(|item| {
            let Value::Object(map) = item else {
                return None;
            };
            let snippet = map
                .get("snippet")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_string();
            let source_ref = map
                .get("source_ref")
                .and_then(Value::as_str)
                .unwrap_or_default()
                .trim()
                .to_string();
            if snippet.is_empty() && source_ref.is_empty() {
                return None;
            }
            Some(LessonEvidenceSnapshot { snippet, source_ref })
        })
        .collect()
}

fn read_string(provenance: Option<&JsonMap>, key: &str) -> Option<String> {
    provenance.and_then(|map| match map.get(key) {
        Some(Value::String(value)) => Some(value.clone()),
        Some(value) => Some(value.to_string()),
        None => None,
    })
}

fn read_f64(provenance: Option<&JsonMap>, key: &str, default: f64) -> f64 {
    provenance
        .and_then(|map| map.get(key))
        .and_then(|value| match value {
            Value::Number(number) => number.as_f64(),
            Value::String(raw) => raw.parse::<f64>().ok(),
            _ => None,
        })
        .unwrap_or(default)
}

fn tier_weight(tier: &str) -> f64 {
    match tier {
        "fresh" => 1.0,
        "recent" => 0.75,
        "monthly" => 0.45,
        "quarterly" => 0.30,
        "yearly" => 0.15,
        _ => 0.15,
    }
}

fn match_tokens(haystack: &str, tokens: &[String]) -> Vec<String> {
    let haystack = haystack.to_ascii_lowercase();
    tokens
        .iter()
        .filter(|token| haystack.contains(token.as_str()))
        .cloned()
        .collect()
}

fn count_tokens(lesson: &ObjectRecord, snapshots: &[LessonEvidenceSnapshot], tokens: &[String]) -> usize {
    match_tokens(
        &format!(
            "{}\n{}",
            lesson_summary(lesson),
            snapshots
                .iter()
                .map(|snapshot| format!("{}\n{}", snapshot.snippet, snapshot.source_ref))
                .collect::<Vec<_>>()
                .join("\n")
        ),
        tokens,
    )
    .len()
}

fn best_snippet(lesson: &ObjectRecord, snapshots: &[LessonEvidenceSnapshot]) -> Option<String> {
    snapshots
        .iter()
        .find(|snapshot| !snapshot.snippet.is_empty())
        .map(|snapshot| truncate_text(&snapshot.snippet, 160))
        .or_else(|| {
            lesson
                .semantic_payload
                .as_ref()
                .and_then(|payload| payload.summary.as_deref())
                .map(|summary| truncate_text(summary, 160))
        })
}

fn truncate_text(text: &str, max_chars: usize) -> String {
    let trimmed = text.trim();
    if trimmed.chars().count() <= max_chars {
        return trimmed.to_string();
    }
    let mut out = trimmed.chars().take(max_chars).collect::<String>();
    out = out.trim_end().to_string();
    out.push_str("...");
    out
}

fn raw_risk_flags(token_count: usize, matched_tokens: &[String], score: &LessonScore) -> Vec<String> {
    let mut flags = Vec::new();
    if matched_tokens.len() <= std::cmp::max(1, token_count / 4) {
        flags.push("low-specificity".to_string());
    }
    if score.semantic < 0.35 && (score.freshness_contribution + score.evidence_contribution) > score.semantic_contribution
    {
        flags.push("freshness-evidence-boosted".to_string());
    }
    flags
}

fn is_weak_semantic_hit(hit: &RankedLesson, query_token_count: usize) -> bool {
    let Some(explain) = hit.hit.explain.as_ref() else {
        return false;
    };
    if explain.ranking_source != "semantic-node-first" {
        return false;
    }
    if explain
        .risk_flags
        .iter()
        .any(|flag| matches!(flag.as_str(), "representative-mismatch" | "broad-node"))
    {
        return true;
    }
    explain.matched_tokens.len() <= std::cmp::max(1, query_token_count / 4)
}

fn matched_token_count(hit: &AgentQueryHit) -> usize {
    hit.explain
        .as_ref()
        .map(|explain| explain.matched_tokens.len())
        .unwrap_or(0)
}

fn titles_materially_differ(node_title: Option<&str>, lesson_title: Option<&str>) -> bool {
    let node_tokens = normalize_title_tokens(node_title);
    let lesson_tokens = normalize_title_tokens(lesson_title);
    if node_tokens.is_empty() || lesson_tokens.is_empty() {
        return false;
    }
    if node_tokens == lesson_tokens {
        return false;
    }
    let overlap = node_tokens
        .keys()
        .filter(|token| lesson_tokens.contains_key(*token))
        .count();
    overlap < std::cmp::min(node_tokens.len(), lesson_tokens.len())
}

fn normalize_title_tokens(value: Option<&str>) -> BTreeMap<String, ()> {
    tokenize(value.unwrap_or_default())
        .into_iter()
        .map(|token| (token, ()))
        .collect()
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    use serde_json::json;

    use crate::model::SemanticPayload;
    use crate::store::AmsStore;

    use super::*;

    fn make_snapshot() -> AmsStore {
        let mut store = AmsStore::new();
        store
            .upsert_object(
                "lesson:decision:a",
                "lesson",
                None,
                Some(SemanticPayload {
                    embedding: None,
                    tags: None,
                    summary: Some("Decision: generate search optimized titles developer sessions stored long".to_string()),
                    provenance: Some(BTreeMap::from([
                        ("freshness_tier".to_string(), json!("fresh")),
                        ("confidence".to_string(), json!(0.9)),
                        ("evidence_health".to_string(), json!(0.8)),
                        ("decay_multiplier".to_string(), json!(1.0)),
                        (
                            "evidence_snapshots".to_string(),
                            json!([
                                {
                                    "snippet": "Generate search optimized titles for developer sessions",
                                    "source_ref": "chat-msg:1"
                                }
                            ]),
                        ),
                    ])),
                }),
                None,
            )
            .unwrap();
        store
            .create_container(
                "lesson-semantic-members:search-title",
                "container",
                "lesson_semantic_members",
            )
            .unwrap();
        store
            .add_object(
                "lesson-semantic-members:search-title",
                "lesson:decision:a",
                None,
                None,
            )
            .unwrap();
        store
            .upsert_object(
                "lesson-semantic:search-title",
                "lesson_semantic_node",
                None,
                Some(SemanticPayload {
                    embedding: None,
                    tags: None,
                    summary: Some("Decision: generate search optimized titles developer sessions stored long".to_string()),
                    provenance: Some(BTreeMap::from([(
                        "members_container_id".to_string(),
                        json!("lesson-semantic-members:search-title"),
                    )])),
                }),
                None,
            )
            .unwrap();
        store
    }

    #[test]
    fn rank_snapshot_lessons_prefers_semantic_node_hits() {
        let result = rank_snapshot_lessons(
            &make_snapshot(),
            "search optimized titles",
            5,
            None,
            &HashMap::new(),
            &HashSet::new(),
        )
        .expect("lesson surface");

        assert_eq!(result.diagnostics.scoring_lane, "semantic-node-first");
        assert_eq!(result.hits.len(), 1);
        assert_eq!(
            result.hits[0].title,
            "Decision: generate search optimized titles developer sessions stored long"
        );
    }
}
