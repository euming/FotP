use std::fs;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};

use crate::agent_query::{run_agent_query, AgentQueryRequest};
use crate::context::QueryContextOptions;
use crate::corpus::{import_materialized_corpus, CardState, MaterializedCorpus};
use crate::route_memory::{load_route_replay_records, RouteMemoryBiasOptions, RouteMemoryStore};

#[derive(Clone, Debug, PartialEq, Eq, Deserialize)]
pub struct ParityCase {
    pub name: String,
    #[serde(default)]
    pub input: Option<String>,
    pub query: String,
    #[serde(default = "default_top")]
    pub top: usize,
    #[serde(default)]
    pub explain: bool,
    #[serde(default)]
    pub binder: Option<String>,
    #[serde(default)]
    pub seed_card: Option<String>,
    #[serde(default)]
    pub state: Option<String>,
    #[serde(default)]
    pub include_retracted: bool,
    #[serde(default)]
    pub current_node: Option<String>,
    #[serde(default)]
    pub parent_node: Option<String>,
    #[serde(default)]
    pub grandparent_node: Option<String>,
    #[serde(default)]
    pub role: Option<String>,
    #[serde(default)]
    pub mode: Option<String>,
    #[serde(default)]
    pub failure_bucket: Option<String>,
    #[serde(default)]
    pub artifact: Option<String>,
    #[serde(default = "default_traversal_budget")]
    pub traversal_budget: usize,
    #[serde(default)]
    pub no_active_thread_context: bool,
    #[serde(default)]
    pub route_replay: Option<String>,
    #[serde(default)]
    pub expected_top_ref: Option<String>,
    #[serde(default)]
    pub expected_hit_count: Option<usize>,
    #[serde(default)]
    pub expected_short_term_count: Option<usize>,
    #[serde(default)]
    pub expected_fallback_count: Option<usize>,
    #[serde(default)]
    pub expected_scope_lens: Option<String>,
    #[serde(default)]
    pub expected_contains: Vec<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ParityReport {
    pub case_name: String,
    pub passed: bool,
    pub actual_top_ref: Option<String>,
    pub actual_hit_count: usize,
    pub actual_short_term_count: usize,
    pub actual_fallback_count: usize,
    pub actual_scope_lens: String,
    pub failures: Vec<String>,
}

pub fn load_parity_cases(path: &Path) -> Result<Vec<ParityCase>> {
    let file = fs::File::open(path).with_context(|| format!("failed to open parity case file '{}'", path.display()))?;
    let reader = BufReader::new(file);
    let mut cases = Vec::new();
    for (index, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("failed to read line {} from '{}'", index + 1, path.display()))?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("//") {
            continue;
        }
        let case: ParityCase = serde_json::from_str(trimmed).with_context(|| {
            format!(
                "failed to parse parity case on line {} in '{}'",
                index + 1,
                path.display()
            )
        })?;
        cases.push(case);
    }
    Ok(cases)
}

pub fn run_parity_validation(
    corpus: &MaterializedCorpus,
    cases: &[ParityCase],
    cases_root: &Path,
) -> Result<Vec<ParityReport>> {
    let mut reports = Vec::with_capacity(cases.len());
    for case in cases {
        let case_corpus = match case.input.as_deref() {
            Some(relative_path) => {
                let input_path = resolve_relative_path(cases_root, relative_path);
                Some(import_materialized_corpus(&input_path)?)
            }
            None => None,
        };
        let corpus = case_corpus.as_ref().unwrap_or(corpus);
        let route_memory = match case.route_replay.as_deref() {
            Some(relative_path) => {
                let replay_path = resolve_relative_path(cases_root, relative_path);
                let replay_records = load_route_replay_records(&replay_path)?;
                Some(RouteMemoryStore::from_replay_records(&replay_records))
            }
            None => None,
        };
        let mut query_corpus = corpus.clone();
        let result = run_agent_query(
            &mut query_corpus,
            &AgentQueryRequest {
                query: case.query.clone(),
                top: case.top.max(1),
                binder_filters: parse_multi_value(case.binder.as_deref()),
                seed_card: case.seed_card.clone(),
                state_filter: case.state.as_deref().map(CardState::parse).transpose()?,
                include_retracted: case.include_retracted,
                explain: case.explain,
                context_options: QueryContextOptions {
                    current_node_id: case.current_node.clone(),
                    parent_node_id: case.parent_node.clone(),
                    grandparent_node_id: case.grandparent_node.clone(),
                    agent_role: case.role.clone(),
                    mode: case.mode.clone(),
                    failure_bucket: case.failure_bucket.clone(),
                    active_artifacts: parse_multi_value(case.artifact.as_deref()),
                    traversal_budget: case.traversal_budget.max(1),
                    no_active_thread_context: case.no_active_thread_context,
                },
                route_memory,
                route_memory_bias_options: RouteMemoryBiasOptions::default(),
                include_latent: false,
                touch: true,
            },
        )?;

        let mut failures = Vec::new();
        let actual_top_ref = result
            .hits
            .first()
            .map(|hit| hit.source_ref.clone())
            .or_else(|| result.short_term.first().map(|hit| hit.source_ref.clone()))
            .or_else(|| result.fallback.first().map(|summary| summary.source_ref.clone()));
        if let Some(expected_top_ref) = case.expected_top_ref.as_deref() {
            if actual_top_ref.as_deref() != Some(expected_top_ref) {
                failures.push(format!(
                    "top_ref expected '{}' but got '{}'",
                    expected_top_ref,
                    actual_top_ref.as_deref().unwrap_or("<none>")
                ));
            }
        }
        if let Some(expected_hit_count) = case.expected_hit_count {
            if result.hits.len() != expected_hit_count {
                failures.push(format!(
                    "hit_count expected {} but got {}",
                    expected_hit_count,
                    result.hits.len()
                ));
            }
        }
        if let Some(expected_short_term_count) = case.expected_short_term_count {
            if result.short_term.len() != expected_short_term_count {
                failures.push(format!(
                    "short_term_count expected {} but got {}",
                    expected_short_term_count,
                    result.short_term.len()
                ));
            }
        }
        if let Some(expected_fallback_count) = case.expected_fallback_count {
            if result.fallback.len() != expected_fallback_count {
                failures.push(format!(
                    "fallback_count expected {} but got {}",
                    expected_fallback_count,
                    result.fallback.len()
                ));
            }
        }
        if let Some(expected_scope_lens) = case.expected_scope_lens.as_deref() {
            if result.diagnostics.scope_lens != expected_scope_lens {
                failures.push(format!(
                    "scope_lens expected '{}' but got '{}'",
                    expected_scope_lens, result.diagnostics.scope_lens
                ));
            }
        }
        for needle in &case.expected_contains {
            if !result.markdown.contains(needle) {
                failures.push(format!("missing expected text '{}'", needle));
            }
        }

        reports.push(ParityReport {
            case_name: case.name.clone(),
            passed: failures.is_empty(),
            actual_top_ref,
            actual_hit_count: result.hits.len(),
            actual_short_term_count: result.short_term.len(),
            actual_fallback_count: result.fallback.len(),
            actual_scope_lens: result.diagnostics.scope_lens,
            failures,
        });
    }
    Ok(reports)
}

pub fn write_parity_reports(path: &Path, reports: &[ParityReport]) -> Result<()> {
    let mut lines = String::new();
    for report in reports {
        lines.push_str(&serde_json::to_string(report)?);
        lines.push('\n');
    }
    fs::write(path, lines).with_context(|| format!("failed to write parity report '{}'", path.display()))
}

fn resolve_relative_path(root: &Path, relative: &str) -> PathBuf {
    let path = Path::new(relative);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

fn parse_multi_value(raw: Option<&str>) -> Vec<String> {
    raw.into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

const fn default_top() -> usize {
    8
}

const fn default_traversal_budget() -> usize {
    3
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
    fn run_parity_validation_reports_pass_for_matching_case() {
        let reports = run_parity_validation(
            &make_corpus(),
            &[ParityCase {
                name: "basic".to_string(),
                input: None,
                query: "search cache".to_string(),
                top: 5,
                explain: true,
                binder: None,
                seed_card: None,
                state: None,
                include_retracted: false,
                current_node: None,
                parent_node: None,
                grandparent_node: None,
                role: None,
                mode: None,
                failure_bucket: None,
                artifact: None,
                traversal_budget: 3,
                no_active_thread_context: false,
                route_replay: None,
                expected_top_ref: Some("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1".to_string()),
                expected_hit_count: Some(1),
                expected_short_term_count: None,
                expected_fallback_count: None,
                expected_scope_lens: Some("global".to_string()),
                expected_contains: vec!["# AGENT MEMORY".to_string(), "## Explain".to_string()],
            }],
            Path::new("."),
        )
        .unwrap();

        assert_eq!(reports.len(), 1);
        assert!(reports[0].passed);
        assert!(reports[0].failures.is_empty());
    }
}
