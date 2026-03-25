use std::collections::{BTreeMap, BTreeSet};
use std::fs::{File, OpenOptions};
use std::io::{BufRead, BufReader, Write};
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use uuid::Uuid;

use crate::active_inference::{
    compute_epistemic_value, compute_expected_free_energy, decay_precision, update_precision,
    StereotypePrior,
};
use crate::context::QueryContext;

#[derive(Clone, Debug, PartialEq)]
pub struct RouteMemoryTargetBias {
    pub target_card_id: Uuid,
    pub bias: f64,
    pub strong_wins: usize,
    pub weak_wins: usize,
    pub candidate_misses: usize,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RouteMemoryBiasOptions {
    pub min_strong_wins_to_activate: usize,
    pub bias_scale: f64,
    pub min_bias_to_apply: f64,
    pub max_episodes: usize,
}

impl Default for RouteMemoryBiasOptions {
    fn default() -> Self {
        Self {
            min_strong_wins_to_activate: 1,
            bias_scale: 1.0,
            min_bias_to_apply: 0.0001,
            max_episodes: 16,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RouteReplayRecord {
    pub query: String,
    #[serde(default)]
    pub top: usize,
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
    pub no_active_thread_context: bool,
    #[serde(default)]
    pub episodes: Vec<RouteReplayEpisodeEntry>,
    #[serde(default)]
    pub expected_refs: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RouteReplayEpisodeEntry {
    pub frame: RouteReplayFrameInput,
    pub route: RouteReplayRouteInput,
    pub episode: RouteReplayEpisodeInput,
    #[serde(default)]
    pub candidate_target_refs: Vec<String>,
    pub winning_target_ref: String,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteReplayFrameInput {
    pub scope_lens: String,
    pub agent_role: String,
    pub mode: String,
    #[serde(default)]
    pub lineage_node_ids: Vec<String>,
    #[serde(default)]
    pub artifact_refs: Option<Vec<String>>,
    #[serde(default)]
    pub failure_bucket: Option<String>,
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct RouteReplayRouteInput {
    pub ranking_source: String,
    pub path: String,
    pub cost: f64,
    #[serde(default)]
    pub risk_flags: Option<Vec<String>>,
}

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct RouteReplayEpisodeInput {
    pub query_text: String,
    pub occurred_at: DateTime<FixedOffset>,
    pub weak_result: bool,
    pub used_fallback: bool,
    pub winning_target_ref: String,
    #[serde(default)]
    pub top_target_refs: Vec<String>,
    #[serde(default)]
    pub user_feedback: Option<UserFeedback>,
    #[serde(default)]
    pub tool_outcome: Option<ToolOutcome>,
}

/// Classification of a tool use outcome for FEP prediction.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash, PartialOrd, Ord, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ToolOutcome {
    Success,
    Weak,
    Null,
    Error,
    Wasteful,
}

impl std::fmt::Display for ToolOutcome {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match self {
            ToolOutcome::Success => write!(f, "Success"),
            ToolOutcome::Weak => write!(f, "Weak"),
            ToolOutcome::Null => write!(f, "Null"),
            ToolOutcome::Error => write!(f, "Error"),
            ToolOutcome::Wasteful => write!(f, "Wasteful"),
        }
    }
}

/// Classify the outcome of a tool use episode from route replay signals.
pub fn classify_tool_outcome(entry: &RouteReplayEpisodeEntry) -> ToolOutcome {
    let cost = entry.route.cost;
    let weak = entry.episode.weak_result;
    let fallback = entry.episode.used_fallback;
    let risk_flags = entry.route.risk_flags.as_deref().unwrap_or(&[]);
    let candidate_count = entry.candidate_target_refs.len();
    let has_global_fallback = risk_flags.iter().any(|f| f == "global-fallback");

    // Error: very high cost
    if cost > 0.95 {
        return ToolOutcome::Error;
    }

    // Null: weak result with no candidates
    if weak && candidate_count == 0 {
        return ToolOutcome::Null;
    }

    // Wasteful: used fallback with high cost, or many candidates + weak
    if (fallback && cost > 0.7) || (candidate_count >= 3 && weak) {
        return ToolOutcome::Wasteful;
    }

    // Weak: weak result, global-fallback flag, or 3+ risk flags
    if weak || has_global_fallback || risk_flags.len() >= 3 {
        return ToolOutcome::Weak;
    }

    ToolOutcome::Success
}

/// RLHF feedback signal attached to a route episode.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum UserFeedback {
    /// User confirmed the result was helpful.
    Positive,
    /// User indicated the result was not what they wanted.
    Negative,
    /// User specified a different card/container as the correct answer.
    Correction { intended_ref: String },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct RetrievalFrameFingerprint {
    pub scope_lens: String,
    pub agent_role: String,
    pub mode: String,
    pub failure_bucket: Option<String>,
    pub lineage_node_ids: Vec<String>,
    pub artifact_refs: Vec<String>,
}

impl RetrievalFrameFingerprint {
    pub fn frame_key(&self) -> String {
        let failure_bucket = normalize(self.failure_bucket.as_deref());
        let lineage = self
            .lineage_node_ids
            .iter()
            .map(|value| normalize(Some(value.as_str())))
            .collect::<Vec<_>>()
            .join(">");
        let artifacts = self
            .artifact_refs
            .iter()
            .map(|value| normalize_artifact(value))
            .collect::<Vec<_>>()
            .join(">");
        format!(
            "scope={}|role={}|mode={}|failure={}|lineage={}|artifacts={}",
            normalize(Some(self.scope_lens.as_str())),
            normalize(Some(self.agent_role.as_str())),
            normalize(Some(self.mode.as_str())),
            failure_bucket,
            lineage,
            artifacts
        )
    }
}

#[derive(Clone, Debug, Default)]
pub struct RouteMemoryStore {
    episodes_by_frame: BTreeMap<String, Vec<RouteReplayEpisodeEntry>>,
}

impl RouteMemoryStore {
    pub fn from_replay_records(records: &[RouteReplayRecord]) -> Self {
        let mut episodes_by_frame: BTreeMap<String, Vec<RouteReplayEpisodeEntry>> = BTreeMap::new();
        for record in records {
            for entry in &record.episodes {
                let key = frame_from_input(&entry.frame).frame_key();
                episodes_by_frame.entry(key).or_default().push(entry.clone());
            }
        }
        Self { episodes_by_frame }
    }

    pub fn from_episodes<I>(episodes: I) -> Self
    where
        I: IntoIterator<Item = RouteReplayEpisodeEntry>,
    {
        let mut episodes_by_frame: BTreeMap<String, Vec<RouteReplayEpisodeEntry>> = BTreeMap::new();
        for entry in episodes {
            let key = frame_from_input(&entry.frame).frame_key();
            episodes_by_frame.entry(key).or_default().push(entry);
        }
        Self { episodes_by_frame }
    }

    pub fn get_target_biases(
        &self,
        context: &QueryContext,
        options: &RouteMemoryBiasOptions,
    ) -> BTreeMap<Uuid, RouteMemoryTargetBias> {
        if options.max_episodes == 0 || options.bias_scale <= 0.0 {
            return BTreeMap::new();
        }

        let Some(frame) = build_frame_fingerprint(context) else {
            return BTreeMap::new();
        };
        let Some(episodes) = self.episodes_by_frame.get(&frame.frame_key()) else {
            return BTreeMap::new();
        };

        let mut sorted = episodes.clone();
        sorted.sort_by(|left, right| right.episode.occurred_at.cmp(&left.episode.occurred_at));

        let mut accumulators: BTreeMap<Uuid, BiasAccumulator> = BTreeMap::new();
        for entry in sorted.into_iter().take(options.max_episodes) {
            let Some(result_card_id) = parse_target_card_id(&entry.winning_target_ref) else {
                continue;
            };

            let strong_result = !entry.episode.weak_result && !entry.episode.used_fallback;
            let risk_penalty = episode_route_risk_penalty(entry.route.risk_flags.as_deref().unwrap_or(&[]));
            let result_delta = if strong_result {
                (0.14 - risk_penalty).max(0.06)
            } else {
                -(0.10 + risk_penalty + if entry.episode.used_fallback { 0.04 } else { 0.0 })
            };

            let accumulator = accumulators.entry(result_card_id).or_default();
            accumulator.bias += result_delta;
            if strong_result {
                accumulator.strong_wins += 1;
            } else {
                accumulator.weak_wins += 1;
            }

            if !strong_result {
                continue;
            }

            let candidate_ids = entry
                .candidate_target_refs
                .iter()
                .filter_map(|target_ref| parse_target_card_id(target_ref))
                .filter(|candidate_id| *candidate_id != result_card_id)
                .collect::<BTreeSet<_>>();
            for candidate_id in candidate_ids {
                let accumulator = accumulators.entry(candidate_id).or_default();
                accumulator.bias -= 0.03;
                accumulator.candidate_misses += 1;
            }
        }

        let mut result = BTreeMap::new();
        for (card_id, accumulator) in accumulators {
            let mut raw_bias = accumulator.bias.clamp(-0.20, 0.24);
            if raw_bias > 0.0 && accumulator.strong_wins < options.min_strong_wins_to_activate {
                raw_bias = 0.0;
            }

            let mut scaled_bias = raw_bias * options.bias_scale;
            if scaled_bias.abs() < options.min_bias_to_apply {
                scaled_bias = 0.0;
            }

            if scaled_bias.abs() < 0.0001 && accumulator.strong_wins == 0 && accumulator.weak_wins == 0 {
                continue;
            }

            result.insert(
                card_id,
                RouteMemoryTargetBias {
                    target_card_id: card_id,
                    bias: scaled_bias,
                    strong_wins: accumulator.strong_wins,
                    weak_wins: accumulator.weak_wins,
                    candidate_misses: accumulator.candidate_misses,
                },
            );
        }
        result
    }
}

// ---------------------------------------------------------------------------
// Phase 5: Precision Learning — per-edge learned costs
// ---------------------------------------------------------------------------

/// A single precision observation for an edge, recording the prediction error.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct PrecisionObservation {
    pub edge_id: String,
    pub observed_error: f64,
    pub timestamp: DateTime<FixedOffset>,
}

/// Per-edge learned precision state.
#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct EdgePrecision {
    pub edge_id: String,
    pub precision: f64,
    pub default_precision: f64,
}

impl EdgePrecision {
    pub fn new(edge_id: String, default_precision: f64) -> Self {
        Self {
            edge_id,
            precision: default_precision,
            default_precision,
        }
    }

    /// Apply a batch of observations to update precision, then decay toward default.
    pub fn learn(&mut self, observations: &[f64], decay_rate: f64) {
        self.precision = update_precision(observations, self.precision);
        self.precision = decay_precision(self.precision, self.default_precision, decay_rate);
    }

    /// Return the learned cost for this edge: default_cost / precision.
    /// Higher precision means lower cost (more reliable edge).
    pub fn learned_cost(&self, default_cost: f64) -> f64 {
        let effective_precision = self.precision.max(0.01);
        default_cost / effective_precision
    }
}

/// Store for per-edge precision values, keyed by edge_id.
#[derive(Clone, Debug, Default)]
pub struct EdgePrecisionStore {
    precisions: BTreeMap<String, EdgePrecision>,
}

impl EdgePrecisionStore {
    pub fn get(&self, edge_id: &str) -> Option<&EdgePrecision> {
        self.precisions.get(edge_id)
    }

    pub fn get_or_default(&mut self, edge_id: &str, default_precision: f64) -> &mut EdgePrecision {
        self.precisions
            .entry(edge_id.to_string())
            .or_insert_with(|| EdgePrecision::new(edge_id.to_string(), default_precision))
    }

    /// Record observations and update precision for an edge.
    pub fn observe(&mut self, edge_id: &str, observations: &[f64], default_precision: f64, decay_rate: f64) {
        let entry = self.get_or_default(edge_id, default_precision);
        entry.learn(observations, decay_rate);
    }

    /// Return the learned cost for an edge, or the default cost if no precision data exists.
    pub fn learned_cost_for_edge(&self, edge_id: &str, default_cost: f64) -> f64 {
        match self.precisions.get(edge_id) {
            Some(ep) => ep.learned_cost(default_cost),
            None => default_cost,
        }
    }

    pub fn all_precisions(&self) -> &BTreeMap<String, EdgePrecision> {
        &self.precisions
    }
}

/// Compute EFE-based route biases using active inference.
///
/// For each target card that appears in route episodes, computes the expected free energy
/// G(route) = -pragmatic_value - epistemic_value, where:
/// - pragmatic_value is derived from win/loss history (strong wins increase, weak/fallback decrease)
/// - epistemic_value is the information gain (expected reduction in free energy from the prior)
///
/// Returns biases sorted by expected free energy (lowest = best).
pub fn compute_efe_biases(
    route_memory: &RouteMemoryStore,
    context: &QueryContext,
    priors: &[StereotypePrior],
    options: &RouteMemoryBiasOptions,
) -> Vec<RouteMemoryTargetBias> {
    if options.max_episodes == 0 || options.bias_scale <= 0.0 {
        return Vec::new();
    }

    let Some(frame) = build_frame_fingerprint(context) else {
        return Vec::new();
    };
    let Some(episodes) = route_memory.episodes_by_frame.get(&frame.frame_key()) else {
        return Vec::new();
    };

    let mut sorted = episodes.clone();
    sorted.sort_by(|left, right| right.episode.occurred_at.cmp(&left.episode.occurred_at));

    // Build a prior lookup by container_id
    let prior_map: BTreeMap<&str, &StereotypePrior> = priors
        .iter()
        .map(|p| (p.container_id.as_str(), p))
        .collect();

    // Accumulate pragmatic value per target from episode history
    let mut pragmatic_values: BTreeMap<Uuid, f64> = BTreeMap::new();
    let mut episode_counts: BTreeMap<Uuid, usize> = BTreeMap::new();

    for entry in sorted.into_iter().take(options.max_episodes) {
        let Some(result_card_id) = parse_target_card_id(&entry.winning_target_ref) else {
            continue;
        };

        let strong_result = !entry.episode.weak_result && !entry.episode.used_fallback;
        let risk_penalty = episode_route_risk_penalty(
            entry.route.risk_flags.as_deref().unwrap_or(&[]),
        );

        // Pragmatic value: positive for strong wins, negative for weak/fallback
        let pragmatic_delta = if strong_result {
            (0.14 - risk_penalty).max(0.06)
        } else {
            -(0.10 + risk_penalty + if entry.episode.used_fallback { 0.04 } else { 0.0 })
        };

        *pragmatic_values.entry(result_card_id).or_default() += pragmatic_delta;
        *episode_counts.entry(result_card_id).or_default() += 1;
    }

    // Compute EFE for each target and convert to biases
    let mut biases: Vec<RouteMemoryTargetBias> = pragmatic_values
        .iter()
        .map(|(&card_id, &pragmatic_value)| {
            // Compute epistemic value from priors if available
            let card_id_str = card_id.to_string();
            let container_id = format!("chat-session:{card_id_str}");
            let epistemic_value = prior_map
                .get(container_id.as_str())
                .map(|prior| {
                    let mut observations = BTreeMap::new();
                    observations.insert("pragmatic_value".to_string(), pragmatic_value);
                    compute_epistemic_value(prior, &observations)
                })
                .unwrap_or(0.0);

            let efe = compute_expected_free_energy(
                // Prior is used for the signature but EFE only needs pragmatic + epistemic
                priors.first().unwrap_or(&StereotypePrior {
                    container_id: String::new(),
                    attribute_priors: BTreeMap::new(),
                }),
                pragmatic_value,
                epistemic_value,
            );

            // Convert EFE to bias: lower EFE (more negative) = better = positive bias
            // Normalize: EFE is already negative for good routes, so negate to get positive bias
            let raw_bias = (-efe).clamp(-0.20, 0.24) * options.bias_scale;
            let count = episode_counts.get(&card_id).copied().unwrap_or(0);
            let strong_wins = if pragmatic_value > 0.0 { count } else { 0 };
            let weak_wins = if pragmatic_value <= 0.0 { count } else { 0 };

            RouteMemoryTargetBias {
                target_card_id: card_id,
                bias: if raw_bias.abs() < options.min_bias_to_apply { 0.0 } else { raw_bias },
                strong_wins,
                weak_wins,
                candidate_misses: 0,
            }
        })
        .filter(|bias| bias.bias.abs() >= 0.0001 || bias.strong_wins > 0 || bias.weak_wins > 0)
        .collect();

    // Sort by expected free energy (lowest = best = highest bias)
    biases.sort_by(|a, b| {
        b.bias
            .partial_cmp(&a.bias)
            .unwrap_or(std::cmp::Ordering::Equal)
    });

    biases
}

pub fn load_route_replay_records(path: &Path) -> Result<Vec<RouteReplayRecord>> {
    let file = File::open(path).with_context(|| format!("failed to open replay input '{}'", path.display()))?;
    let reader = BufReader::new(file);
    let mut records = Vec::new();
    for (index, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("failed to read line {} from '{}'", index + 1, path.display()))?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("//") {
            continue;
        }
        let record: RouteReplayRecord = serde_json::from_str(trimmed).with_context(|| {
            format!(
                "failed to parse route-replay record on line {} in '{}'",
                index + 1,
                path.display()
            )
        })?;
        records.push(record);
    }
    Ok(records)
}

pub fn load_route_episode_entries(path: &Path) -> Result<Vec<RouteReplayEpisodeEntry>> {
    let file =
        File::open(path).with_context(|| format!("failed to open route-memory input '{}'", path.display()))?;
    let reader = BufReader::new(file);
    let mut episodes = Vec::new();
    for (index, line) in reader.lines().enumerate() {
        let line =
            line.with_context(|| format!("failed to read line {} from '{}'", index + 1, path.display()))?;
        let trimmed = line.trim();
        if trimmed.is_empty() || trimmed.starts_with("//") {
            continue;
        }
        let episode: RouteReplayEpisodeEntry = serde_json::from_str(trimmed).with_context(|| {
            format!(
                "failed to parse route-memory episode on line {} in '{}'",
                index + 1,
                path.display()
            )
        })?;
        episodes.push(episode);
    }
    Ok(episodes)
}

pub fn append_route_episode_entry(path: &Path, episode: &RouteReplayEpisodeEntry) -> Result<()> {
    if let Some(parent) = path.parent() {
        std::fs::create_dir_all(parent).with_context(|| {
            format!(
                "failed to create route-memory directory '{}'",
                parent.display()
            )
        })?;
    }

    let mut file = OpenOptions::new()
        .create(true)
        .append(true)
        .open(path)
        .with_context(|| format!("failed to open route-memory output '{}'", path.display()))?;
    writeln!(file, "{}", serde_json::to_string(episode)?)
        .with_context(|| format!("failed to append route-memory entry '{}'", path.display()))
}

pub fn default_route_memory_path(input: &Path) -> PathBuf {
    let file_name = input
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or("route-memory");
    let route_file_name = if let Some(prefix) = file_name.strip_suffix(".memory.jsonl") {
        format!("{prefix}.route-memory.jsonl")
    } else if let Some(prefix) = file_name.strip_suffix(".jsonl") {
        format!("{prefix}.route-memory.jsonl")
    } else {
        format!("{file_name}.route-memory.jsonl")
    };
    input.with_file_name(route_file_name)
}

pub fn build_frame_fingerprint(context: &QueryContext) -> Option<RetrievalFrameFingerprint> {
    if !context.has_lineage() && context.failure_bucket.is_none() {
        return None;
    }

    Some(RetrievalFrameFingerprint {
        scope_lens: context.scope_lens().to_string(),
        agent_role: context.agent_role.clone(),
        mode: context.mode.clone(),
        failure_bucket: context.failure_bucket.clone(),
        lineage_node_ids: context.lineage.iter().map(|scope| scope.node_id.clone()).collect(),
        artifact_refs: context.active_artifacts.clone(),
    })
}

pub fn parse_target_card_id(raw: &str) -> Option<Uuid> {
    let trimmed = raw.trim();
    if trimmed.is_empty() {
        return None;
    }
    if let Ok(parsed) = Uuid::parse_str(trimmed) {
        return Some(parsed);
    }

    for candidate in [trimmed.rsplit(':').next(), trimmed.rsplit('/').next()] {
        let Some(candidate) = candidate.map(str::trim).filter(|value| !value.is_empty()) else {
            continue;
        };
        if let Ok(parsed) = Uuid::parse_str(candidate) {
            return Some(parsed);
        }
    }
    None
}

pub fn canonical_target_ref(raw: &str) -> String {
    parse_target_card_id(raw)
        .map(|card_id| card_id.to_string())
        .unwrap_or_else(|| raw.trim().to_string())
}

fn frame_from_input(input: &RouteReplayFrameInput) -> RetrievalFrameFingerprint {
    RetrievalFrameFingerprint {
        scope_lens: input.scope_lens.clone(),
        agent_role: input.agent_role.clone(),
        mode: input.mode.clone(),
        failure_bucket: input.failure_bucket.clone(),
        lineage_node_ids: input.lineage_node_ids.clone(),
        artifact_refs: input.artifact_refs.clone().unwrap_or_default(),
    }
}

fn normalize(value: Option<&str>) -> String {
    value.map(str::trim).unwrap_or_default().to_string()
}

fn normalize_artifact(value: &str) -> String {
    let normalized = normalize(Some(value));
    if normalized.is_empty() {
        return normalized;
    }

    Path::new(&normalized)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(normalized.as_str())
        .to_ascii_lowercase()
}

fn episode_route_risk_penalty(risk_flags: &[String]) -> f64 {
    let flags = risk_flags
        .iter()
        .map(|value| value.as_str())
        .collect::<BTreeSet<_>>();
    let mut penalty = 0.0;
    if flags.contains("global-fallback") {
        penalty += 0.03;
    }
    if flags.contains("broad-node") {
        penalty += 0.02;
    }
    if flags.contains("low-specificity") {
        penalty += 0.02;
    }
    if flags.contains("representative-mismatch") {
        penalty += 0.01;
    }
    penalty
}

#[derive(Clone, Debug, Default)]
struct BiasAccumulator {
    bias: f64,
    strong_wins: usize,
    weak_wins: usize,
    candidate_misses: usize,
}

#[cfg(test)]
mod tests {
    use crate::context::{LineageScope, QueryContext};

    use super::*;

    fn make_context() -> QueryContext {
        QueryContext {
            lineage: vec![LineageScope {
                level: "self".to_string(),
                object_id: "task-thread:child".to_string(),
                node_id: "child-thread".to_string(),
                title: "child-thread".to_string(),
                current_step: String::new(),
                next_command: String::new(),
                branch_off_anchor: None,
                artifact_refs: vec!["src/MemoryGraph.Application/RetrievalService.cs".to_string()],
            }],
            agent_role: "implementer".to_string(),
            mode: "build".to_string(),
            failure_bucket: None,
            active_artifacts: vec!["src/MemoryGraph.Application/RetrievalService.cs".to_string()],
            traversal_budget: 3,
            source: "explicit".to_string(),
        }
    }

    fn make_entry(
        occurred_at: &str,
        winning_target_ref: &str,
        candidate_target_refs: Vec<String>,
        weak_result: bool,
        used_fallback: bool,
        risk_flags: Vec<String>,
    ) -> RouteReplayEpisodeEntry {
        RouteReplayEpisodeEntry {
            frame: RouteReplayFrameInput {
                scope_lens: "local-first-lineage".to_string(),
                agent_role: "implementer".to_string(),
                mode: "build".to_string(),
                lineage_node_ids: vec!["child-thread".to_string()],
                artifact_refs: Some(vec!["src/MemoryGraph.Application/RetrievalService.cs".to_string()]),
                failure_bucket: None,
            },
            route: RouteReplayRouteInput {
                ranking_source: "raw-lesson".to_string(),
                path: "retrieval-graph:self-thread -> in-bucket".to_string(),
                cost: 0.55,
                risk_flags: Some(risk_flags),
            },
            episode: RouteReplayEpisodeInput {
                query_text: "retrieval graph cutover note".to_string(),
                occurred_at: DateTime::parse_from_rfc3339(occurred_at).unwrap(),
                weak_result,
                used_fallback,
                winning_target_ref: winning_target_ref.to_string(),
                top_target_refs: vec![winning_target_ref.to_string()],
                user_feedback: None,
                tool_outcome: None,
            },
            candidate_target_refs,
            winning_target_ref: winning_target_ref.to_string(),
        }
    }

    #[test]
    fn get_target_biases_rewards_strong_wins_and_penalizes_weak_fallback() {
        let local = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap();
        let broad = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2").unwrap();
        let store = RouteMemoryStore::from_episodes([
            make_entry(
                "2026-03-10T08:00:00+00:00",
                &local.to_string(),
                vec![local.to_string(), broad.to_string()],
                false,
                false,
                vec![],
            ),
            make_entry(
                "2026-03-10T08:05:00+00:00",
                &broad.to_string(),
                vec![local.to_string()],
                true,
                true,
                vec!["global-fallback".to_string(), "broad-node".to_string()],
            ),
        ]);

        let biases = store.get_target_biases(&make_context(), &RouteMemoryBiasOptions::default());

        assert!(biases.get(&local).is_some_and(|bias| bias.bias > 0.0));
        assert!(biases.get(&broad).is_some_and(|bias| bias.bias < 0.0));
        assert_eq!(biases.get(&local).unwrap().strong_wins, 1);
        assert_eq!(biases.get(&broad).unwrap().weak_wins, 1);
    }

    #[test]
    fn get_target_biases_requires_exact_frame_match() {
        let local = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap();
        let store = RouteMemoryStore::from_episodes([make_entry(
            "2026-03-10T08:00:00+00:00",
            &local.to_string(),
            vec![local.to_string()],
            false,
            false,
            vec![],
        )]);
        let mut context = make_context();
        context.mode = "review".to_string();

        let biases = store.get_target_biases(&context, &RouteMemoryBiasOptions::default());

        assert!(biases.is_empty());
    }

    #[test]
    fn bias_options_require_two_wins_and_scale_bias() {
        let local = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap();
        let store = RouteMemoryStore::from_episodes([make_entry(
            "2026-03-10T08:00:00+00:00",
            &local.to_string(),
            vec![local.to_string()],
            false,
            false,
            vec![],
        )]);

        let default_bias = store
            .get_target_biases(&make_context(), &RouteMemoryBiasOptions::default())
            .get(&local)
            .unwrap()
            .bias;
        let strict = RouteMemoryBiasOptions {
            min_strong_wins_to_activate: 2,
            ..RouteMemoryBiasOptions::default()
        };
        let scaled = RouteMemoryBiasOptions {
            bias_scale: 0.5,
            ..RouteMemoryBiasOptions::default()
        };

        let strict_bias = store.get_target_biases(&make_context(), &strict).get(&local).unwrap().bias;
        let scaled_bias = store.get_target_biases(&make_context(), &scaled).get(&local).unwrap().bias;

        assert!(default_bias > 0.0);
        assert_eq!(strict_bias, 0.0);
        assert!((scaled_bias - (default_bias * 0.5)).abs() < 0.00001);
    }

    #[test]
    fn parse_target_card_id_accepts_prefixed_refs() {
        let card_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap();
        assert_eq!(parse_target_card_id(&card_id.to_string()), Some(card_id));
        assert_eq!(parse_target_card_id(&format!("card:{card_id}")), Some(card_id));
        assert_eq!(parse_target_card_id(&format!("chat-session:{card_id}")), Some(card_id));
        assert_eq!(
            parse_target_card_id(&format!("https://example.com/{card_id}")),
            Some(card_id)
        );
    }

    #[test]
    fn load_route_replay_records_skips_comments_and_blank_lines() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        std::fs::write(
            temp.path(),
            "// comment\n\n{\"query\":\"q\",\"top\":1,\"episodes\":[]}\n",
        )
        .unwrap();

        let records = load_route_replay_records(temp.path()).unwrap();

        assert_eq!(records.len(), 1);
        assert_eq!(records[0].query, "q");
    }

    #[test]
    fn default_route_memory_path_rewrites_memory_jsonl_name() {
        let path = Path::new(r"C:\tmp\all-agents-sessions.memory.jsonl");

        let route_path = default_route_memory_path(path);

        assert_eq!(
            route_path,
            PathBuf::from(r"C:\tmp\all-agents-sessions.route-memory.jsonl")
        );
    }

    #[test]
    fn append_and_load_route_episode_entries_roundtrip() {
        let temp = tempfile::NamedTempFile::new().unwrap();
        let episode = make_entry(
            "2026-03-10T08:00:00+00:00",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1",
            vec!["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1".to_string()],
            false,
            false,
            vec![],
        );

        append_route_episode_entry(temp.path(), &episode).unwrap();
        let episodes = load_route_episode_entries(temp.path()).unwrap();

        assert_eq!(episodes, vec![episode]);
    }

    // Phase 5: Precision learning tests

    #[test]
    fn learned_cost_overrides_default_in_route_scoring() {
        let mut store = EdgePrecisionStore::default();
        let default_cost = 1.0;

        // High precision -> lower learned cost
        let ep = store.get_or_default("edge:parent", 1.0);
        ep.precision = 2.0; // double precision

        let learned = store.learned_cost_for_edge("edge:parent", default_cost);
        assert!((learned - 0.5).abs() < f64::EPSILON); // 1.0 / 2.0

        // Unknown edge returns default
        let unknown = store.learned_cost_for_edge("edge:unknown", default_cost);
        assert!((unknown - default_cost).abs() < f64::EPSILON);
    }

    #[test]
    fn decay_returns_precision_to_defaults_over_time() {
        let mut store = EdgePrecisionStore::default();
        let default_precision = 1.0;
        let ep = store.get_or_default("edge:test", default_precision);
        ep.precision = 5.0; // artificially high

        // Apply decay repeatedly
        for _ in 0..20 {
            ep.learn(&[], 0.3); // empty observations, just decay
        }

        // Should have decayed close to default
        assert!((ep.precision - default_precision).abs() < 0.01);
    }

    #[test]
    fn edge_precision_observe_updates_and_decays() {
        let mut store = EdgePrecisionStore::default();
        let default_precision = 1.0;

        // Low-error observations should increase precision
        store.observe("edge:reliable", &[0.01, 0.02], default_precision, 0.1);
        let reliable_precision = store.get("edge:reliable").unwrap().precision;
        assert!(reliable_precision > default_precision * 0.9);

        // High-error observations should decrease precision
        store.observe("edge:unreliable", &[5.0, 6.0], default_precision, 0.1);
        let unreliable_precision = store.get("edge:unreliable").unwrap().precision;
        assert!(unreliable_precision < reliable_precision);
    }

    // Phase 4: EFE bias tests

    #[test]
    fn compute_efe_biases_returns_empty_for_no_episodes() {
        let store = RouteMemoryStore::default();
        let context = make_context();
        let biases = compute_efe_biases(&store, &context, &[], &RouteMemoryBiasOptions::default());
        assert!(biases.is_empty());
    }

    #[test]
    fn compute_efe_biases_returns_empty_with_zero_max_episodes() {
        let local = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap();
        let store = RouteMemoryStore::from_episodes([make_entry(
            "2026-03-10T08:00:00+00:00",
            &local.to_string(),
            vec![],
            false,
            false,
            vec![],
        )]);
        let options = RouteMemoryBiasOptions {
            max_episodes: 0,
            ..RouteMemoryBiasOptions::default()
        };
        let biases = compute_efe_biases(&store, &make_context(), &[], &options);
        assert!(biases.is_empty());
    }

    #[test]
    fn compute_efe_biases_positive_for_strong_wins() {
        let local = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap();
        let store = RouteMemoryStore::from_episodes([
            make_entry(
                "2026-03-10T08:00:00+00:00",
                &local.to_string(),
                vec![],
                false,
                false,
                vec![],
            ),
            make_entry(
                "2026-03-10T08:05:00+00:00",
                &local.to_string(),
                vec![],
                false,
                false,
                vec![],
            ),
        ]);

        let biases = compute_efe_biases(
            &store,
            &make_context(),
            &[],
            &RouteMemoryBiasOptions::default(),
        );

        assert!(!biases.is_empty());
        assert!(biases[0].bias > 0.0);
        assert_eq!(biases[0].target_card_id, local);
    }

    #[test]
    fn compute_efe_biases_negative_for_weak_results() {
        let weak_target = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2").unwrap();
        let store = RouteMemoryStore::from_episodes([make_entry(
            "2026-03-10T08:00:00+00:00",
            &weak_target.to_string(),
            vec![],
            true,
            true,
            vec!["global-fallback".to_string()],
        )]);

        let biases = compute_efe_biases(
            &store,
            &make_context(),
            &[],
            &RouteMemoryBiasOptions::default(),
        );

        assert!(!biases.is_empty());
        assert!(biases.iter().any(|b| b.target_card_id == weak_target && b.bias < 0.0));
    }

    #[test]
    fn compute_efe_biases_sorted_best_first() {
        let good = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap();
        let bad = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2").unwrap();
        let store = RouteMemoryStore::from_episodes([
            make_entry(
                "2026-03-10T08:00:00+00:00",
                &good.to_string(),
                vec![],
                false,
                false,
                vec![],
            ),
            make_entry(
                "2026-03-10T08:05:00+00:00",
                &bad.to_string(),
                vec![],
                true,
                true,
                vec!["global-fallback".to_string(), "broad-node".to_string()],
            ),
        ]);

        let biases = compute_efe_biases(
            &store,
            &make_context(),
            &[],
            &RouteMemoryBiasOptions::default(),
        );

        assert!(biases.len() >= 2);
        // Best route (highest bias) should be first
        assert!(biases[0].bias >= biases[1].bias);
        assert_eq!(biases[0].target_card_id, good);
    }

    #[test]
    fn compute_efe_biases_requires_frame_match() {
        let local = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap();
        let store = RouteMemoryStore::from_episodes([make_entry(
            "2026-03-10T08:00:00+00:00",
            &local.to_string(),
            vec![],
            false,
            false,
            vec![],
        )]);
        let mut context = make_context();
        context.mode = "review".to_string();

        let biases = compute_efe_biases(
            &store,
            &context,
            &[],
            &RouteMemoryBiasOptions::default(),
        );
        assert!(biases.is_empty());
    }

    #[test]
    fn compute_efe_biases_respects_bias_scale() {
        let local = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap();
        let store = RouteMemoryStore::from_episodes([make_entry(
            "2026-03-10T08:00:00+00:00",
            &local.to_string(),
            vec![],
            false,
            false,
            vec![],
        )]);

        let biases_default = compute_efe_biases(
            &store,
            &make_context(),
            &[],
            &RouteMemoryBiasOptions::default(),
        );
        let biases_scaled = compute_efe_biases(
            &store,
            &make_context(),
            &[],
            &RouteMemoryBiasOptions {
                bias_scale: 0.5,
                ..RouteMemoryBiasOptions::default()
            },
        );

        if !biases_default.is_empty() && !biases_scaled.is_empty() {
            // Scaled bias should be smaller in magnitude
            assert!(biases_scaled[0].bias.abs() <= biases_default[0].bias.abs() + f64::EPSILON);
        }
    }

    // Tool outcome classification tests

    fn make_outcome_entry(
        weak_result: bool,
        used_fallback: bool,
        _cost: f64,
        risk_flags: Vec<String>,
        candidates: usize,
    ) -> RouteReplayEpisodeEntry {
        make_entry(
            "2026-03-10T08:00:00+00:00",
            "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1",
            (0..candidates)
                .map(|i| format!("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaa{:03}", i))
                .collect(),
            weak_result,
            used_fallback,
            risk_flags,
        )
    }

    #[test]
    fn classify_tool_outcome_success() {
        let entry = make_outcome_entry(false, false, 0.5, vec![], 1);
        assert_eq!(classify_tool_outcome(&entry), ToolOutcome::Success);
    }

    #[test]
    fn classify_tool_outcome_error_high_cost() {
        let mut entry = make_outcome_entry(false, false, 0.5, vec![], 1);
        entry.route.cost = 0.96;
        assert_eq!(classify_tool_outcome(&entry), ToolOutcome::Error);
    }

    #[test]
    fn classify_tool_outcome_null_weak_no_candidates() {
        let entry = make_outcome_entry(true, false, 0.5, vec![], 0);
        assert_eq!(classify_tool_outcome(&entry), ToolOutcome::Null);
    }

    #[test]
    fn classify_tool_outcome_wasteful_fallback_high_cost() {
        let mut entry = make_outcome_entry(false, true, 0.5, vec![], 1);
        entry.route.cost = 0.75;
        assert_eq!(classify_tool_outcome(&entry), ToolOutcome::Wasteful);
    }

    #[test]
    fn classify_tool_outcome_wasteful_many_candidates_weak() {
        let entry = make_outcome_entry(true, false, 0.5, vec![], 3);
        assert_eq!(classify_tool_outcome(&entry), ToolOutcome::Wasteful);
    }

    #[test]
    fn classify_tool_outcome_weak_global_fallback() {
        let entry = make_outcome_entry(false, false, 0.5, vec!["global-fallback".to_string()], 1);
        assert_eq!(classify_tool_outcome(&entry), ToolOutcome::Weak);
    }

    #[test]
    fn classify_tool_outcome_weak_many_risk_flags() {
        let entry = make_outcome_entry(
            false,
            false,
            0.5,
            vec![
                "broad-node".to_string(),
                "low-specificity".to_string(),
                "representative-mismatch".to_string(),
            ],
            1,
        );
        assert_eq!(classify_tool_outcome(&entry), ToolOutcome::Weak);
    }

    #[test]
    fn tool_outcome_serde_backward_compat() {
        // Existing JSON without tool_outcome should still deserialize
        let json = r#"{
            "query_text": "test",
            "occurred_at": "2026-03-10T08:00:00+00:00",
            "weak_result": false,
            "used_fallback": false,
            "winning_target_ref": "abc",
            "top_target_refs": []
        }"#;
        let episode: RouteReplayEpisodeInput = serde_json::from_str(json).unwrap();
        assert_eq!(episode.tool_outcome, None);
    }

    #[test]
    fn tool_outcome_serde_roundtrip() {
        let json = r#"{
            "query_text": "test",
            "occurred_at": "2026-03-10T08:00:00+00:00",
            "weak_result": false,
            "used_fallback": false,
            "winning_target_ref": "abc",
            "top_target_refs": [],
            "tool_outcome": "success"
        }"#;
        let episode: RouteReplayEpisodeInput = serde_json::from_str(json).unwrap();
        assert_eq!(episode.tool_outcome, Some(ToolOutcome::Success));

        let serialized = serde_json::to_string(&episode).unwrap();
        assert!(serialized.contains("\"tool_outcome\":\"success\""));
    }
}
