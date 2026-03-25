use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::active_inference::{decay_precision, GaussianParam};
use crate::model::{now_fixed, HypothesisAnnotation};
use crate::route_memory::{RouteReplayEpisodeEntry, ToolOutcome, parse_target_card_id};
use crate::store::AmsStore;
use crate::tool_outcome::{
    bootstrap_tool_outcome_priors, classify_agent_tool_outcome, write_tool_outcome_priors_to_snapshot,
    ToolOutcomeDistribution,
};

// ---------------------------------------------------------------------------
// Container Relevance Prior Bootstrap
// ---------------------------------------------------------------------------

/// Per-container, per-context relevance statistics.
#[derive(Clone, Debug, Default)]
struct RelevanceAccumulator {
    wins: usize,
    appearances: usize,
}

/// Context kind key derived from a route episode frame.
fn context_kind(scope_lens: &str, agent_role: &str) -> String {
    format!("{}:{}", scope_lens, agent_role)
}

/// Bootstrap container relevance priors from route episode history.
///
/// For each container that appears in episodes (as winning or candidate),
/// compute P(useful|context_kind) = wins / appearances with variance inversely
/// proportional to observation count.
pub fn bootstrap_relevance_priors(
    episodes: &[RouteReplayEpisodeEntry],
) -> BTreeMap<String, BTreeMap<String, GaussianParam>> {
    // container_id -> context_kind -> accumulator
    let mut accumulators: BTreeMap<String, BTreeMap<String, RelevanceAccumulator>> = BTreeMap::new();

    for entry in episodes {
        let ctx = context_kind(&entry.frame.scope_lens, &entry.frame.agent_role);

        // The winning target is a "win" for that container
        let winning_ref = &entry.winning_target_ref;
        let winning_id = parse_target_card_id(winning_ref)
            .map(|id| format!("chat-session:{}", id))
            .unwrap_or_else(|| winning_ref.clone());

        // Record win for the winning container
        let acc = accumulators
            .entry(winning_id.clone())
            .or_default()
            .entry(ctx.clone())
            .or_default();
        acc.wins += 1;
        acc.appearances += 1;

        // Record appearance-only for candidates that didn't win
        for candidate_ref in &entry.candidate_target_refs {
            let candidate_id = parse_target_card_id(candidate_ref)
                .map(|id| format!("chat-session:{}", id))
                .unwrap_or_else(|| candidate_ref.clone());
            if candidate_id != winning_id {
                let acc = accumulators
                    .entry(candidate_id)
                    .or_default()
                    .entry(ctx.clone())
                    .or_default();
                acc.appearances += 1;
            }
        }
    }

    // Convert accumulators to Gaussian priors
    let mut result: BTreeMap<String, BTreeMap<String, GaussianParam>> = BTreeMap::new();
    for (container_id, context_map) in accumulators {
        let mut priors = BTreeMap::new();
        for (ctx_kind, acc) in context_map {
            let mean = if acc.appearances > 0 {
                acc.wins as f64 / acc.appearances as f64
            } else {
                0.5 // uninformative prior
            };
            // Variance decreases with more observations (min 0.01)
            let variance = if acc.appearances > 0 {
                (1.0 / (acc.appearances as f64 + 1.0)).max(0.01)
            } else {
                1.0 // high uncertainty
            };
            priors.insert(ctx_kind, GaussianParam { mean, variance });
        }
        result.insert(container_id, priors);
    }

    result
}

/// Write bootstrapped relevance priors into an AMS snapshot's hypothesis_state.
///
/// Each container gets `fep:prior:relevance:{context_kind}` keys with JSON-encoded
/// GaussianParam values.
pub fn write_relevance_priors_to_snapshot(
    snapshot: &mut AmsStore,
    priors: &BTreeMap<String, BTreeMap<String, GaussianParam>>,
) -> usize {
    let now = now_fixed();
    let mut total_written = 0;

    for (container_id, context_priors) in priors {
        let Some(container) = snapshot.containers_mut().get_mut(container_id) else {
            continue;
        };
        for (ctx_kind, param) in context_priors {
            let key = format!("fep:prior:relevance:{}", ctx_kind);
            let value = serde_json::to_string(param).unwrap_or_default();
            container.hypothesis_state.insert(
                key.clone(),
                HypothesisAnnotation {
                    key,
                    value,
                    updated_at: now,
                },
            );
            total_written += 1;
        }
    }

    total_written
}

// ---------------------------------------------------------------------------
// Agent Tool-Call Prior Bootstrap
// ---------------------------------------------------------------------------

const AGENT_TOOL_OUTCOME_CONTAINER: &str = "fep:agent-tool-outcome-priors";

const ALL_OUTCOMES: [ToolOutcome; 5] = [
    ToolOutcome::Success,
    ToolOutcome::Weak,
    ToolOutcome::Null,
    ToolOutcome::Error,
    ToolOutcome::Wasteful,
];

/// Bootstrap agent tool-call priors from objects in the snapshot.
///
/// Walks all objects with `object_kind == "tool-call"`, reads provenance fields
/// (`is_error`, `result_preview`, `tool_name`) from `semantic_payload.provenance`,
/// classifies each via [`classify_agent_tool_outcome`], and builds a per-tool
/// [`ToolOutcomeDistribution`] keyed by `tool_name`.
pub fn bootstrap_agent_tool_priors(
    snapshot: &AmsStore,
) -> BTreeMap<String, ToolOutcomeDistribution> {
    // tool_name -> outcome -> count
    let mut counts: BTreeMap<String, BTreeMap<ToolOutcome, usize>> = BTreeMap::new();
    let mut totals: BTreeMap<String, usize> = BTreeMap::new();

    for (_id, obj) in snapshot.objects() {
        if obj.object_kind != "tool-call" {
            continue;
        }

        let provenance = obj
            .semantic_payload
            .as_ref()
            .and_then(|sp| sp.provenance.as_ref());

        let Some(prov) = provenance else { continue };

        let tool_name = prov
            .get("tool_name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");

        let is_error = prov
            .get("is_error")
            .and_then(|v| v.as_bool())
            .unwrap_or(false);

        let result_preview = prov
            .get("result_preview")
            .and_then(|v| v.as_str())
            .unwrap_or("");

        let outcome = classify_agent_tool_outcome(is_error, result_preview, tool_name);

        *counts
            .entry(tool_name.to_string())
            .or_default()
            .entry(outcome)
            .or_default() += 1;
        *totals.entry(tool_name.to_string()).or_default() += 1;
    }

    let mut result = BTreeMap::new();
    for (tool, outcome_counts) in counts {
        let total = totals[&tool];
        let mut outcome_params = BTreeMap::new();
        for outcome in &ALL_OUTCOMES {
            let count = outcome_counts.get(outcome).copied().unwrap_or(0);
            let mean = if total > 0 {
                count as f64 / total as f64
            } else {
                1.0 / ALL_OUTCOMES.len() as f64
            };
            let variance = if total > 0 {
                (1.0 / (total as f64 + 1.0)).max(0.01)
            } else {
                1.0
            };
            outcome_params.insert(*outcome, GaussianParam { mean, variance });
        }
        result.insert(
            tool.clone(),
            ToolOutcomeDistribution {
                context_key: tool,
                outcome_params,
                total_observations: total,
            },
        );
    }

    result
}

/// Write agent tool-call priors into the snapshot's priors container.
///
/// Each tool gets a `fep:agent-tool:{tool_name}` key with a JSON-encoded
/// [`ToolOutcomeDistribution`] value.
pub fn write_agent_tool_priors_to_snapshot(
    snapshot: &mut AmsStore,
    priors: &BTreeMap<String, ToolOutcomeDistribution>,
) -> usize {
    // Ensure the container exists
    if snapshot
        .containers()
        .get(AGENT_TOOL_OUTCOME_CONTAINER)
        .is_none()
    {
        let _ = snapshot.create_container(
            AGENT_TOOL_OUTCOME_CONTAINER,
            "fep_priors",
            "fep_priors",
        );
    }

    let now = now_fixed();
    let mut total_written = 0;

    let Some(container) = snapshot
        .containers_mut()
        .get_mut(AGENT_TOOL_OUTCOME_CONTAINER)
    else {
        return 0;
    };

    for (tool_name, distribution) in priors {
        let key = format!("fep:agent-tool:{}", tool_name);
        let value = serde_json::to_string(distribution).unwrap_or_default();
        container.hypothesis_state.insert(
            key.clone(),
            HypothesisAnnotation {
                key,
                value,
                updated_at: now,
            },
        );
        total_written += 1;
    }

    total_written
}

/// Load agent tool-call priors from the snapshot.
pub fn load_agent_tool_priors_from_snapshot(
    snapshot: &AmsStore,
) -> BTreeMap<String, ToolOutcomeDistribution> {
    let mut result = BTreeMap::new();

    let Some(container) = snapshot.containers().get(AGENT_TOOL_OUTCOME_CONTAINER) else {
        return result;
    };

    for (key, annotation) in &container.hypothesis_state {
        if let Some(tool_name) = key.strip_prefix("fep:agent-tool:") {
            if let Ok(distribution) =
                serde_json::from_str::<ToolOutcomeDistribution>(&annotation.value)
            {
                result.insert(tool_name.to_string(), distribution);
            }
        }
    }

    result
}

// ---------------------------------------------------------------------------
// Precision Decay
// ---------------------------------------------------------------------------

/// Default precision for tool outcome priors (1/variance at bootstrap time).
const DEFAULT_TOOL_PRIOR_PRECISION: f64 = 1.0;

// ---------------------------------------------------------------------------
// Tool Duration Priors
// ---------------------------------------------------------------------------

const AGENT_TOOL_DURATION_CONTAINER: &str = "fep:agent-tool-duration-priors";

/// Gaussian prior over wall-clock duration (in seconds) for a single tool.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolDurationPrior {
    /// Tool name (e.g. "Bash", "Read").
    pub tool_name: String,
    /// Mean observed duration in seconds.
    pub mean_s: f64,
    /// Variance of observed durations.
    pub variance_s: f64,
    /// Number of observations used to build this prior.
    pub count: usize,
}

/// A slow-tool finding produced by [`detect_slow_tools`].
#[derive(Clone, Debug)]
pub struct SlowToolEntry {
    pub tool_name: String,
    pub tool_use_id: String,
    pub observed_s: f64,
    pub free_energy: f64,
    pub prior_mean_s: f64,
    pub prior_variance_s: f64,
    pub prior_count: usize,
}

/// Bootstrap per-tool duration priors from `tool-call` objects in the snapshot.
///
/// Walks all objects with `object_kind == "tool-call"` that have a numeric
/// `duration_s` in their provenance, and fits a Gaussian (mean, variance)
/// over the observed durations for each `tool_name`.
pub fn bootstrap_tool_duration_priors(
    snapshot: &AmsStore,
) -> BTreeMap<String, ToolDurationPrior> {
    // tool_name -> list of observed duration_s values
    let mut samples: BTreeMap<String, Vec<f64>> = BTreeMap::new();

    for (_id, obj) in snapshot.objects() {
        if obj.object_kind != "tool-call" {
            continue;
        }
        let prov = obj
            .semantic_payload
            .as_ref()
            .and_then(|sp| sp.provenance.as_ref());
        let Some(prov) = prov else { continue };

        let tool_name = prov
            .get("tool_name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let Some(duration_s) = prov.get("duration_s").and_then(|v| v.as_f64()) else {
            continue;
        };
        if duration_s < 0.0 {
            continue;
        }

        samples
            .entry(tool_name.to_string())
            .or_default()
            .push(duration_s);
    }

    let mut result = BTreeMap::new();
    for (tool_name, durations) in samples {
        let n = durations.len();
        if n == 0 {
            continue;
        }
        let mean = durations.iter().copied().sum::<f64>() / n as f64;
        let variance = if n > 1 {
            let sq_diff: f64 = durations.iter().map(|&d| (d - mean).powi(2)).sum();
            (sq_diff / (n - 1) as f64).max(0.01)
        } else {
            // Single observation: use unit variance as uninformative default
            1.0
        };
        result.insert(
            tool_name.clone(),
            ToolDurationPrior {
                tool_name,
                mean_s: mean,
                variance_s: variance,
                count: n,
            },
        );
    }

    result
}

/// Compute the FEP free energy (Gaussian surprise) of an observed duration
/// against a prior.
///
/// Returns `-ln(p(observed | prior))` under a Gaussian with the prior's
/// mean and variance, clamped to `[0, 20]`.
pub fn compute_duration_free_energy(prior: &ToolDurationPrior, observed_s: f64) -> f64 {
    let variance = prior.variance_s.max(0.01);
    let deviation = (observed_s - prior.mean_s).powi(2) / (2.0 * variance);
    let log_norm = 0.5 * (2.0 * std::f64::consts::PI * variance).ln();
    (deviation + log_norm).clamp(0.0, 20.0)
}

/// Write tool duration priors into the snapshot's duration container.
pub fn write_tool_duration_priors_to_snapshot(
    snapshot: &mut AmsStore,
    priors: &BTreeMap<String, ToolDurationPrior>,
) -> usize {
    if snapshot
        .containers()
        .get(AGENT_TOOL_DURATION_CONTAINER)
        .is_none()
    {
        let _ = snapshot.create_container(
            AGENT_TOOL_DURATION_CONTAINER,
            "fep_priors",
            "fep_priors",
        );
    }

    let now = now_fixed();
    let mut total_written = 0;

    let Some(container) = snapshot
        .containers_mut()
        .get_mut(AGENT_TOOL_DURATION_CONTAINER)
    else {
        return 0;
    };

    for (tool_name, prior) in priors {
        let key = format!("fep:tool-duration:{}", tool_name);
        let value = serde_json::to_string(prior).unwrap_or_default();
        container.hypothesis_state.insert(
            key.clone(),
            HypothesisAnnotation {
                key,
                value,
                updated_at: now,
            },
        );
        total_written += 1;
    }

    total_written
}

/// Load tool duration priors from the snapshot.
pub fn load_tool_duration_priors_from_snapshot(
    snapshot: &AmsStore,
) -> BTreeMap<String, ToolDurationPrior> {
    let mut result = BTreeMap::new();

    let Some(container) = snapshot.containers().get(AGENT_TOOL_DURATION_CONTAINER) else {
        return result;
    };

    for (key, annotation) in &container.hypothesis_state {
        if let Some(tool_name) = key.strip_prefix("fep:tool-duration:") {
            if let Ok(prior) = serde_json::from_str::<ToolDurationPrior>(&annotation.value) {
                result.insert(tool_name.to_string(), prior);
            }
        }
    }

    result
}

/// Detect tool calls whose duration is surprisingly high (high free energy)
/// relative to the bootstrapped duration priors.
///
/// Only tool-call objects created after `since` and with a recorded `duration_s`
/// are evaluated. Returns entries sorted by free energy descending.
pub fn detect_slow_tools(
    snapshot: &AmsStore,
    priors: &BTreeMap<String, ToolDurationPrior>,
    since: chrono::DateTime<chrono::FixedOffset>,
    threshold: f64,
) -> Vec<SlowToolEntry> {
    let mut entries: Vec<SlowToolEntry> = Vec::new();

    for (id, obj) in snapshot.objects() {
        if obj.object_kind != "tool-call" {
            continue;
        }

        // Time-filter: only consider recent calls
        if obj.created_at <= since {
            continue;
        }

        let prov = obj
            .semantic_payload
            .as_ref()
            .and_then(|sp| sp.provenance.as_ref());
        let Some(prov) = prov else { continue };

        let tool_name = prov
            .get("tool_name")
            .and_then(|v| v.as_str())
            .unwrap_or("unknown");
        let Some(duration_s) = prov.get("duration_s").and_then(|v| v.as_f64()) else {
            continue;
        };

        let Some(prior) = priors.get(tool_name) else {
            continue;
        };

        let fe = compute_duration_free_energy(prior, duration_s);
        if fe >= threshold {
            entries.push(SlowToolEntry {
                tool_name: tool_name.to_string(),
                tool_use_id: id.clone(),
                observed_s: duration_s,
                free_energy: fe,
                prior_mean_s: prior.mean_s,
                prior_variance_s: prior.variance_s,
                prior_count: prior.count,
            });
        }
    }

    entries.sort_by(|a, b| b.free_energy.partial_cmp(&a.free_energy).unwrap_or(std::cmp::Ordering::Equal));
    entries
}

/// Apply precision decay to all agent tool priors.
///
/// Moves each outcome parameter's precision (1/variance) toward
/// `DEFAULT_TOOL_PRIOR_PRECISION` at the given `decay_rate`. This increases
/// variance (uncertainty) on stale priors so that new anomalies are not
/// suppressed by high confidence from old data.
///
/// Returns the number of parameters decayed.
pub fn decay_agent_tool_priors(
    priors: &mut BTreeMap<String, ToolOutcomeDistribution>,
    decay_rate: f64,
) -> usize {
    let mut count = 0;
    for distribution in priors.values_mut() {
        for param in distribution.outcome_params.values_mut() {
            let current_precision = 1.0 / param.variance.max(0.01);
            let new_precision = decay_precision(
                current_precision,
                DEFAULT_TOOL_PRIOR_PRECISION,
                decay_rate,
            );
            param.variance = (1.0 / new_precision.max(0.01)).max(0.01);
            count += 1;
        }
    }
    count
}

/// Run the full FEP bootstrap pipeline: compute relevance priors from episodes
/// and write them into the snapshot.
pub fn run_fep_bootstrap(
    snapshot: &mut AmsStore,
    episodes: &[RouteReplayEpisodeEntry],
) -> FepBootstrapReport {
    let priors = bootstrap_relevance_priors(episodes);
    let containers_with_priors = priors.len();
    let total_prior_keys = priors.values().map(|m| m.len()).sum::<usize>();
    let keys_written = write_relevance_priors_to_snapshot(snapshot, &priors);

    // Tool outcome priors (from route episodes)
    let tool_outcome_priors = bootstrap_tool_outcome_priors(episodes);
    let tool_outcome_contexts = tool_outcome_priors.len();
    let tool_outcome_keys_written =
        write_tool_outcome_priors_to_snapshot(snapshot, &tool_outcome_priors);

    // Agent tool-call priors (from snapshot objects)
    let agent_tool_priors = bootstrap_agent_tool_priors(snapshot);
    let agent_tool_contexts = agent_tool_priors.len();
    let agent_tool_keys_written =
        write_agent_tool_priors_to_snapshot(snapshot, &agent_tool_priors);

    // Tool duration priors (from snapshot tool-call objects with duration_s)
    let duration_priors = bootstrap_tool_duration_priors(snapshot);
    let duration_tools = duration_priors.len();
    let duration_keys_written = write_tool_duration_priors_to_snapshot(snapshot, &duration_priors);

    FepBootstrapReport {
        episodes_processed: episodes.len(),
        containers_with_priors,
        total_prior_keys,
        keys_written,
        tool_outcome_contexts,
        tool_outcome_keys_written,
        agent_tool_contexts,
        agent_tool_keys_written,
        duration_tools,
        duration_keys_written,
    }
}

/// Summary report from the bootstrap pipeline.
#[derive(Clone, Debug)]
pub struct FepBootstrapReport {
    pub episodes_processed: usize,
    pub containers_with_priors: usize,
    pub total_prior_keys: usize,
    pub keys_written: usize,
    pub tool_outcome_contexts: usize,
    pub tool_outcome_keys_written: usize,
    pub agent_tool_contexts: usize,
    pub agent_tool_keys_written: usize,
    pub duration_tools: usize,
    pub duration_keys_written: usize,
}

impl std::fmt::Display for FepBootstrapReport {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        writeln!(f, "FEP Bootstrap Report:")?;
        writeln!(f, "  episodes processed:        {}", self.episodes_processed)?;
        writeln!(f, "  containers with priors:    {}", self.containers_with_priors)?;
        writeln!(f, "  total prior keys:          {}", self.total_prior_keys)?;
        writeln!(f, "  keys written to store:     {}", self.keys_written)?;
        writeln!(f, "  tool outcome contexts:     {}", self.tool_outcome_contexts)?;
        writeln!(f, "  tool outcome keys written: {}", self.tool_outcome_keys_written)?;
        writeln!(f, "  agent tool contexts:       {}", self.agent_tool_contexts)?;
        writeln!(f, "  agent tool keys written:   {}", self.agent_tool_keys_written)?;
        writeln!(f, "  duration tools:            {}", self.duration_tools)?;
        writeln!(f, "  duration keys written:     {}", self.duration_keys_written)
    }
}

// ---------------------------------------------------------------------------
// RLHF Feedback Classification
// ---------------------------------------------------------------------------

/// User feedback signal types.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum UserFeedbackSignal {
    Positive,
    Negative,
    Neutral,
}

/// Classify a user message as positive, negative, or neutral feedback.
///
/// Uses pattern matching on common feedback phrases. This is intentionally
/// simple — a more sophisticated version could use embeddings or LLM classification.
pub fn classify_user_feedback(message: &str) -> UserFeedbackSignal {
    let lower = message.to_ascii_lowercase();

    // Check negative patterns first (more specific, should take priority)
    const NEGATIVE_PATTERNS: &[&str] = &[
        "not what i meant",
        "that's not right",
        "that's wrong",
        "thats wrong",
        "thats not right",
        "wrong answer",
        "wrong result",
        "incorrect",
        "not helpful",
        "not useful",
        "no that's not",
        "no, that's not",
        "no thats not",
        "not relevant",
        "off topic",
        "off-topic",
        "try again",
        "that's not it",
        "thats not it",
        "not what i asked",
        "not what i wanted",
        "no, wrong",
    ];

    for pattern in NEGATIVE_PATTERNS {
        if lower.contains(pattern) {
            return UserFeedbackSignal::Negative;
        }
    }

    // Simple negative signals (must be near start of message or standalone)
    if lower.starts_with("no,") || lower.starts_with("no ") || lower == "no" || lower.starts_with("wrong") {
        return UserFeedbackSignal::Negative;
    }

    // Positive patterns
    const POSITIVE_PATTERNS: &[&str] = &[
        "good job",
        "that's right",
        "thats right",
        "that's correct",
        "thats correct",
        "exactly",
        "perfect",
        "great work",
        "well done",
        "nice work",
        "that's helpful",
        "thats helpful",
        "very helpful",
        "exactly what i",
        "that's what i needed",
        "thats what i needed",
        "looks good",
        "looks great",
        "looks correct",
        "thank you",
        "thanks",
        "yes, that's",
        "yes that's",
        "yes thats",
        "correct",
    ];

    for pattern in POSITIVE_PATTERNS {
        if lower.contains(pattern) {
            return UserFeedbackSignal::Positive;
        }
    }

    // Simple positive signals
    if lower.starts_with("yes,") || lower.starts_with("yes ") || lower == "yes" || lower == "y" {
        return UserFeedbackSignal::Positive;
    }

    UserFeedbackSignal::Neutral
}

/// Batch classify feedback from session message pairs.
///
/// Given a sequence of (role, message) pairs from a session transcript,
/// identifies RLHF feedback signals from user messages that follow assistant
/// messages. Returns (message_index, signal) pairs.
pub fn classify_session_feedback(messages: &[(String, String)]) -> Vec<(usize, UserFeedbackSignal)> {
    let mut results = Vec::new();
    let mut prev_was_assistant = false;

    for (index, (role, message)) in messages.iter().enumerate() {
        if role == "user" && prev_was_assistant {
            let signal = classify_user_feedback(message);
            if signal != UserFeedbackSignal::Neutral {
                results.push((index, signal));
            }
        }
        prev_was_assistant = role == "assistant";
    }

    results
}

/// Look up a relevance prior for a specific container and context kind from the snapshot.
pub fn get_relevance_prior(
    snapshot: &AmsStore,
    container_id: &str,
    scope_lens: &str,
    agent_role: &str,
) -> Option<GaussianParam> {
    let container = snapshot.containers().get(container_id)?;
    let key = format!("fep:prior:relevance:{}:{}", scope_lens, agent_role);
    let annotation = container.hypothesis_state.get(&key)?;
    serde_json::from_str(&annotation.value).ok()
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::route_memory::{
        RouteReplayEpisodeEntry, RouteReplayEpisodeInput, RouteReplayFrameInput,
        RouteReplayRouteInput,
    };
    use chrono::DateTime;

    fn make_episode(
        winning: &str,
        candidates: Vec<&str>,
        scope_lens: &str,
        agent_role: &str,
    ) -> RouteReplayEpisodeEntry {
        RouteReplayEpisodeEntry {
            frame: RouteReplayFrameInput {
                scope_lens: scope_lens.to_string(),
                agent_role: agent_role.to_string(),
                mode: "build".to_string(),
                lineage_node_ids: vec!["node-1".to_string()],
                artifact_refs: None,
                failure_bucket: None,
            },
            route: RouteReplayRouteInput {
                ranking_source: "raw-lesson".to_string(),
                path: "test-path".to_string(),
                cost: 0.5,
                risk_flags: None,
            },
            episode: RouteReplayEpisodeInput {
                query_text: "test query".to_string(),
                occurred_at: DateTime::parse_from_rfc3339("2026-03-10T08:00:00+00:00").unwrap(),
                weak_result: false,
                used_fallback: false,
                winning_target_ref: winning.to_string(),
                top_target_refs: vec![winning.to_string()],
                user_feedback: None,
                tool_outcome: None,
            },
            candidate_target_refs: candidates.into_iter().map(String::from).collect(),
            winning_target_ref: winning.to_string(),
        }
    }

    #[test]
    fn bootstrap_relevance_priors_computes_win_rates() {
        let card_a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1";
        let card_b = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2";

        let episodes = vec![
            make_episode(card_a, vec![card_a, card_b], "local-first-lineage", "implementer"),
            make_episode(card_a, vec![card_a, card_b], "local-first-lineage", "implementer"),
            make_episode(card_b, vec![card_a, card_b], "local-first-lineage", "implementer"),
        ];

        let priors = bootstrap_relevance_priors(&episodes);
        let container_a = format!("chat-session:{}", card_a);
        let container_b = format!("chat-session:{}", card_b);

        // card_a won 2/3 appearances
        let a_prior = &priors[&container_a]["local-first-lineage:implementer"];
        assert!((a_prior.mean - 2.0 / 3.0).abs() < 0.01);

        // card_b won 1/3 appearances
        let b_prior = &priors[&container_b]["local-first-lineage:implementer"];
        assert!((b_prior.mean - 1.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn bootstrap_variance_decreases_with_more_observations() {
        let card_a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1";
        let few_episodes = vec![
            make_episode(card_a, vec![card_a], "lineage", "impl"),
        ];
        let many_episodes: Vec<_> = (0..10)
            .map(|_| make_episode(card_a, vec![card_a], "lineage", "impl"))
            .collect();

        let few_priors = bootstrap_relevance_priors(&few_episodes);
        let many_priors = bootstrap_relevance_priors(&many_episodes);

        let few_var = few_priors[&format!("chat-session:{}", card_a)]["lineage:impl"].variance;
        let many_var = many_priors[&format!("chat-session:{}", card_a)]["lineage:impl"].variance;
        assert!(many_var < few_var);
    }

    #[test]
    fn write_priors_to_snapshot_creates_hypothesis_state_keys() {
        let mut snapshot = AmsStore::new();
        snapshot
            .create_container("chat-session:test".to_string(), "chat_session", "chat_session")
            .unwrap();

        let mut priors = BTreeMap::new();
        let mut ctx_priors = BTreeMap::new();
        ctx_priors.insert(
            "lineage:impl".to_string(),
            GaussianParam { mean: 0.8, variance: 0.1 },
        );
        priors.insert("chat-session:test".to_string(), ctx_priors);

        let written = write_relevance_priors_to_snapshot(&mut snapshot, &priors);
        assert_eq!(written, 1);

        let container = snapshot.containers().get("chat-session:test").unwrap();
        assert!(container
            .hypothesis_state
            .contains_key("fep:prior:relevance:lineage:impl"));
    }

    #[test]
    fn classify_user_feedback_detects_positive() {
        assert_eq!(classify_user_feedback("Good job, that's exactly what I needed"), UserFeedbackSignal::Positive);
        assert_eq!(classify_user_feedback("yes"), UserFeedbackSignal::Positive);
        assert_eq!(classify_user_feedback("That's correct"), UserFeedbackSignal::Positive);
        assert_eq!(classify_user_feedback("thanks"), UserFeedbackSignal::Positive);
    }

    #[test]
    fn classify_user_feedback_detects_negative() {
        assert_eq!(classify_user_feedback("Not what I meant"), UserFeedbackSignal::Negative);
        assert_eq!(classify_user_feedback("no, that's not right"), UserFeedbackSignal::Negative);
        assert_eq!(classify_user_feedback("wrong"), UserFeedbackSignal::Negative);
        assert_eq!(classify_user_feedback("That's wrong"), UserFeedbackSignal::Negative);
    }

    #[test]
    fn classify_user_feedback_detects_neutral() {
        assert_eq!(classify_user_feedback("Can you also add logging?"), UserFeedbackSignal::Neutral);
        assert_eq!(classify_user_feedback("What about the other file?"), UserFeedbackSignal::Neutral);
    }

    #[test]
    fn classify_session_feedback_finds_feedback_after_assistant() {
        let messages = vec![
            ("assistant".to_string(), "Here's the result".to_string()),
            ("user".to_string(), "Good job, perfect".to_string()),
            ("assistant".to_string(), "Updated the file".to_string()),
            ("user".to_string(), "Not what I meant".to_string()),
            ("user".to_string(), "Can you fix it?".to_string()), // no preceding assistant
        ];

        let feedback = classify_session_feedback(&messages);
        assert_eq!(feedback.len(), 2);
        assert_eq!(feedback[0], (1, UserFeedbackSignal::Positive));
        assert_eq!(feedback[1], (3, UserFeedbackSignal::Negative));
    }

    #[test]
    fn run_fep_bootstrap_end_to_end() {
        let card_a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1";
        let mut snapshot = AmsStore::new();
        snapshot
            .create_container(
                format!("chat-session:{}", card_a),
                "chat_session",
                "chat_session",
            )
            .unwrap();

        let episodes = vec![
            make_episode(card_a, vec![card_a], "lineage", "impl"),
            make_episode(card_a, vec![card_a], "lineage", "impl"),
        ];

        let report = run_fep_bootstrap(&mut snapshot, &episodes);
        assert_eq!(report.episodes_processed, 2);
        assert_eq!(report.containers_with_priors, 1);
        assert!(report.keys_written > 0);

        // Verify hypothesis_state was written
        let container = snapshot
            .containers()
            .get(&format!("chat-session:{}", card_a))
            .unwrap();
        assert!(container
            .hypothesis_state
            .keys()
            .any(|k| k.starts_with("fep:prior:relevance:")));
    }

    #[test]
    fn bootstrap_agent_tool_priors_from_snapshot_objects() {
        use crate::model::{ObjectRecord, SemanticPayload};
        use serde_json::json;

        let mut snapshot = AmsStore::new();

        // Create tool-call objects with provenance
        let make_tool_obj = |id: &str, tool: &str, is_error: bool, preview: &str| {
            let mut prov = BTreeMap::new();
            prov.insert("tool_name".to_string(), json!(tool));
            prov.insert("is_error".to_string(), json!(is_error));
            prov.insert("result_preview".to_string(), json!(preview));
            ObjectRecord::new(
                id.to_string(),
                "tool-call".to_string(),
                None,
                Some(SemanticPayload {
                    embedding: None,
                    tags: None,
                    summary: None,
                    provenance: Some(prov),
                }),
                None,
            )
        };

        snapshot.insert_object_record(make_tool_obj("tc-1", "Bash", false, "Compiled OK"));
        snapshot.insert_object_record(make_tool_obj("tc-2", "Bash", true, "exit code 1"));
        snapshot.insert_object_record(make_tool_obj("tc-3", "Bash", false, "Compiled OK"));
        snapshot.insert_object_record(make_tool_obj("tc-4", "Grep", false, "No matches"));
        snapshot.insert_object_record(make_tool_obj("tc-5", "Grep", false, "src/main.rs:42"));
        // Non-tool-call object should be ignored
        snapshot.insert_object_record(ObjectRecord::new(
            "other-1".to_string(),
            "chat-message".to_string(),
            None,
            None,
            None,
        ));

        let priors = bootstrap_agent_tool_priors(&snapshot);

        // Should have priors for Bash and Grep
        assert_eq!(priors.len(), 2);

        let bash = &priors["Bash"];
        assert_eq!(bash.total_observations, 3);
        // 2 success, 1 error out of 3
        let bash_success = bash.outcome_params[&ToolOutcome::Success].mean;
        assert!((bash_success - 2.0 / 3.0).abs() < 0.01);
        let bash_error = bash.outcome_params[&ToolOutcome::Error].mean;
        assert!((bash_error - 1.0 / 3.0).abs() < 0.01);

        let grep = &priors["Grep"];
        assert_eq!(grep.total_observations, 2);
        // 1 success, 1 null out of 2
        let grep_success = grep.outcome_params[&ToolOutcome::Success].mean;
        assert!((grep_success - 0.5).abs() < 0.01);
        let grep_null = grep.outcome_params[&ToolOutcome::Null].mean;
        assert!((grep_null - 0.5).abs() < 0.01);
    }

    #[test]
    fn agent_tool_priors_snapshot_roundtrip() {
        use crate::model::{ObjectRecord, SemanticPayload};
        use serde_json::json;

        let mut snapshot = AmsStore::new();

        let mut prov = BTreeMap::new();
        prov.insert("tool_name".to_string(), json!("Read"));
        prov.insert("is_error".to_string(), json!(false));
        prov.insert("result_preview".to_string(), json!("fn main() {}"));
        snapshot.insert_object_record(ObjectRecord::new(
            "tc-1".to_string(),
            "tool-call".to_string(),
            None,
            Some(SemanticPayload {
                embedding: None,
                tags: None,
                summary: None,
                provenance: Some(prov),
            }),
            None,
        ));

        let priors = bootstrap_agent_tool_priors(&snapshot);
        let written = write_agent_tool_priors_to_snapshot(&mut snapshot, &priors);
        assert_eq!(written, 1);

        let loaded = load_agent_tool_priors_from_snapshot(&snapshot);
        assert_eq!(loaded.len(), 1);
        assert_eq!(loaded["Read"].total_observations, 1);
        assert_eq!(
            loaded["Read"].outcome_params[&ToolOutcome::Success].mean,
            priors["Read"].outcome_params[&ToolOutcome::Success].mean,
        );
    }

    #[test]
    fn get_relevance_prior_reads_back_written_prior() {
        let card_a = "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1";
        let mut snapshot = AmsStore::new();
        snapshot
            .create_container(
                format!("chat-session:{}", card_a),
                "chat_session",
                "chat_session",
            )
            .unwrap();

        let episodes = vec![
            make_episode(card_a, vec![card_a], "local-first-lineage", "implementer"),
        ];
        run_fep_bootstrap(&mut snapshot, &episodes);

        let prior = get_relevance_prior(
            &snapshot,
            &format!("chat-session:{}", card_a),
            "local-first-lineage",
            "implementer",
        );
        assert!(prior.is_some());
        let param = prior.unwrap();
        assert!((param.mean - 1.0).abs() < 0.01); // 1 win / 1 appearance
    }

    #[test]
    fn agent_tool_free_energy_higher_for_unexpected_failure() {
        use crate::model::{ObjectRecord, SemanticPayload};
        use crate::route_memory::ToolOutcome;
        use crate::tool_outcome::compute_tool_outcome_free_energy;
        use serde_json::json;

        let mut snapshot = AmsStore::new();

        // Build a tool with 95% success rate: 19 successes, 1 error
        for i in 0..19 {
            let mut prov = BTreeMap::new();
            prov.insert("tool_name".to_string(), json!("Bash"));
            prov.insert("is_error".to_string(), json!(false));
            prov.insert("result_preview".to_string(), json!("OK"));
            snapshot.insert_object_record(ObjectRecord::new(
                format!("tc-s-{}", i),
                "tool-call".to_string(),
                None,
                Some(SemanticPayload {
                    embedding: None,
                    tags: None,
                    summary: None,
                    provenance: Some(prov),
                }),
                None,
            ));
        }
        {
            let mut prov = BTreeMap::new();
            prov.insert("tool_name".to_string(), json!("Bash"));
            prov.insert("is_error".to_string(), json!(true));
            prov.insert("result_preview".to_string(), json!("fail"));
            snapshot.insert_object_record(ObjectRecord::new(
                "tc-e-0".to_string(),
                "tool-call".to_string(),
                None,
                Some(SemanticPayload {
                    embedding: None,
                    tags: None,
                    summary: None,
                    provenance: Some(prov),
                }),
                None,
            ));
        }

        let priors = bootstrap_agent_tool_priors(&snapshot);
        let bash_dist = &priors["Bash"];

        assert_eq!(bash_dist.total_observations, 20);

        // Free energy for Error should be much higher than for Success
        let fe_success = compute_tool_outcome_free_energy(bash_dist, ToolOutcome::Success);
        let fe_error = compute_tool_outcome_free_energy(bash_dist, ToolOutcome::Error);
        assert!(
            fe_error > fe_success,
            "FE(Error)={} should be > FE(Success)={}",
            fe_error,
            fe_success,
        );

        // And Null (never seen) should also have high FE
        let fe_null = compute_tool_outcome_free_energy(bash_dist, ToolOutcome::Null);
        assert!(
            fe_null > fe_success,
            "FE(Null)={} should be > FE(Success)={}",
            fe_null,
            fe_success,
        );
    }

    #[test]
    fn bootstrap_agent_tool_priors_skips_missing_provenance() {
        use crate::model::ObjectRecord;

        let mut snapshot = AmsStore::new();

        // tool-call with no semantic_payload
        snapshot.insert_object_record(ObjectRecord::new(
            "tc-no-payload".to_string(),
            "tool-call".to_string(),
            None,
            None,
            None,
        ));

        let priors = bootstrap_agent_tool_priors(&snapshot);
        assert!(priors.is_empty(), "Objects without provenance should be skipped");
    }
}
