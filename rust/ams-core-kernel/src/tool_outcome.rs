use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};

use crate::active_inference::GaussianParam;
use crate::model::{now_fixed, HypothesisAnnotation};
use crate::route_memory::{classify_tool_outcome, RouteReplayEpisodeEntry, ToolOutcome};
use crate::store::AmsStore;

// ---------------------------------------------------------------------------
// Agent Tool-Call Classification
// ---------------------------------------------------------------------------

/// Null-pattern strings that indicate a tool returned no meaningful result.
const NULL_PATTERNS: &[&str] = &[
    "no files found",
    "no matches",
    "0 results",
    "no results",
    "not found",
    "no such file",
    "empty",
];

/// Classify an agent tool-call record into a [`ToolOutcome`].
///
/// This is the entry-point for the FEP agent-tool-outcome pipeline (phase 2a).
/// It takes raw provenance fields from a tool-call event and maps them to the
/// existing `ToolOutcome` enum so that the same FEP prior / belief-update
/// machinery can be reused.
///
/// The `tool_name` parameter (e.g. `"Bash"`, `"Read"`, `"Grep"`) serves as the
/// context key when building per-tool priors.
///
/// # Classification rules
///
/// | Condition | Outcome |
/// |-----------|---------|
/// | `is_error == true` | `Error` |
/// | `result_preview` empty or matches a null pattern | `Null` |
/// | Otherwise | `Success` |
pub fn classify_agent_tool_outcome(
    is_error: bool,
    result_preview: &str,
    _tool_name: &str,
) -> ToolOutcome {
    if is_error {
        return ToolOutcome::Error;
    }

    let trimmed = result_preview.trim();
    if trimmed.is_empty() {
        return ToolOutcome::Null;
    }

    let lower = trimmed.to_ascii_lowercase();
    for pattern in NULL_PATTERNS {
        if lower.contains(pattern) {
            return ToolOutcome::Null;
        }
    }

    ToolOutcome::Success
}

// ---------------------------------------------------------------------------
// Tool Outcome Distribution
// ---------------------------------------------------------------------------

/// Per-context distribution over tool outcome categories.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolOutcomeDistribution {
    pub context_key: String,
    pub outcome_params: BTreeMap<ToolOutcome, GaussianParam>,
    pub total_observations: usize,
}

/// Prediction result for a given context.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct ToolOutcomePrediction {
    pub context_key: String,
    pub most_likely: ToolOutcome,
    pub success_probability: f64,
    pub outcome_probabilities: BTreeMap<ToolOutcome, f64>,
}

const ALL_OUTCOMES: [ToolOutcome; 5] = [
    ToolOutcome::Success,
    ToolOutcome::Weak,
    ToolOutcome::Null,
    ToolOutcome::Error,
    ToolOutcome::Wasteful,
];

/// Build a context key from scope_lens and agent_role.
fn context_key(scope_lens: &str, agent_role: &str) -> String {
    format!("{}:{}", scope_lens, agent_role)
}

// ---------------------------------------------------------------------------
// Bootstrap
// ---------------------------------------------------------------------------

/// Bootstrap tool outcome priors from route episode history.
///
/// For each (scope_lens, agent_role) context, counts occurrences of each
/// outcome category and converts to probability distributions with variance
/// inversely proportional to observation count.
pub fn bootstrap_tool_outcome_priors(
    episodes: &[RouteReplayEpisodeEntry],
) -> BTreeMap<String, ToolOutcomeDistribution> {
    // context_key -> outcome -> count
    let mut counts: BTreeMap<String, BTreeMap<ToolOutcome, usize>> = BTreeMap::new();
    let mut totals: BTreeMap<String, usize> = BTreeMap::new();

    for entry in episodes {
        let ctx = context_key(&entry.frame.scope_lens, &entry.frame.agent_role);
        let outcome = entry
            .episode
            .tool_outcome
            .unwrap_or_else(|| classify_tool_outcome(entry));
        *counts.entry(ctx.clone()).or_default().entry(outcome).or_default() += 1;
        *totals.entry(ctx).or_default() += 1;
    }

    let mut result = BTreeMap::new();
    for (ctx, outcome_counts) in counts {
        let total = totals[&ctx];
        let mut outcome_params = BTreeMap::new();
        for outcome in &ALL_OUTCOMES {
            let count = outcome_counts.get(outcome).copied().unwrap_or(0);
            let mean = if total > 0 {
                count as f64 / total as f64
            } else {
                1.0 / ALL_OUTCOMES.len() as f64
            };
            // Variance decreases with more observations (min 0.01)
            let variance = if total > 0 {
                (1.0 / (total as f64 + 1.0)).max(0.01)
            } else {
                1.0
            };
            outcome_params.insert(*outcome, GaussianParam { mean, variance });
        }
        result.insert(
            ctx.clone(),
            ToolOutcomeDistribution {
                context_key: ctx,
                outcome_params,
                total_observations: total,
            },
        );
    }

    result
}

// ---------------------------------------------------------------------------
// Free Energy
// ---------------------------------------------------------------------------

/// Compute the free energy (surprise) of an observed outcome given the distribution.
///
/// Uses KL-like surprise: -ln(P(observed)). Higher values = more surprising.
pub fn compute_tool_outcome_free_energy(
    distribution: &ToolOutcomeDistribution,
    observed: ToolOutcome,
) -> f64 {
    let param = distribution
        .outcome_params
        .get(&observed)
        .cloned()
        .unwrap_or(GaussianParam {
            mean: 1.0 / ALL_OUTCOMES.len() as f64,
            variance: 1.0,
        });

    // Surprise = -ln(mean), clamped to avoid infinity
    let p = param.mean.max(0.001);
    -p.ln()
}

// ---------------------------------------------------------------------------
// Belief Update
// ---------------------------------------------------------------------------

/// Bayesian posterior update on all outcome classes given an observation.
///
/// The observed outcome's mean is increased; all others are decreased.
/// Precision (1/variance) controls the learning rate.
pub fn update_tool_outcome_beliefs(
    distribution: &mut ToolOutcomeDistribution,
    observed: ToolOutcome,
    precision: f64,
) {
    let learning_rate = (precision / (precision + distribution.total_observations as f64 + 1.0))
        .clamp(0.01, 0.5);

    for outcome in &ALL_OUTCOMES {
        let param = distribution
            .outcome_params
            .entry(*outcome)
            .or_insert(GaussianParam {
                mean: 1.0 / ALL_OUTCOMES.len() as f64,
                variance: 1.0,
            });

        if *outcome == observed {
            param.mean += learning_rate * (1.0 - param.mean);
        } else {
            param.mean -= learning_rate * param.mean;
        }
        param.mean = param.mean.clamp(0.001, 0.999);

        // Reduce variance with each observation
        param.variance = (param.variance * (1.0 - learning_rate * 0.5)).max(0.01);
    }

    distribution.total_observations += 1;
}

// ---------------------------------------------------------------------------
// Prediction
// ---------------------------------------------------------------------------

/// Predict the most likely tool outcome for a given context.
pub fn predict_tool_outcome(
    priors: &BTreeMap<String, ToolOutcomeDistribution>,
    scope_lens: &str,
    agent_role: &str,
) -> Option<ToolOutcomePrediction> {
    let ctx = context_key(scope_lens, agent_role);
    let distribution = priors.get(&ctx)?;

    let mut probabilities = BTreeMap::new();
    let mut best_outcome = ToolOutcome::Success;
    let mut best_prob = 0.0;
    let mut sum = 0.0;

    for outcome in &ALL_OUTCOMES {
        let p = distribution
            .outcome_params
            .get(outcome)
            .map(|param| param.mean)
            .unwrap_or(1.0 / ALL_OUTCOMES.len() as f64);
        probabilities.insert(*outcome, p);
        sum += p;
        if p > best_prob {
            best_prob = p;
            best_outcome = *outcome;
        }
    }

    // Normalize
    if sum > 0.0 {
        for p in probabilities.values_mut() {
            *p /= sum;
        }
    }

    let success_probability = probabilities
        .get(&ToolOutcome::Success)
        .copied()
        .unwrap_or(0.0);

    Some(ToolOutcomePrediction {
        context_key: ctx,
        most_likely: best_outcome,
        success_probability,
        outcome_probabilities: probabilities,
    })
}

// ---------------------------------------------------------------------------
// Persistence
// ---------------------------------------------------------------------------

const TOOL_OUTCOME_CONTAINER: &str = "fep:tool-outcome-priors";

/// Write tool outcome priors into an AMS snapshot as hypothesis_state entries.
pub fn write_tool_outcome_priors_to_snapshot(
    snapshot: &mut AmsStore,
    priors: &BTreeMap<String, ToolOutcomeDistribution>,
) -> usize {
    // Ensure the container exists
    if snapshot.containers().get(TOOL_OUTCOME_CONTAINER).is_none() {
        let _ = snapshot.create_container(
            TOOL_OUTCOME_CONTAINER,
            "fep_priors",
            "fep_priors",
        );
    }

    let now = now_fixed();
    let mut total_written = 0;

    let Some(container) = snapshot.containers_mut().get_mut(TOOL_OUTCOME_CONTAINER) else {
        return 0;
    };

    for (ctx_key, distribution) in priors {
        let key = format!("fep:tool-outcome:{}", ctx_key);
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

/// Load tool outcome priors from an AMS snapshot.
pub fn load_tool_outcome_priors_from_snapshot(
    snapshot: &AmsStore,
) -> BTreeMap<String, ToolOutcomeDistribution> {
    let mut result = BTreeMap::new();

    let Some(container) = snapshot.containers().get(TOOL_OUTCOME_CONTAINER) else {
        return result;
    };

    for (key, annotation) in &container.hypothesis_state {
        if let Some(ctx_key) = key.strip_prefix("fep:tool-outcome:") {
            if let Ok(distribution) =
                serde_json::from_str::<ToolOutcomeDistribution>(&annotation.value)
            {
                result.insert(ctx_key.to_string(), distribution);
            }
        }
    }

    result
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

    fn make_episode_with_outcome(
        scope_lens: &str,
        agent_role: &str,
        weak: bool,
        fallback: bool,
        cost: f64,
        risk_flags: Vec<String>,
        candidates: usize,
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
                cost,
                risk_flags: Some(risk_flags).filter(|f| !f.is_empty()),
            },
            episode: RouteReplayEpisodeInput {
                query_text: "test query".to_string(),
                occurred_at: DateTime::parse_from_rfc3339("2026-03-10T08:00:00+00:00").unwrap(),
                weak_result: weak,
                used_fallback: fallback,
                winning_target_ref: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1".to_string(),
                top_target_refs: vec!["aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1".to_string()],
                user_feedback: None,
                tool_outcome: None,
            },
            candidate_target_refs: (0..candidates)
                .map(|i| format!("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaa{:03}", i))
                .collect(),
            winning_target_ref: "aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1".to_string(),
        }
    }

    #[test]
    fn bootstrap_computes_outcome_rates() {
        let episodes = vec![
            make_episode_with_outcome("lineage", "impl", false, false, 0.5, vec![], 1),
            make_episode_with_outcome("lineage", "impl", false, false, 0.5, vec![], 1),
            make_episode_with_outcome("lineage", "impl", true, false, 0.5, vec![], 0),
        ];

        let priors = bootstrap_tool_outcome_priors(&episodes);
        assert_eq!(priors.len(), 1);

        let dist = &priors["lineage:impl"];
        assert_eq!(dist.total_observations, 3);

        // 2 successes out of 3
        let success_mean = dist.outcome_params[&ToolOutcome::Success].mean;
        assert!((success_mean - 2.0 / 3.0).abs() < 0.01);

        // 1 null out of 3
        let null_mean = dist.outcome_params[&ToolOutcome::Null].mean;
        assert!((null_mean - 1.0 / 3.0).abs() < 0.01);
    }

    #[test]
    fn variance_decreases_with_observations() {
        let few = vec![make_episode_with_outcome(
            "lineage", "impl", false, false, 0.5, vec![], 1,
        )];
        let many: Vec<_> = (0..10)
            .map(|_| make_episode_with_outcome("lineage", "impl", false, false, 0.5, vec![], 1))
            .collect();

        let few_priors = bootstrap_tool_outcome_priors(&few);
        let many_priors = bootstrap_tool_outcome_priors(&many);

        let few_var = few_priors["lineage:impl"].outcome_params[&ToolOutcome::Success].variance;
        let many_var = many_priors["lineage:impl"].outcome_params[&ToolOutcome::Success].variance;
        assert!(many_var < few_var);
    }

    #[test]
    fn free_energy_higher_for_surprising_outcome() {
        let episodes = vec![
            make_episode_with_outcome("lineage", "impl", false, false, 0.5, vec![], 1),
            make_episode_with_outcome("lineage", "impl", false, false, 0.5, vec![], 1),
            make_episode_with_outcome("lineage", "impl", false, false, 0.5, vec![], 1),
        ];
        let priors = bootstrap_tool_outcome_priors(&episodes);
        let dist = &priors["lineage:impl"];

        let fe_success = compute_tool_outcome_free_energy(dist, ToolOutcome::Success);
        let fe_error = compute_tool_outcome_free_energy(dist, ToolOutcome::Error);

        // Error is surprising given all-success history
        assert!(fe_error > fe_success);
    }

    #[test]
    fn belief_update_shifts_toward_observation() {
        let episodes = vec![
            make_episode_with_outcome("lineage", "impl", false, false, 0.5, vec![], 1),
            make_episode_with_outcome("lineage", "impl", false, false, 0.5, vec![], 1),
        ];
        let priors = bootstrap_tool_outcome_priors(&episodes);
        let mut dist = priors["lineage:impl"].clone();

        let before_error = dist.outcome_params[&ToolOutcome::Error].mean;
        update_tool_outcome_beliefs(&mut dist, ToolOutcome::Error, 1.0);
        let after_error = dist.outcome_params[&ToolOutcome::Error].mean;

        assert!(after_error > before_error);
        assert_eq!(dist.total_observations, 3);
    }

    #[test]
    fn prediction_returns_most_likely() {
        let episodes = vec![
            make_episode_with_outcome("lineage", "impl", false, false, 0.5, vec![], 1),
            make_episode_with_outcome("lineage", "impl", false, false, 0.5, vec![], 1),
            make_episode_with_outcome("lineage", "impl", true, false, 0.5, vec![], 0),
        ];
        let priors = bootstrap_tool_outcome_priors(&episodes);

        let prediction = predict_tool_outcome(&priors, "lineage", "impl").unwrap();
        assert_eq!(prediction.most_likely, ToolOutcome::Success);
        assert!(prediction.success_probability > 0.5);
    }

    #[test]
    fn prediction_returns_none_for_unknown_context() {
        let priors = BTreeMap::new();
        assert!(predict_tool_outcome(&priors, "unknown", "role").is_none());
    }

    #[test]
    fn classify_agent_tool_outcome_error() {
        assert_eq!(
            classify_agent_tool_outcome(true, "some output", "Bash"),
            ToolOutcome::Error,
        );
        assert_eq!(
            classify_agent_tool_outcome(true, "", "Read"),
            ToolOutcome::Error,
        );
    }

    #[test]
    fn classify_agent_tool_outcome_null() {
        assert_eq!(
            classify_agent_tool_outcome(false, "", "Grep"),
            ToolOutcome::Null,
        );
        assert_eq!(
            classify_agent_tool_outcome(false, "   ", "Grep"),
            ToolOutcome::Null,
        );
        assert_eq!(
            classify_agent_tool_outcome(false, "No files found", "Glob"),
            ToolOutcome::Null,
        );
        assert_eq!(
            classify_agent_tool_outcome(false, "No matches", "Grep"),
            ToolOutcome::Null,
        );
        assert_eq!(
            classify_agent_tool_outcome(false, "0 results returned", "Bash"),
            ToolOutcome::Null,
        );
    }

    #[test]
    fn classify_agent_tool_outcome_success() {
        assert_eq!(
            classify_agent_tool_outcome(false, "fn main() { }", "Read"),
            ToolOutcome::Success,
        );
        assert_eq!(
            classify_agent_tool_outcome(false, "Compiled successfully", "Bash"),
            ToolOutcome::Success,
        );
    }

    #[test]
    fn classify_agent_tool_outcome_null_case_insensitive() {
        // Null patterns should match case-insensitively
        assert_eq!(
            classify_agent_tool_outcome(false, "NO FILES FOUND", "Glob"),
            ToolOutcome::Null,
        );
        assert_eq!(
            classify_agent_tool_outcome(false, "No Matches here", "Grep"),
            ToolOutcome::Null,
        );
        assert_eq!(
            classify_agent_tool_outcome(false, "NOT FOUND in path", "Bash"),
            ToolOutcome::Null,
        );
    }

    #[test]
    fn classify_agent_tool_outcome_error_trumps_null_preview() {
        // is_error=true should always yield Error, even if preview matches null patterns
        assert_eq!(
            classify_agent_tool_outcome(true, "No files found", "Glob"),
            ToolOutcome::Error,
        );
        assert_eq!(
            classify_agent_tool_outcome(true, "", "Bash"),
            ToolOutcome::Error,
        );
    }

    #[test]
    fn classify_agent_tool_outcome_whitespace_only_is_null() {
        assert_eq!(
            classify_agent_tool_outcome(false, "   \t\n  ", "Bash"),
            ToolOutcome::Null,
        );
    }

    #[test]
    fn snapshot_roundtrip() {
        let episodes = vec![
            make_episode_with_outcome("lineage", "impl", false, false, 0.5, vec![], 1),
            make_episode_with_outcome("lineage", "impl", true, false, 0.5, vec![], 0),
        ];
        let priors = bootstrap_tool_outcome_priors(&episodes);

        let mut snapshot = AmsStore::new();
        let written = write_tool_outcome_priors_to_snapshot(&mut snapshot, &priors);
        assert_eq!(written, 1);

        let loaded = load_tool_outcome_priors_from_snapshot(&snapshot);
        assert_eq!(loaded.len(), 1);
        assert_eq!(
            loaded["lineage:impl"].total_observations,
            priors["lineage:impl"].total_observations
        );
    }
}
