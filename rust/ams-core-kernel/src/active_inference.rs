use std::collections::BTreeMap;

use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::{now_fixed, ContainerRecord, HypothesisAnnotation};
use crate::store::AmsStore;

// ---------------------------------------------------------------------------
// Phase 1: Generative Stereotype Priors
// ---------------------------------------------------------------------------

/// Parameters for a Gaussian (continuous) prior distribution.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct GaussianParam {
    pub mean: f64,
    pub variance: f64,
}

/// A prior distribution over a single attribute, either categorical or Gaussian.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
#[serde(tag = "kind", rename_all = "snake_case")]
pub enum AttributePrior {
    Categorical {
        counts: BTreeMap<String, usize>,
        total: usize,
    },
    Gaussian(GaussianParam),
}

/// Generative prior for a container, built from its stereotype edges and hypothesis state.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct StereotypePrior {
    pub container_id: String,
    pub attribute_priors: BTreeMap<String, AttributePrior>,
}

/// Extract a generative stereotype prior for a container.
///
/// Walks the container's `stereotype` field (from `ExpectationMetadata`) and its
/// `hypothesis_state` to build empirical distributions. If a stereotype link-node
/// is present, the linked container's hypothesis_state is also incorporated.
///
/// The computed prior is cached in `hypothesis_state["fep:prior"]` as JSON.
pub fn extract_stereotype_prior(snapshot: &AmsStore, container_id: &str) -> Option<StereotypePrior> {
    let container = snapshot.containers().get(container_id)?;
    let mut attribute_priors = BTreeMap::new();

    // 1. Build priors from stereotype metadata (categorical counts from JSON map values)
    if let Some(stereotype) = &container.expectation_metadata.stereotype {
        for (attr_key, attr_value) in stereotype {
            if let Some(prior) = prior_from_value(attr_key, attr_value) {
                attribute_priors.insert(attr_key.clone(), prior);
            }
        }
    }

    // 2. Incorporate hypothesis_state values as Gaussian observations
    for (key, annotation) in &container.hypothesis_state {
        if key.starts_with("fep:") {
            continue; // skip FEP internal keys
        }
        if let Ok(numeric) = annotation.value.parse::<f64>() {
            attribute_priors
                .entry(key.clone())
                .or_insert(AttributePrior::Gaussian(GaussianParam {
                    mean: numeric,
                    variance: 1.0, // unit variance default for single observation
                }));
        }
    }

    // 3. Walk stereotype link-node if present, merging linked container's hypothesis_state
    if let Some(ref linknode_id) = container.expectation_metadata.stereotype_linknode_id {
        if let Some(link_node) = snapshot.link_nodes().get(linknode_id) {
            let linked_container_id = &link_node.container_id;
            if linked_container_id != container_id {
                if let Some(linked_container) = snapshot.containers().get(linked_container_id) {
                    merge_hypothesis_priors(&mut attribute_priors, &linked_container.hypothesis_state);
                }
            }
        }
    }

    if attribute_priors.is_empty() {
        return None;
    }

    Some(StereotypePrior {
        container_id: container_id.to_string(),
        attribute_priors,
    })
}

/// Cache a computed prior into the container's hypothesis_state as `fep:prior`.
pub fn cache_prior(container: &mut ContainerRecord, prior: &StereotypePrior) {
    if let Ok(json_str) = serde_json::to_string(prior) {
        container.hypothesis_state.insert(
            "fep:prior".to_string(),
            HypothesisAnnotation {
                key: "fep:prior".to_string(),
                value: json_str,
                updated_at: now_fixed(),
            },
        );
    }
}

/// Load a cached prior from a container's hypothesis_state.
pub fn load_cached_prior(container: &ContainerRecord) -> Option<StereotypePrior> {
    let annotation = container.hypothesis_state.get("fep:prior")?;
    serde_json::from_str(&annotation.value).ok()
}

/// Return the expected value for a given attribute from the prior distribution.
pub fn predict(prior: &StereotypePrior, attribute: &str) -> Option<f64> {
    let attr_prior = prior.attribute_priors.get(attribute)?;
    match attr_prior {
        AttributePrior::Gaussian(param) => Some(param.mean),
        AttributePrior::Categorical { counts, total } => {
            if *total == 0 {
                return None;
            }
            // For categorical: return the probability of the most frequent category
            let max_count = counts.values().max().copied().unwrap_or(0);
            Some(max_count as f64 / *total as f64)
        }
    }
}

// ---------------------------------------------------------------------------
// Phase 2: Free Energy Scoring
// ---------------------------------------------------------------------------

/// Compute the variational free energy for a set of observations against a prior.
///
/// F = complexity - accuracy, where:
/// - accuracy = sum(obs_k * precision_k) for alignment dimensions
/// - complexity = sum_k((obs_k - prior_mean_k)^2 / (2 * prior_var_k))
///
/// Lower free energy means better fit to the generative model.
pub fn compute_free_energy(
    prior: &StereotypePrior,
    observations: &BTreeMap<String, f64>,
    precisions: &BTreeMap<String, f64>,
) -> f64 {
    let default_precision = 1.0;
    let mut accuracy = 0.0;
    let mut complexity = 0.0;

    for (key, &obs_value) in observations {
        let precision = precisions.get(key).copied().unwrap_or(default_precision);
        accuracy += obs_value * precision;

        if let Some(attr_prior) = prior.attribute_priors.get(key) {
            match attr_prior {
                AttributePrior::Gaussian(param) => {
                    let variance = param.variance.max(0.01);
                    complexity += (obs_value - param.mean).powi(2) / (2.0 * variance);
                }
                AttributePrior::Categorical { counts, total } => {
                    if *total > 0 {
                        let max_prob = counts.values().max().copied().unwrap_or(0) as f64 / *total as f64;
                        complexity += (obs_value - max_prob).powi(2) / 2.0;
                    }
                }
            }
        }
    }

    complexity - accuracy
}

// ---------------------------------------------------------------------------
// Phase 3: Belief Updating
// ---------------------------------------------------------------------------

/// The delta to apply to a container's beliefs after observing query results.
#[derive(Clone, Debug, Serialize, Deserialize, PartialEq)]
pub struct BeliefDelta {
    pub container_id: String,
    pub confidence_delta: f64,
    pub prediction_error: f64,
    pub updated_attributes: BTreeMap<String, f64>,
}

/// Compute the belief delta for a container given observations and edge cost.
///
/// The Markov blanket attenuates the update by `1/cost` — higher-cost edges
/// (further from the agent's current context) produce weaker updates.
pub fn compute_belief_delta(
    prior: &StereotypePrior,
    observations: &BTreeMap<String, f64>,
    edge_cost: f64,
) -> BeliefDelta {
    let attenuation = if edge_cost > 0.0 { 1.0 / edge_cost } else { 1.0 };
    let mut total_error = 0.0;
    let mut updated_attributes = BTreeMap::new();

    for (key, &obs_value) in observations {
        let prior_mean = predict(prior, key).unwrap_or(0.0);
        let error = (obs_value - prior_mean).abs();
        total_error += error;

        // Move toward observation, attenuated by edge cost
        let update = prior_mean + (obs_value - prior_mean) * attenuation.min(1.0);
        updated_attributes.insert(key.clone(), update);
    }

    let prediction_error = total_error * attenuation.min(1.0);
    // Confidence increases when prediction error is low, decreases when high
    let confidence_delta = (1.0 - prediction_error.min(1.0)) * attenuation.min(1.0) - 0.5;

    BeliefDelta {
        container_id: prior.container_id.clone(),
        confidence_delta,
        prediction_error,
        updated_attributes,
    }
}

/// Apply a belief delta to a container's hypothesis_state.
///
/// Writes `fep:confidence`, `fep:prediction_error`, and `fep:last_observed` keys.
pub fn apply_belief_delta(container: &mut ContainerRecord, delta: &BeliefDelta) {
    let now = now_fixed();

    // Update or accumulate confidence
    let current_confidence = container
        .hypothesis_state
        .get("fep:confidence")
        .and_then(|a| a.value.parse::<f64>().ok())
        .unwrap_or(0.0);
    let new_confidence = (current_confidence + delta.confidence_delta).clamp(-1.0, 1.0);

    container.hypothesis_state.insert(
        "fep:confidence".to_string(),
        HypothesisAnnotation {
            key: "fep:confidence".to_string(),
            value: format!("{:.4}", new_confidence),
            updated_at: now,
        },
    );

    container.hypothesis_state.insert(
        "fep:prediction_error".to_string(),
        HypothesisAnnotation {
            key: "fep:prediction_error".to_string(),
            value: format!("{:.4}", delta.prediction_error),
            updated_at: now,
        },
    );

    container.hypothesis_state.insert(
        "fep:last_observed".to_string(),
        HypothesisAnnotation {
            key: "fep:last_observed".to_string(),
            value: now.to_rfc3339(),
            updated_at: now,
        },
    );

    // Write updated attribute values
    for (key, &value) in &delta.updated_attributes {
        container.hypothesis_state.insert(
            key.clone(),
            HypothesisAnnotation {
                key: key.clone(),
                value: format!("{:.4}", value),
                updated_at: now,
            },
        );
    }
}

// ---------------------------------------------------------------------------
// Phase 4: Active Inference Action Selection (Expected Free Energy)
// ---------------------------------------------------------------------------

/// Compute the expected free energy for a route/action.
///
/// G(route) = -pragmatic_value - epistemic_value
/// Lower G means the route is more desirable (better expected outcome).
pub fn compute_expected_free_energy(
    _prior: &StereotypePrior,
    pragmatic_value: f64,
    epistemic_value: f64,
) -> f64 {
    -pragmatic_value - epistemic_value
}

/// Compute the epistemic value (information gain) of observing a set of outcomes
/// given a prior. This is the expected reduction in uncertainty — approximated as
/// the KL divergence between a predicted posterior (shifted by observations) and
/// the prior.
///
/// For each observed attribute, we compute:
///   D_KL = (obs - prior_mean)^2 / (2 * variance) + ln(sqrt(variance))
/// This approximates the information gain under Gaussian assumptions.
pub fn compute_epistemic_value(prior: &StereotypePrior, observations: &BTreeMap<String, f64>) -> f64 {
    let mut total_info_gain = 0.0;

    for (key, &obs_value) in observations {
        if let Some(attr_prior) = prior.attribute_priors.get(key) {
            match attr_prior {
                AttributePrior::Gaussian(param) => {
                    let variance = param.variance.max(0.01);
                    // KL divergence component: squared prediction error / variance + uncertainty term
                    let prediction_error = (obs_value - param.mean).powi(2) / (2.0 * variance);
                    let uncertainty_bonus = (variance.sqrt()).ln().max(0.0);
                    total_info_gain += prediction_error + uncertainty_bonus;
                }
                AttributePrior::Categorical { counts, total } => {
                    if *total > 0 {
                        let max_prob = counts.values().max().copied().unwrap_or(0) as f64 / *total as f64;
                        // Higher entropy (lower max_prob) means more to learn
                        let entropy_term = 1.0 - max_prob;
                        total_info_gain += entropy_term;
                    }
                }
            }
        } else {
            // No prior for this attribute — maximum epistemic value (completely unknown)
            total_info_gain += 1.0;
        }
    }

    total_info_gain
}

// ---------------------------------------------------------------------------
// Phase 4b: RLHF Belief Updates
// ---------------------------------------------------------------------------

/// Default precision for automatic (non-RLHF) observations.
pub const AUTOMATIC_PRECISION: f64 = 1.0;
/// Default precision for RLHF observations (~5x stronger than automatic).
pub const RLHF_PRECISION: f64 = 5.0;

/// Compute a relevance observation for free energy scoring.
///
/// - `won`: true if this container produced the winning card, false if present but didn't win
/// - Returns 1.0 for a win, 0.0 for a loss
pub fn relevance_observation(won: bool) -> f64 {
    if won { 1.0 } else { 0.0 }
}

/// Compute free energy for a container's relevance prior against an observation.
///
/// This is the simplified single-attribute version used for the relevance model:
///   F = (observation - prior_mean)^2 / (2 * variance)
///
/// Lower F means the observation was expected (less surprise).
pub fn compute_relevance_free_energy(prior_mean: f64, prior_variance: f64, observation: f64) -> f64 {
    let variance = prior_variance.max(0.01);
    (observation - prior_mean).powi(2) / (2.0 * variance)
}

/// Apply an RLHF-weighted belief delta to a container's hypothesis_state.
///
/// Similar to `apply_belief_delta` but uses high precision for RLHF signals.
/// The `rlhf_precision` multiplier makes these updates ~5x stronger than automatic.
///
/// - `observation`: 1.0 for positive feedback, 0.0 for negative
/// - `precision`: the precision weight (use RLHF_PRECISION for RLHF, AUTOMATIC_PRECISION for auto)
pub fn apply_rlhf_belief_delta(
    container: &mut ContainerRecord,
    relevance_key: &str,
    observation: f64,
    precision: f64,
) {
    let now = now_fixed();

    // Read current prior mean from hypothesis_state
    let current_mean = container
        .hypothesis_state
        .get(relevance_key)
        .and_then(|a| serde_json::from_str::<GaussianParam>(&a.value).ok())
        .map(|p| p.mean)
        .unwrap_or(0.5);

    let current_variance = container
        .hypothesis_state
        .get(relevance_key)
        .and_then(|a| serde_json::from_str::<GaussianParam>(&a.value).ok())
        .map(|p| p.variance)
        .unwrap_or(1.0);

    // Bayesian update with precision weighting:
    // new_precision = old_precision + observation_precision
    // new_mean = (old_precision * old_mean + obs_precision * obs) / new_precision
    let old_precision = 1.0 / current_variance.max(0.01);
    let new_precision = old_precision + precision;
    let new_mean = (old_precision * current_mean + precision * observation) / new_precision;
    let new_variance = (1.0 / new_precision).max(0.01);

    let updated_param = GaussianParam {
        mean: new_mean,
        variance: new_variance,
    };
    let value = serde_json::to_string(&updated_param).unwrap_or_default();

    container.hypothesis_state.insert(
        relevance_key.to_string(),
        HypothesisAnnotation {
            key: relevance_key.to_string(),
            value,
            updated_at: now,
        },
    );

    // Record the RLHF observation metadata
    container.hypothesis_state.insert(
        "fep:last_rlhf_observation".to_string(),
        HypothesisAnnotation {
            key: "fep:last_rlhf_observation".to_string(),
            value: format!("obs={:.2},precision={:.1}", observation, precision),
            updated_at: now,
        },
    );
}

// ---------------------------------------------------------------------------
// Phase 5: Precision Learning
// ---------------------------------------------------------------------------

/// Decay precision toward a default value over time.
pub fn decay_precision(current: f64, default: f64, decay_rate: f64) -> f64 {
    current + decay_rate * (default - current)
}

/// Update precision based on observed prediction errors.
/// Lower errors -> higher precision (more reliable).
pub fn update_precision(observations: &[f64], current_precision: f64) -> f64 {
    if observations.is_empty() {
        return current_precision;
    }
    let mean_error: f64 = observations.iter().sum::<f64>() / observations.len() as f64;
    // Precision = inverse of mean squared error, bounded
    let observed_precision = 1.0 / (mean_error.powi(2) + 0.01);
    // Exponential moving average with current
    0.7 * current_precision + 0.3 * observed_precision
}

// ---------------------------------------------------------------------------
// Phase 6: Hierarchical Free Energy
// ---------------------------------------------------------------------------

/// Compute hierarchical free energy for a container, incorporating parent-level models.
///
/// Walks up the container hierarchy (via link nodes that reference parent containers).
/// Each parent acts as a higher-level generative model. The total free energy is:
///   F_total = F_self + sum(F_parent * attenuation)
/// where attenuation is based on edge cost (Markov blanket boundary).
pub fn compute_hierarchical_free_energy(
    snapshot: &AmsStore,
    container_id: &str,
    priors: &BTreeMap<String, StereotypePrior>,
) -> f64 {
    // Compute own free energy
    let own_prior = priors.get(container_id);
    let own_fe = if let Some(prior) = own_prior {
        let observations = extract_observations_from_container(snapshot, container_id);
        let precisions = BTreeMap::new();
        compute_free_energy(prior, &observations, &precisions)
    } else {
        0.0
    };

    // Walk parent containers and accumulate attenuated free energy
    let mut total_parent_fe = 0.0;
    let parent_ids = find_parent_container_ids(snapshot, container_id);
    for (parent_id, edge_cost) in &parent_ids {
        if let Some(parent_prior) = priors.get(parent_id.as_str()) {
            let parent_obs = extract_observations_from_container(snapshot, parent_id);
            let parent_precisions = BTreeMap::new();
            let parent_fe = compute_free_energy(parent_prior, &parent_obs, &parent_precisions);
            let attenuation = if *edge_cost > 0.0 { 1.0 / edge_cost } else { 1.0 };
            total_parent_fe += parent_fe * attenuation.min(1.0);
        }
    }

    own_fe + total_parent_fe
}

/// Propagate a belief delta up the container hierarchy.
/// Each level attenuates the update based on edge cost.
pub fn hierarchical_belief_update(
    snapshot: &AmsStore,
    container_id: &str,
    delta: &BeliefDelta,
    priors: &BTreeMap<String, StereotypePrior>,
) -> Vec<BeliefDelta> {
    let mut deltas = vec![delta.clone()];
    let parent_ids = find_parent_container_ids(snapshot, container_id);

    for (parent_id, edge_cost) in &parent_ids {
        if let Some(parent_prior) = priors.get(parent_id.as_str()) {
            let parent_delta = compute_belief_delta(parent_prior, &delta.updated_attributes, *edge_cost);
            deltas.push(parent_delta);
        }
    }

    deltas
}

// Helper: extract numeric observations from container hypothesis_state
fn extract_observations_from_container(snapshot: &AmsStore, container_id: &str) -> BTreeMap<String, f64> {
    let mut obs = BTreeMap::new();
    if let Some(container) = snapshot.containers().get(container_id) {
        for (key, annotation) in &container.hypothesis_state {
            if key.starts_with("fep:") { continue; }
            if let Ok(v) = annotation.value.parse::<f64>() {
                obs.insert(key.clone(), v);
            }
        }
    }
    obs
}

// Helper: find parent container IDs by walking link nodes
fn find_parent_container_ids(snapshot: &AmsStore, container_id: &str) -> Vec<(String, f64)> {
    let mut parents = Vec::new();
    let seen_container = container_id.to_string();

    for (_, link_node) in snapshot.link_nodes() {
        if link_node.container_id != seen_container {
            let has_shared_object = snapshot.link_nodes().values().any(|other| {
                other.container_id == seen_container && other.object_id == link_node.object_id
            });
            if has_shared_object {
                let edge_cost = link_node.metadata
                    .as_ref()
                    .and_then(|m| m.get("cost"))
                    .and_then(|v| v.as_f64())
                    .unwrap_or(1.0);
                if !parents.iter().any(|(id, _): &(String, f64)| id == &link_node.container_id) {
                    parents.push((link_node.container_id.clone(), edge_cost));
                }
            }
        }
    }

    parents
}

// ---------------------------------------------------------------------------
// Internal helpers
// ---------------------------------------------------------------------------

/// Build an `AttributePrior` from a JSON value in the stereotype map.
fn prior_from_value(_key: &str, value: &Value) -> Option<AttributePrior> {
    match value {
        Value::Number(n) => {
            let v = n.as_f64()?;
            Some(AttributePrior::Gaussian(GaussianParam {
                mean: v,
                variance: 1.0,
            }))
        }
        Value::Object(map) => {
            // Interpret as categorical counts: { "category_a": 3, "category_b": 5 }
            let mut counts = BTreeMap::new();
            let mut total: usize = 0;
            for (cat_key, cat_val) in map {
                if let Some(n) = cat_val.as_u64() {
                    counts.insert(cat_key.clone(), n as usize);
                    total += n as usize;
                }
            }
            if counts.is_empty() {
                None
            } else {
                Some(AttributePrior::Categorical { counts, total })
            }
        }
        Value::String(s) => {
            // Try numeric string
            if let Ok(v) = s.parse::<f64>() {
                Some(AttributePrior::Gaussian(GaussianParam {
                    mean: v,
                    variance: 1.0,
                }))
            } else {
                // Single categorical observation
                let mut counts = BTreeMap::new();
                counts.insert(s.clone(), 1);
                Some(AttributePrior::Categorical { counts, total: 1 })
            }
        }
        _ => None,
    }
}

/// Merge hypothesis_state values into existing attribute priors.
fn merge_hypothesis_priors(
    priors: &mut BTreeMap<String, AttributePrior>,
    hypothesis_state: &BTreeMap<String, HypothesisAnnotation>,
) {
    for (key, annotation) in hypothesis_state {
        if key.starts_with("fep:") {
            continue;
        }
        if let Ok(numeric) = annotation.value.parse::<f64>() {
            let entry = priors.entry(key.clone());
            match entry {
                std::collections::btree_map::Entry::Occupied(mut existing) => {
                    if let AttributePrior::Gaussian(ref mut param) = existing.get_mut() {
                        // Bayesian update: simple running mean with reduced variance
                        let new_mean = (param.mean + numeric) / 2.0;
                        let new_variance = param.variance / 2.0;
                        param.mean = new_mean;
                        param.variance = new_variance.max(0.01);
                    }
                }
                std::collections::btree_map::Entry::Vacant(slot) => {
                    slot.insert(AttributePrior::Gaussian(GaussianParam {
                        mean: numeric,
                        variance: 1.0,
                    }));
                }
            }
        }
    }
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    fn make_store_with_stereotype() -> AmsStore {
        let mut store = AmsStore::new();
        store
            .create_container("ctr:test".to_string(), "container", "smartlist")
            .unwrap();

        let container = store.containers_mut().get_mut("ctr:test").unwrap();

        // Set up stereotype with mixed types
        let mut stereotype = BTreeMap::new();
        stereotype.insert(
            "relevance".to_string(),
            Value::Number(serde_json::Number::from_f64(0.8).unwrap()),
        );
        let mut cat_map = serde_json::Map::new();
        cat_map.insert("topic_a".to_string(), Value::Number(3.into()));
        cat_map.insert("topic_b".to_string(), Value::Number(7.into()));
        stereotype.insert("topic_distribution".to_string(), Value::Object(cat_map));

        container.expectation_metadata.stereotype = Some(stereotype);

        // Add hypothesis_state entries
        container.hypothesis_state.insert(
            "confidence".to_string(),
            HypothesisAnnotation {
                key: "confidence".to_string(),
                value: "0.75".to_string(),
                updated_at: now_fixed(),
            },
        );

        store
    }

    #[test]
    fn extract_prior_from_stereotype_and_hypothesis() {
        let store = make_store_with_stereotype();
        let prior = extract_stereotype_prior(&store, "ctr:test").unwrap();

        assert_eq!(prior.container_id, "ctr:test");
        assert!(prior.attribute_priors.contains_key("relevance"));
        assert!(prior.attribute_priors.contains_key("topic_distribution"));
        assert!(prior.attribute_priors.contains_key("confidence"));
    }

    #[test]
    fn extract_prior_returns_none_for_empty_container() {
        let mut store = AmsStore::new();
        store
            .create_container("ctr:empty".to_string(), "container", "smartlist")
            .unwrap();

        assert!(extract_stereotype_prior(&store, "ctr:empty").is_none());
    }

    #[test]
    fn extract_prior_returns_none_for_unknown_container() {
        let store = AmsStore::new();
        assert!(extract_stereotype_prior(&store, "ctr:nonexistent").is_none());
    }

    #[test]
    fn predict_gaussian_returns_mean() {
        let store = make_store_with_stereotype();
        let prior = extract_stereotype_prior(&store, "ctr:test").unwrap();

        let predicted = predict(&prior, "relevance").unwrap();
        assert!((predicted - 0.8).abs() < f64::EPSILON);
    }

    #[test]
    fn predict_categorical_returns_max_probability() {
        let store = make_store_with_stereotype();
        let prior = extract_stereotype_prior(&store, "ctr:test").unwrap();

        let predicted = predict(&prior, "topic_distribution").unwrap();
        // topic_b has 7/10 = 0.7
        assert!((predicted - 0.7).abs() < f64::EPSILON);
    }

    #[test]
    fn predict_unknown_attribute_returns_none() {
        let store = make_store_with_stereotype();
        let prior = extract_stereotype_prior(&store, "ctr:test").unwrap();

        assert!(predict(&prior, "nonexistent").is_none());
    }

    #[test]
    fn cache_and_load_prior_roundtrips() {
        let store = make_store_with_stereotype();
        let prior = extract_stereotype_prior(&store, "ctr:test").unwrap();

        let mut container = store.containers().get("ctr:test").unwrap().clone();
        cache_prior(&mut container, &prior);

        let loaded = load_cached_prior(&container).unwrap();
        assert_eq!(loaded, prior);
    }

    #[test]
    fn fep_keys_are_excluded_from_priors() {
        let mut store = AmsStore::new();
        store
            .create_container("ctr:fep".to_string(), "container", "smartlist")
            .unwrap();

        let container = store.containers_mut().get_mut("ctr:fep").unwrap();
        container.hypothesis_state.insert(
            "fep:prior".to_string(),
            HypothesisAnnotation {
                key: "fep:prior".to_string(),
                value: "should_be_ignored".to_string(),
                updated_at: now_fixed(),
            },
        );
        container.hypothesis_state.insert(
            "actual_metric".to_string(),
            HypothesisAnnotation {
                key: "actual_metric".to_string(),
                value: "0.5".to_string(),
                updated_at: now_fixed(),
            },
        );

        let prior = extract_stereotype_prior(&store, "ctr:fep").unwrap();
        assert!(!prior.attribute_priors.contains_key("fep:prior"));
        assert!(prior.attribute_priors.contains_key("actual_metric"));
    }

    #[test]
    fn stereotype_linknode_merges_linked_container_hypothesis() {
        let mut store = AmsStore::new();

        // Source container
        store
            .create_container("ctr:source".to_string(), "container", "smartlist")
            .unwrap();
        // Target container (linked via stereotype)
        store
            .create_container("ctr:target".to_string(), "container", "smartlist")
            .unwrap();

        // Add an object for the link
        store
            .upsert_object("obj:link", "link_object", None, None, None)
            .unwrap();
        let link_id = store.add_object("ctr:target", "obj:link", None, None).unwrap();

        // Set hypothesis on target
        let target = store.containers_mut().get_mut("ctr:target").unwrap();
        target.hypothesis_state.insert(
            "linked_score".to_string(),
            HypothesisAnnotation {
                key: "linked_score".to_string(),
                value: "0.9".to_string(),
                updated_at: now_fixed(),
            },
        );

        // Set source to point to target via stereotype_linknode_id
        let source = store.containers_mut().get_mut("ctr:source").unwrap();
        source.expectation_metadata.stereotype_linknode_id = Some(link_id);
        source.hypothesis_state.insert(
            "local_score".to_string(),
            HypothesisAnnotation {
                key: "local_score".to_string(),
                value: "0.6".to_string(),
                updated_at: now_fixed(),
            },
        );

        let prior = extract_stereotype_prior(&store, "ctr:source").unwrap();
        assert!(prior.attribute_priors.contains_key("local_score"));
        assert!(prior.attribute_priors.contains_key("linked_score"));
    }

    #[test]
    fn prior_from_string_value_numeric() {
        let prior = prior_from_value("test", &Value::String("3.14".to_string())).unwrap();
        match prior {
            AttributePrior::Gaussian(p) => assert!((p.mean - 3.14).abs() < f64::EPSILON),
            _ => panic!("expected Gaussian"),
        }
    }

    #[test]
    fn compute_free_energy_zero_when_observations_match_prior() {
        let mut priors = BTreeMap::new();
        priors.insert(
            "score".to_string(),
            AttributePrior::Gaussian(GaussianParam {
                mean: 0.5,
                variance: 1.0,
            }),
        );
        let prior = StereotypePrior {
            container_id: "test".to_string(),
            attribute_priors: priors,
        };
        let mut observations = BTreeMap::new();
        observations.insert("score".to_string(), 0.5);
        let precisions = BTreeMap::new();

        let fe = compute_free_energy(&prior, &observations, &precisions);
        // complexity = (0.5 - 0.5)^2 / 2 = 0; accuracy = 0.5 * 1.0 = 0.5
        // F = 0 - 0.5 = -0.5
        assert!((fe - (-0.5)).abs() < f64::EPSILON);
    }

    #[test]
    fn compute_free_energy_complexity_grows_with_distance_from_prior() {
        let mut priors = BTreeMap::new();
        priors.insert(
            "score".to_string(),
            AttributePrior::Gaussian(GaussianParam {
                mean: 0.5,
                variance: 1.0,
            }),
        );
        let prior = StereotypePrior {
            container_id: "test".to_string(),
            attribute_priors: priors,
        };
        // Hold accuracy constant by using same precision=0 so accuracy contribution is zero
        let mut precisions = BTreeMap::new();
        precisions.insert("score".to_string(), 0.0);

        let mut obs_close = BTreeMap::new();
        obs_close.insert("score".to_string(), 0.6);
        let mut obs_far = BTreeMap::new();
        obs_far.insert("score".to_string(), 5.0);

        let fe_close = compute_free_energy(&prior, &obs_close, &precisions);
        let fe_far = compute_free_energy(&prior, &obs_far, &precisions);
        // With zero precision, only complexity matters -> further = higher FE
        assert!(fe_far > fe_close);
    }

    #[test]
    fn compute_free_energy_respects_precision_weighting() {
        let mut priors = BTreeMap::new();
        priors.insert(
            "score".to_string(),
            AttributePrior::Gaussian(GaussianParam {
                mean: 0.5,
                variance: 1.0,
            }),
        );
        let prior = StereotypePrior {
            container_id: "test".to_string(),
            attribute_priors: priors,
        };
        let mut observations = BTreeMap::new();
        observations.insert("score".to_string(), 0.5);

        let mut low_precision = BTreeMap::new();
        low_precision.insert("score".to_string(), 0.1);
        let mut high_precision = BTreeMap::new();
        high_precision.insert("score".to_string(), 10.0);

        let fe_low = compute_free_energy(&prior, &observations, &low_precision);
        let fe_high = compute_free_energy(&prior, &observations, &high_precision);
        // Higher precision -> higher accuracy -> lower free energy
        assert!(fe_high < fe_low);
    }

    #[test]
    fn prior_from_string_value_categorical() {
        let prior = prior_from_value("test", &Value::String("some_category".to_string())).unwrap();
        match prior {
            AttributePrior::Categorical { counts, total } => {
                assert_eq!(total, 1);
                assert_eq!(counts.get("some_category"), Some(&1));
            }
            _ => panic!("expected Categorical"),
        }
    }

    #[test]
    fn compute_belief_delta_low_error_increases_confidence() {
        let mut priors = BTreeMap::new();
        priors.insert(
            "score".to_string(),
            AttributePrior::Gaussian(GaussianParam {
                mean: 0.5,
                variance: 1.0,
            }),
        );
        let prior = StereotypePrior {
            container_id: "ctr:test".to_string(),
            attribute_priors: priors,
        };
        let mut observations = BTreeMap::new();
        observations.insert("score".to_string(), 0.5); // exact match

        let delta = compute_belief_delta(&prior, &observations, 1.0);
        assert!(delta.confidence_delta > 0.0);
        assert!(delta.prediction_error < f64::EPSILON);
    }

    #[test]
    fn compute_belief_delta_high_cost_attenuates_update() {
        let mut priors = BTreeMap::new();
        priors.insert(
            "score".to_string(),
            AttributePrior::Gaussian(GaussianParam {
                mean: 0.5,
                variance: 1.0,
            }),
        );
        let prior = StereotypePrior {
            container_id: "ctr:test".to_string(),
            attribute_priors: priors,
        };
        let mut observations = BTreeMap::new();
        observations.insert("score".to_string(), 1.0);

        let delta_close = compute_belief_delta(&prior, &observations, 1.0);
        let delta_far = compute_belief_delta(&prior, &observations, 10.0);
        // Higher cost -> more attenuation -> smaller prediction error
        assert!(delta_far.prediction_error < delta_close.prediction_error);
    }

    #[test]
    fn apply_belief_delta_writes_fep_keys() {
        let mut store = AmsStore::new();
        store
            .create_container("ctr:belief".to_string(), "container", "smartlist")
            .unwrap();

        let mut priors = BTreeMap::new();
        priors.insert(
            "score".to_string(),
            AttributePrior::Gaussian(GaussianParam {
                mean: 0.5,
                variance: 1.0,
            }),
        );
        let prior = StereotypePrior {
            container_id: "ctr:belief".to_string(),
            attribute_priors: priors,
        };
        let mut observations = BTreeMap::new();
        observations.insert("score".to_string(), 0.7);

        let delta = compute_belief_delta(&prior, &observations, 1.0);
        let container = store.containers_mut().get_mut("ctr:belief").unwrap();
        apply_belief_delta(container, &delta);

        assert!(container.hypothesis_state.contains_key("fep:confidence"));
        assert!(container.hypothesis_state.contains_key("fep:prediction_error"));
        assert!(container.hypothesis_state.contains_key("fep:last_observed"));
        assert!(container.hypothesis_state.contains_key("score"));

        let confidence: f64 = container
            .hypothesis_state
            .get("fep:confidence")
            .unwrap()
            .value
            .parse()
            .unwrap();
        assert!(confidence > -1.0 && confidence < 1.0);
    }

    #[test]
    fn apply_belief_delta_accumulates_confidence() {
        let mut store = AmsStore::new();
        store
            .create_container("ctr:accum".to_string(), "container", "smartlist")
            .unwrap();

        let mut priors = BTreeMap::new();
        priors.insert(
            "score".to_string(),
            AttributePrior::Gaussian(GaussianParam {
                mean: 0.5,
                variance: 1.0,
            }),
        );
        let prior = StereotypePrior {
            container_id: "ctr:accum".to_string(),
            attribute_priors: priors,
        };
        let mut observations = BTreeMap::new();
        observations.insert("score".to_string(), 0.5);

        let delta = compute_belief_delta(&prior, &observations, 1.0);
        let container = store.containers_mut().get_mut("ctr:accum").unwrap();

        apply_belief_delta(container, &delta);
        let c1: f64 = container.hypothesis_state.get("fep:confidence").unwrap().value.parse().unwrap();

        apply_belief_delta(container, &delta);
        let c2: f64 = container.hypothesis_state.get("fep:confidence").unwrap().value.parse().unwrap();

        // Confidence should accumulate (both positive deltas)
        assert!(c2 > c1);
    }

    // Phase 4 tests

    #[test]
    fn compute_efe_negative_for_good_route() {
        let prior = StereotypePrior {
            container_id: "test".to_string(),
            attribute_priors: BTreeMap::new(),
        };
        let efe = compute_expected_free_energy(&prior, 0.5, 0.3);
        // G = -0.5 - 0.3 = -0.8
        assert!((efe - (-0.8)).abs() < f64::EPSILON);
    }

    #[test]
    fn compute_efe_zero_when_no_value() {
        let prior = StereotypePrior {
            container_id: "test".to_string(),
            attribute_priors: BTreeMap::new(),
        };
        let efe = compute_expected_free_energy(&prior, 0.0, 0.0);
        assert!(efe.abs() < f64::EPSILON);
    }

    #[test]
    fn compute_efe_more_negative_with_higher_values() {
        let prior = StereotypePrior {
            container_id: "test".to_string(),
            attribute_priors: BTreeMap::new(),
        };
        let efe_low = compute_expected_free_energy(&prior, 0.1, 0.1);
        let efe_high = compute_expected_free_energy(&prior, 1.0, 1.0);
        assert!(efe_high < efe_low);
    }

    #[test]
    fn epistemic_value_increases_with_prediction_error() {
        let mut priors = BTreeMap::new();
        priors.insert(
            "score".to_string(),
            AttributePrior::Gaussian(GaussianParam {
                mean: 0.5,
                variance: 1.0,
            }),
        );
        let prior = StereotypePrior {
            container_id: "test".to_string(),
            attribute_priors: priors,
        };

        let mut obs_close = BTreeMap::new();
        obs_close.insert("score".to_string(), 0.6);
        let mut obs_far = BTreeMap::new();
        obs_far.insert("score".to_string(), 5.0);

        let ev_close = compute_epistemic_value(&prior, &obs_close);
        let ev_far = compute_epistemic_value(&prior, &obs_far);
        assert!(ev_far > ev_close);
    }

    #[test]
    fn epistemic_value_higher_for_unknown_attributes() {
        let prior = StereotypePrior {
            container_id: "test".to_string(),
            attribute_priors: BTreeMap::new(), // no priors at all
        };
        let mut observations = BTreeMap::new();
        observations.insert("unknown".to_string(), 0.5);

        let ev = compute_epistemic_value(&prior, &observations);
        // Unknown attribute -> max epistemic value of 1.0
        assert!((ev - 1.0).abs() < f64::EPSILON);
    }

    // Phase 5 tests

    #[test]
    fn decay_precision_returns_default_when_rate_is_one() {
        let result = decay_precision(5.0, 1.0, 1.0);
        assert!((result - 1.0).abs() < f64::EPSILON);
    }

    #[test]
    fn decay_precision_returns_current_when_rate_is_zero() {
        let result = decay_precision(5.0, 1.0, 0.0);
        assert!((result - 5.0).abs() < f64::EPSILON);
    }

    #[test]
    fn update_precision_increases_for_low_error_observations() {
        let current = 1.0;
        let observations = vec![0.01, 0.02, 0.01]; // very low errors
        let updated = update_precision(&observations, current);
        assert!(updated > current);
    }

    #[test]
    fn update_precision_decreases_for_high_error_observations() {
        let current = 10.0;
        let observations = vec![5.0, 6.0, 7.0]; // high errors
        let updated = update_precision(&observations, current);
        assert!(updated < current);
    }

    #[test]
    fn update_precision_returns_current_for_empty_observations() {
        let current = 3.5;
        let updated = update_precision(&[], current);
        assert!((updated - current).abs() < f64::EPSILON);
    }

    #[test]
    fn epistemic_value_categorical_reflects_entropy() {
        let mut priors = BTreeMap::new();

        // Low entropy: one dominant category
        let mut counts_low = BTreeMap::new();
        counts_low.insert("a".to_string(), 9);
        counts_low.insert("b".to_string(), 1);
        priors.insert(
            "low_entropy".to_string(),
            AttributePrior::Categorical {
                counts: counts_low,
                total: 10,
            },
        );

        // High entropy: even distribution
        let mut counts_high = BTreeMap::new();
        counts_high.insert("a".to_string(), 5);
        counts_high.insert("b".to_string(), 5);
        priors.insert(
            "high_entropy".to_string(),
            AttributePrior::Categorical {
                counts: counts_high,
                total: 10,
            },
        );

        let prior = StereotypePrior {
            container_id: "test".to_string(),
            attribute_priors: priors,
        };

        let mut obs_low = BTreeMap::new();
        obs_low.insert("low_entropy".to_string(), 0.5);
        let mut obs_high = BTreeMap::new();
        obs_high.insert("high_entropy".to_string(), 0.5);

        let ev_low = compute_epistemic_value(&prior, &obs_low);
        let ev_high = compute_epistemic_value(&prior, &obs_high);
        // Higher entropy -> more to learn -> higher epistemic value
        assert!(ev_high > ev_low);
    }

    // Phase 6 tests

    fn make_hierarchical_store() -> AmsStore {
        let mut store = AmsStore::new();

        // Child container
        store.create_container("ctr:child".to_string(), "container", "smartlist").unwrap();
        let child = store.containers_mut().get_mut("ctr:child").unwrap();
        child.hypothesis_state.insert(
            "score".to_string(),
            HypothesisAnnotation {
                key: "score".to_string(),
                value: "0.8".to_string(),
                updated_at: now_fixed(),
            },
        );

        // Parent container
        store.create_container("ctr:parent".to_string(), "container", "smartlist").unwrap();
        let parent = store.containers_mut().get_mut("ctr:parent").unwrap();
        parent.hypothesis_state.insert(
            "score".to_string(),
            HypothesisAnnotation {
                key: "score".to_string(),
                value: "0.6".to_string(),
                updated_at: now_fixed(),
            },
        );

        // Shared object linking child and parent
        store.upsert_object("obj:shared", "thing", None, None, None).unwrap();
        store.add_object("ctr:child", "obj:shared", None, Some("ln-child".to_string())).unwrap();
        store.add_object("ctr:parent", "obj:shared", None, Some("ln-parent".to_string())).unwrap();

        store
    }

    #[test]
    fn hierarchical_fe_returns_own_fe_when_no_parents() {
        let mut store = AmsStore::new();
        store.create_container("ctr:alone".to_string(), "container", "smartlist").unwrap();
        let c = store.containers_mut().get_mut("ctr:alone").unwrap();
        c.hypothesis_state.insert(
            "score".to_string(),
            HypothesisAnnotation {
                key: "score".to_string(),
                value: "0.5".to_string(),
                updated_at: now_fixed(),
            },
        );

        let mut priors = BTreeMap::new();
        priors.insert(
            "ctr:alone".to_string(),
            StereotypePrior {
                container_id: "ctr:alone".to_string(),
                attribute_priors: {
                    let mut ap = BTreeMap::new();
                    ap.insert("score".to_string(), AttributePrior::Gaussian(GaussianParam { mean: 0.5, variance: 1.0 }));
                    ap
                },
            },
        );

        let fe = compute_hierarchical_free_energy(&store, "ctr:alone", &priors);
        // No parents, so hierarchical FE equals own FE
        let own_fe = compute_free_energy(priors.get("ctr:alone").unwrap(), &{
            let mut obs = BTreeMap::new();
            obs.insert("score".to_string(), 0.5);
            obs
        }, &BTreeMap::new());
        assert!((fe - own_fe).abs() < f64::EPSILON);
    }

    #[test]
    fn hierarchical_fe_includes_attenuated_parent_fe() {
        let store = make_hierarchical_store();

        let mut priors = BTreeMap::new();
        priors.insert(
            "ctr:child".to_string(),
            StereotypePrior {
                container_id: "ctr:child".to_string(),
                attribute_priors: {
                    let mut ap = BTreeMap::new();
                    ap.insert("score".to_string(), AttributePrior::Gaussian(GaussianParam { mean: 0.5, variance: 1.0 }));
                    ap
                },
            },
        );
        priors.insert(
            "ctr:parent".to_string(),
            StereotypePrior {
                container_id: "ctr:parent".to_string(),
                attribute_priors: {
                    let mut ap = BTreeMap::new();
                    ap.insert("score".to_string(), AttributePrior::Gaussian(GaussianParam { mean: 0.3, variance: 1.0 }));
                    ap
                },
            },
        );

        let hierarchical_fe = compute_hierarchical_free_energy(&store, "ctr:child", &priors);
        let own_fe = compute_free_energy(priors.get("ctr:child").unwrap(), &{
            let mut obs = BTreeMap::new();
            obs.insert("score".to_string(), 0.8);
            obs
        }, &BTreeMap::new());

        // Hierarchical FE should differ from own FE due to parent contribution
        assert!((hierarchical_fe - own_fe).abs() > f64::EPSILON);
    }

    #[test]
    fn hierarchical_belief_update_returns_self_and_parent_deltas() {
        let store = make_hierarchical_store();

        let mut priors = BTreeMap::new();
        priors.insert(
            "ctr:child".to_string(),
            StereotypePrior {
                container_id: "ctr:child".to_string(),
                attribute_priors: {
                    let mut ap = BTreeMap::new();
                    ap.insert("score".to_string(), AttributePrior::Gaussian(GaussianParam { mean: 0.5, variance: 1.0 }));
                    ap
                },
            },
        );
        priors.insert(
            "ctr:parent".to_string(),
            StereotypePrior {
                container_id: "ctr:parent".to_string(),
                attribute_priors: {
                    let mut ap = BTreeMap::new();
                    ap.insert("score".to_string(), AttributePrior::Gaussian(GaussianParam { mean: 0.3, variance: 1.0 }));
                    ap
                },
            },
        );

        let mut observations = BTreeMap::new();
        observations.insert("score".to_string(), 0.9);
        let child_delta = compute_belief_delta(priors.get("ctr:child").unwrap(), &observations, 1.0);

        let deltas = hierarchical_belief_update(&store, "ctr:child", &child_delta, &priors);

        // Should have at least 2 deltas: self + parent
        assert!(deltas.len() >= 2);
        assert_eq!(deltas[0].container_id, "ctr:child");
        assert_eq!(deltas[1].container_id, "ctr:parent");
    }
}
