use std::collections::BTreeMap;

use anyhow::Result;
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::route_memory::ToolOutcome;
use crate::smartlist_write::{create_note, SmartListNoteInfo};
use crate::store::AmsStore;
use crate::tool_outcome::{
    classify_agent_tool_outcome, compute_tool_outcome_free_energy, ToolOutcomeDistribution,
};

// ---------------------------------------------------------------------------
// Tool Anomaly Detection
// ---------------------------------------------------------------------------

/// A single anomalous tool-call event whose free energy exceeds the threshold.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ToolAnomaly {
    pub tool_name: String,
    pub tool_use_id: String,
    pub outcome: ToolOutcome,
    pub free_energy: f64,
    pub threshold: f64,
    pub prior_total_observations: usize,
    pub prior_outcome_means: BTreeMap<String, f64>,
}

/// Default free-energy threshold for anomaly detection.
pub const DEFAULT_ANOMALY_THRESHOLD: f64 = 2.0;

/// Detect anomalous tool-call outcomes in a snapshot.
///
/// Walks all objects with `object_kind == "tool-call"` created after `since`,
/// classifies each outcome via [`classify_agent_tool_outcome`], computes
/// free energy against the tool's prior distribution, and returns those
/// exceeding `threshold`.
///
/// # Arguments
///
/// * `snapshot` — the AMS store containing tool-call objects
/// * `priors` — per-tool prior distributions (keyed by tool_name), typically
///   loaded via [`crate::fep_bootstrap::load_agent_tool_priors_from_snapshot`]
/// * `since` — only consider objects created after this timestamp
/// * `threshold` — free-energy threshold; calls with FE above this are anomalous
pub fn detect_tool_anomalies(
    snapshot: &AmsStore,
    priors: &BTreeMap<String, ToolOutcomeDistribution>,
    since: DateTime<FixedOffset>,
    threshold: f64,
) -> Vec<ToolAnomaly> {
    let mut anomalies = Vec::new();

    for (id, obj) in snapshot.objects() {
        if obj.object_kind != "tool-call" {
            continue;
        }

        if obj.created_at <= since {
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

        // Look up the tool's prior; skip if no prior exists
        let Some(distribution) = priors.get(tool_name) else {
            continue;
        };

        let fe = compute_tool_outcome_free_energy(distribution, outcome);

        if fe > threshold {
            let prior_outcome_means = distribution
                .outcome_params
                .iter()
                .map(|(o, p)| (o.to_string(), p.mean))
                .collect();

            anomalies.push(ToolAnomaly {
                tool_name: tool_name.to_string(),
                tool_use_id: id.clone(),
                outcome,
                free_energy: fe,
                threshold,
                prior_total_observations: distribution.total_observations,
                prior_outcome_means,
            });
        }
    }

    // Sort by free energy descending (most anomalous first)
    anomalies.sort_by(|a, b| b.free_energy.partial_cmp(&a.free_energy).unwrap_or(std::cmp::Ordering::Equal));

    anomalies
}

// ---------------------------------------------------------------------------
// SmartList Note Emission
// ---------------------------------------------------------------------------

const ANOMALY_BUCKET_PATH: &str = "fep-tool-anomalies";
const ANOMALY_CREATED_BY: &str = "fep-anomaly-detector";

/// Emit SmartList notes for a batch of detected tool anomalies.
///
/// Creates one note per anomaly under `smartlist/fep-tool-anomalies`.
/// Each note includes a structured text body and machine-parseable provenance
/// fields: `tool_name`, `tool_use_id`, `outcome`, `free_energy`, `threshold`.
///
/// Returns the list of created notes.
pub fn emit_anomaly_notes(
    store: &mut AmsStore,
    anomalies: &[ToolAnomaly],
    now: DateTime<FixedOffset>,
) -> Result<Vec<SmartListNoteInfo>> {
    let mut notes = Vec::with_capacity(anomalies.len());
    let bucket_paths = vec![ANOMALY_BUCKET_PATH.to_string()];

    for anomaly in anomalies {
        let title = format!(
            "FEP anomaly: {} {} (FE={:.2})",
            anomaly.tool_name, anomaly.outcome, anomaly.free_energy
        );

        let text = format_anomaly_text(anomaly);

        let note = create_note(
            store,
            &title,
            &text,
            &bucket_paths,
            false, // short_term durability — anomalies are transient signals
            ANOMALY_CREATED_BY,
            now,
            None, // auto-generate note_id
        )?;

        // Patch provenance with machine-parseable anomaly fields
        if let Some(obj) = store.objects_mut().get_mut(&note.note_id) {
            let prov = obj
                .semantic_payload
                .get_or_insert_with(Default::default)
                .provenance
                .get_or_insert_with(BTreeMap::new);

            prov.insert(
                "source".to_string(),
                Value::String("fep-anomaly-detector".to_string()),
            );
            prov.insert(
                "tool_name".to_string(),
                Value::String(anomaly.tool_name.clone()),
            );
            prov.insert(
                "tool_use_id".to_string(),
                Value::String(anomaly.tool_use_id.clone()),
            );
            prov.insert(
                "outcome".to_string(),
                Value::String(anomaly.outcome.to_string()),
            );
            prov.insert(
                "free_energy".to_string(),
                Value::Number(
                    serde_json::Number::from_f64(anomaly.free_energy).unwrap_or(0.into()),
                ),
            );
            prov.insert(
                "threshold".to_string(),
                Value::Number(
                    serde_json::Number::from_f64(anomaly.threshold).unwrap_or(0.into()),
                ),
            );
        }

        notes.push(note);
    }

    Ok(notes)
}

/// Format a human-readable text body for an anomaly note.
fn format_anomaly_text(anomaly: &ToolAnomaly) -> String {
    let mut lines = Vec::new();

    lines.push(format!("Tool: {}", anomaly.tool_name));
    lines.push(format!("Tool use ID: {}", anomaly.tool_use_id));
    lines.push(format!("Observed outcome: {}", anomaly.outcome));
    lines.push(format!("Free energy: {:.4}", anomaly.free_energy));
    lines.push(format!("Threshold: {:.4}", anomaly.threshold));
    lines.push(String::new());
    lines.push(format!(
        "Prior distribution (n={}):",
        anomaly.prior_total_observations
    ));
    for (outcome_name, mean) in &anomaly.prior_outcome_means {
        lines.push(format!("  {:<10} mean={:.3}", outcome_name, mean));
    }

    lines.join("\n")
}

// ---------------------------------------------------------------------------
// Tests
// ---------------------------------------------------------------------------

#[cfg(test)]
mod tests {
    use super::*;
    use crate::active_inference::GaussianParam;
    use crate::fep_bootstrap::bootstrap_agent_tool_priors;
    use crate::model::{ObjectRecord, SemanticPayload};
    use chrono::Utc;
    use serde_json::json;
    use std::collections::BTreeMap;

    fn make_tool_obj(
        id: &str,
        tool: &str,
        is_error: bool,
        preview: &str,
        time: DateTime<FixedOffset>,
    ) -> ObjectRecord {
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
            Some(time),
        )
    }

    fn build_high_success_priors() -> BTreeMap<String, ToolOutcomeDistribution> {
        // Simulate a tool with 95% Success rate
        let mut outcome_params = BTreeMap::new();
        outcome_params.insert(ToolOutcome::Success, GaussianParam { mean: 0.95, variance: 0.01 });
        outcome_params.insert(ToolOutcome::Weak, GaussianParam { mean: 0.01, variance: 0.01 });
        outcome_params.insert(ToolOutcome::Null, GaussianParam { mean: 0.02, variance: 0.01 });
        outcome_params.insert(ToolOutcome::Error, GaussianParam { mean: 0.01, variance: 0.01 });
        outcome_params.insert(ToolOutcome::Wasteful, GaussianParam { mean: 0.01, variance: 0.01 });

        let mut priors = BTreeMap::new();
        priors.insert(
            "Bash".to_string(),
            ToolOutcomeDistribution {
                context_key: "Bash".to_string(),
                outcome_params,
                total_observations: 100,
            },
        );
        priors
    }

    #[test]
    fn detects_error_as_anomaly_for_high_success_tool() {
        let mut snapshot = AmsStore::new();
        let now = Utc::now().fixed_offset();
        let past = now - chrono::Duration::hours(1);

        // Recent error call — should be anomalous
        snapshot.insert_object_record(make_tool_obj("tc-err", "Bash", true, "exit 1", now));
        // Recent success call — should not be anomalous
        snapshot.insert_object_record(make_tool_obj("tc-ok", "Bash", false, "OK", now));

        let priors = build_high_success_priors();
        let anomalies = detect_tool_anomalies(&snapshot, &priors, past, DEFAULT_ANOMALY_THRESHOLD);

        assert_eq!(anomalies.len(), 1);
        assert_eq!(anomalies[0].tool_use_id, "tc-err");
        assert_eq!(anomalies[0].outcome, ToolOutcome::Error);
        assert!(anomalies[0].free_energy > DEFAULT_ANOMALY_THRESHOLD);
    }

    #[test]
    fn filters_by_since_timestamp() {
        let mut snapshot = AmsStore::new();
        let now = Utc::now().fixed_offset();
        let old = now - chrono::Duration::hours(48);
        let since = now - chrono::Duration::hours(1);

        // Old error — should be filtered out
        snapshot.insert_object_record(make_tool_obj("tc-old-err", "Bash", true, "fail", old));
        // Recent error — should be detected
        snapshot.insert_object_record(make_tool_obj("tc-new-err", "Bash", true, "fail", now));

        let priors = build_high_success_priors();
        let anomalies = detect_tool_anomalies(&snapshot, &priors, since, DEFAULT_ANOMALY_THRESHOLD);

        assert_eq!(anomalies.len(), 1);
        assert_eq!(anomalies[0].tool_use_id, "tc-new-err");
    }

    #[test]
    fn skips_tools_without_priors() {
        let mut snapshot = AmsStore::new();
        let now = Utc::now().fixed_offset();
        let past = now - chrono::Duration::hours(1);

        // Error from an unknown tool
        snapshot.insert_object_record(make_tool_obj("tc-unk", "UnknownTool", true, "fail", now));

        let priors = build_high_success_priors(); // only has "Bash"
        let anomalies = detect_tool_anomalies(&snapshot, &priors, past, DEFAULT_ANOMALY_THRESHOLD);

        assert!(anomalies.is_empty());
    }

    #[test]
    fn no_anomalies_when_all_expected() {
        let mut snapshot = AmsStore::new();
        let now = Utc::now().fixed_offset();
        let past = now - chrono::Duration::hours(1);

        for i in 0..5 {
            snapshot.insert_object_record(make_tool_obj(
                &format!("tc-ok-{}", i),
                "Bash",
                false,
                "OK",
                now,
            ));
        }

        let priors = build_high_success_priors();
        let anomalies = detect_tool_anomalies(&snapshot, &priors, past, DEFAULT_ANOMALY_THRESHOLD);

        assert!(anomalies.is_empty());
    }

    #[test]
    fn sorted_by_free_energy_descending() {
        let mut snapshot = AmsStore::new();
        let now = Utc::now().fixed_offset();
        let past = now - chrono::Duration::hours(1);

        // Both should be anomalous, but Null (mean=0.02) is less surprising than Error (mean=0.01)
        snapshot.insert_object_record(make_tool_obj("tc-null", "Bash", false, "No matches", now));
        snapshot.insert_object_record(make_tool_obj("tc-err", "Bash", true, "fail", now));

        let priors = build_high_success_priors();
        let anomalies = detect_tool_anomalies(&snapshot, &priors, past, DEFAULT_ANOMALY_THRESHOLD);

        assert!(anomalies.len() >= 2);
        // Most anomalous first
        assert!(anomalies[0].free_energy >= anomalies[1].free_energy);
    }

    #[test]
    fn integrates_with_bootstrapped_priors() {
        let mut snapshot = AmsStore::new();
        let now = Utc::now().fixed_offset();
        let past = now - chrono::Duration::hours(1);

        // Build history: 20 successes for Bash
        for i in 0..20 {
            snapshot.insert_object_record(make_tool_obj(
                &format!("tc-hist-{}", i),
                "Bash",
                false,
                "OK",
                past,
            ));
        }

        // Bootstrap priors from the history
        let priors = bootstrap_agent_tool_priors(&snapshot);

        // Now add a recent error
        snapshot.insert_object_record(make_tool_obj("tc-recent-err", "Bash", true, "crash", now));

        let anomalies = detect_tool_anomalies(&snapshot, &priors, past, DEFAULT_ANOMALY_THRESHOLD);

        assert_eq!(anomalies.len(), 1);
        assert_eq!(anomalies[0].tool_use_id, "tc-recent-err");
        assert_eq!(anomalies[0].outcome, ToolOutcome::Error);
    }

    // -----------------------------------------------------------------------
    // SmartList note emission tests
    // -----------------------------------------------------------------------

    fn make_anomaly(tool: &str, id: &str, outcome: ToolOutcome, fe: f64) -> ToolAnomaly {
        let mut prior_means = BTreeMap::new();
        prior_means.insert("Success".to_string(), 0.95);
        prior_means.insert("Error".to_string(), 0.01);
        prior_means.insert("Null".to_string(), 0.02);
        prior_means.insert("Weak".to_string(), 0.01);
        prior_means.insert("Wasteful".to_string(), 0.01);
        ToolAnomaly {
            tool_name: tool.to_string(),
            tool_use_id: id.to_string(),
            outcome,
            free_energy: fe,
            threshold: DEFAULT_ANOMALY_THRESHOLD,
            prior_total_observations: 100,
            prior_outcome_means: prior_means,
        }
    }

    #[test]
    fn emit_anomaly_notes_creates_notes_under_bucket() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-16T10:00:00+00:00").unwrap();

        let anomalies = vec![
            make_anomaly("Bash", "toolu_abc123", ToolOutcome::Error, 3.21),
            make_anomaly("Grep", "toolu_xyz789", ToolOutcome::Null, 2.50),
        ];

        let notes = emit_anomaly_notes(&mut store, &anomalies, now).unwrap();
        assert_eq!(notes.len(), 2);

        // First note title
        assert!(notes[0].title.contains("Bash"));
        assert!(notes[0].title.contains("Error"));
        assert!(notes[0].title.contains("3.21"));
        assert_eq!(
            notes[0].bucket_paths,
            vec!["smartlist/fep-tool-anomalies".to_string()]
        );

        // Verify provenance was patched with machine-parseable fields
        let obj = store.objects().get(&notes[0].note_id).unwrap();
        let prov = obj
            .semantic_payload
            .as_ref()
            .and_then(|sp| sp.provenance.as_ref())
            .unwrap();
        assert_eq!(prov.get("tool_name").and_then(|v| v.as_str()), Some("Bash"));
        assert_eq!(
            prov.get("tool_use_id").and_then(|v| v.as_str()),
            Some("toolu_abc123")
        );
        assert_eq!(prov.get("outcome").and_then(|v| v.as_str()), Some("Error"));
        assert!(prov.get("free_energy").and_then(|v| v.as_f64()).is_some());
        assert!(prov.get("threshold").and_then(|v| v.as_f64()).is_some());
        assert_eq!(
            prov.get("source").and_then(|v| v.as_str()),
            Some("fep-anomaly-detector")
        );

        // Second note
        assert!(notes[1].title.contains("Grep"));
        assert!(notes[1].title.contains("Null"));
    }

    #[test]
    fn emit_anomaly_notes_empty_input_returns_empty() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-16T10:00:00+00:00").unwrap();

        let notes = emit_anomaly_notes(&mut store, &[], now).unwrap();
        assert!(notes.is_empty());
    }

    #[test]
    fn format_anomaly_text_includes_all_fields() {
        let anomaly = make_anomaly("Read", "toolu_xyz", ToolOutcome::Error, 4.5);
        let text = format_anomaly_text(&anomaly);

        assert!(text.contains("Tool: Read"));
        assert!(text.contains("toolu_xyz"));
        assert!(text.contains("Error"));
        assert!(text.contains("4.5000"));
        assert!(text.contains("2.0000")); // threshold
        assert!(text.contains("Prior distribution (n=100)"));
        assert!(text.contains("Success"));
    }

    #[test]
    fn end_to_end_detect_and_emit() {
        let mut store = AmsStore::new();
        let now = Utc::now().fixed_offset();
        let past = now - chrono::Duration::hours(1);

        // Recent error call
        store.insert_object_record(make_tool_obj("tc-err", "Bash", true, "exit 1", now));

        let priors = build_high_success_priors();
        let anomalies = detect_tool_anomalies(&store, &priors, past, DEFAULT_ANOMALY_THRESHOLD);
        assert!(!anomalies.is_empty());

        let notes = emit_anomaly_notes(&mut store, &anomalies, now).unwrap();
        assert_eq!(notes.len(), anomalies.len());

        // Verify the note references the correct tool
        let prov = store
            .objects()
            .get(&notes[0].note_id)
            .and_then(|o| o.semantic_payload.as_ref())
            .and_then(|sp| sp.provenance.as_ref())
            .unwrap();
        assert_eq!(prov.get("tool_name").and_then(|v| v.as_str()), Some("Bash"));
        assert_eq!(
            prov.get("tool_use_id").and_then(|v| v.as_str()),
            Some("tc-err")
        );
    }

    // -----------------------------------------------------------------------
    // Integration tests (p3d)
    // -----------------------------------------------------------------------

    #[test]
    fn full_pipeline_multi_tool_bootstrap_detect_emit() {
        // End-to-end: build history for two tools, bootstrap priors,
        // inject anomalous calls, detect, emit notes, verify everything.
        let mut store = AmsStore::new();
        let now = Utc::now().fixed_offset();
        let past = now - chrono::Duration::hours(2);
        let since = now - chrono::Duration::hours(1);

        // History: 30 Bash successes, 20 Grep successes
        for i in 0..30 {
            store.insert_object_record(make_tool_obj(
                &format!("hist-bash-{}", i), "Bash", false, "OK", past,
            ));
        }
        for i in 0..20 {
            store.insert_object_record(make_tool_obj(
                &format!("hist-grep-{}", i), "Grep", false, "found match", past,
            ));
        }

        let priors = bootstrap_agent_tool_priors(&store);
        assert!(priors.contains_key("Bash"));
        assert!(priors.contains_key("Grep"));

        // Inject recent anomalous calls
        store.insert_object_record(make_tool_obj("new-bash-err", "Bash", true, "segfault", now));
        store.insert_object_record(make_tool_obj("new-grep-null", "Grep", false, "No matches", now));
        // Also a normal Bash call — should NOT be flagged
        store.insert_object_record(make_tool_obj("new-bash-ok", "Bash", false, "compiled", now));

        let anomalies = detect_tool_anomalies(&store, &priors, since, DEFAULT_ANOMALY_THRESHOLD);

        // Bash error should be anomalous; Grep null may or may not depending on
        // the bootstrapped prior (20 successes -> null has mean ~0 -> high FE).
        assert!(anomalies.iter().any(|a| a.tool_use_id == "new-bash-err"));
        assert!(anomalies.iter().all(|a| a.tool_use_id != "new-bash-ok"));

        // Emit notes
        let notes = emit_anomaly_notes(&mut store, &anomalies, now).unwrap();
        assert_eq!(notes.len(), anomalies.len());

        // Every note is under the anomaly bucket
        for note in &notes {
            assert_eq!(
                note.bucket_paths,
                vec!["smartlist/fep-tool-anomalies".to_string()]
            );
        }

        // Every note has all required provenance fields
        for (i, note) in notes.iter().enumerate() {
            let prov = store
                .objects()
                .get(&note.note_id)
                .and_then(|o| o.semantic_payload.as_ref())
                .and_then(|sp| sp.provenance.as_ref())
                .expect("provenance must exist");

            assert!(prov.get("tool_name").and_then(|v| v.as_str()).is_some());
            assert!(prov.get("tool_use_id").and_then(|v| v.as_str()).is_some());
            assert!(prov.get("outcome").and_then(|v| v.as_str()).is_some());
            assert!(prov.get("free_energy").and_then(|v| v.as_f64()).is_some());
            assert!(prov.get("threshold").and_then(|v| v.as_f64()).is_some());
            assert_eq!(
                prov.get("source").and_then(|v| v.as_str()),
                Some("fep-anomaly-detector")
            );

            // Provenance values match the anomaly struct
            let anomaly = &anomalies[i];
            assert_eq!(
                prov.get("tool_name").and_then(|v| v.as_str()).unwrap(),
                anomaly.tool_name
            );
            assert_eq!(
                prov.get("tool_use_id").and_then(|v| v.as_str()).unwrap(),
                anomaly.tool_use_id
            );
        }
    }

    #[test]
    fn note_title_matches_exact_format() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-16T12:00:00+00:00").unwrap();

        let anomalies = vec![
            make_anomaly("Bash", "tc-1", ToolOutcome::Error, 4.567),
        ];

        let notes = emit_anomaly_notes(&mut store, &anomalies, now).unwrap();
        assert_eq!(notes[0].title, "FEP anomaly: Bash Error (FE=4.57)");
    }

    #[test]
    fn provenance_free_energy_and_threshold_are_exact() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-16T12:00:00+00:00").unwrap();

        let anomalies = vec![
            make_anomaly("Read", "tc-2", ToolOutcome::Null, 3.14),
        ];

        let notes = emit_anomaly_notes(&mut store, &anomalies, now).unwrap();
        let prov = store
            .objects()
            .get(&notes[0].note_id)
            .and_then(|o| o.semantic_payload.as_ref())
            .and_then(|sp| sp.provenance.as_ref())
            .unwrap();

        let fe = prov.get("free_energy").and_then(|v| v.as_f64()).unwrap();
        assert!((fe - 3.14).abs() < 0.001);

        let thresh = prov.get("threshold").and_then(|v| v.as_f64()).unwrap();
        assert!((thresh - 2.0).abs() < 0.001);
    }

    #[test]
    fn multiple_anomalies_same_tool_get_separate_notes() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-16T12:00:00+00:00").unwrap();

        let anomalies = vec![
            make_anomaly("Bash", "tc-err-1", ToolOutcome::Error, 3.5),
            make_anomaly("Bash", "tc-err-2", ToolOutcome::Error, 2.8),
            make_anomaly("Bash", "tc-null-1", ToolOutcome::Null, 2.3),
        ];

        let notes = emit_anomaly_notes(&mut store, &anomalies, now).unwrap();
        assert_eq!(notes.len(), 3);

        // Each note has a unique note_id
        let ids: std::collections::HashSet<_> = notes.iter().map(|n| &n.note_id).collect();
        assert_eq!(ids.len(), 3);

        // Each note's tool_use_id matches the anomaly
        for (i, note) in notes.iter().enumerate() {
            let prov = store
                .objects()
                .get(&note.note_id)
                .and_then(|o| o.semantic_payload.as_ref())
                .and_then(|sp| sp.provenance.as_ref())
                .unwrap();
            assert_eq!(
                prov.get("tool_use_id").and_then(|v| v.as_str()).unwrap(),
                anomalies[i].tool_use_id
            );
        }
    }

    #[test]
    fn custom_threshold_controls_sensitivity() {
        let mut store = AmsStore::new();
        let now = Utc::now().fixed_offset();
        let past = now - chrono::Duration::hours(1);

        store.insert_object_record(make_tool_obj("tc-err", "Bash", true, "fail", now));
        store.insert_object_record(make_tool_obj("tc-null", "Bash", false, "No matches", now));

        let priors = build_high_success_priors();

        // High threshold — fewer anomalies
        let strict = detect_tool_anomalies(&store, &priors, past, 10.0);
        // Low threshold — more anomalies
        let lenient = detect_tool_anomalies(&store, &priors, past, 0.5);

        assert!(lenient.len() >= strict.len());
    }

    #[test]
    fn anomaly_prior_outcome_means_populated() {
        let mut store = AmsStore::new();
        let now = Utc::now().fixed_offset();
        let past = now - chrono::Duration::hours(1);

        store.insert_object_record(make_tool_obj("tc-err", "Bash", true, "fail", now));

        let priors = build_high_success_priors();
        let anomalies = detect_tool_anomalies(&store, &priors, past, DEFAULT_ANOMALY_THRESHOLD);

        assert_eq!(anomalies.len(), 1);
        let a = &anomalies[0];
        assert_eq!(a.prior_total_observations, 100);
        assert!(a.prior_outcome_means.contains_key("Success"));
        assert!(a.prior_outcome_means.contains_key("Error"));
        assert!((a.prior_outcome_means["Success"] - 0.95).abs() < 0.001);
        assert!((a.prior_outcome_means["Error"] - 0.01).abs() < 0.001);
    }

    #[test]
    fn note_text_body_contains_prior_distribution() {
        let mut store = AmsStore::new();
        let now = DateTime::parse_from_rfc3339("2026-03-16T12:00:00+00:00").unwrap();

        let anomalies = vec![
            make_anomaly("Bash", "tc-1", ToolOutcome::Error, 3.0),
        ];

        let notes = emit_anomaly_notes(&mut store, &anomalies, now).unwrap();

        // Read the text from provenance (where create_note stores it)
        let prov = store
            .objects()
            .get(&notes[0].note_id)
            .and_then(|o| o.semantic_payload.as_ref())
            .and_then(|sp| sp.provenance.as_ref())
            .unwrap();
        let text = prov.get("text").and_then(|v| v.as_str()).unwrap();

        assert!(text.contains("Tool: Bash"));
        assert!(text.contains("Observed outcome: Error"));
        assert!(text.contains("Prior distribution (n=100)"));
        assert!(text.contains("Success"));
        assert!(text.contains("0.950")); // Success mean
    }
}
