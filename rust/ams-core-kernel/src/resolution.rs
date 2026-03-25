//! Layer 3d — Object Resolution Engine
//!
//! Handles all resolution states for AMS objects and cached artifacts:
//!
//! - `Resolved`   — object is present and fully addressable.
//! - `Deferred`   — object reference is valid but content is not yet loaded.
//! - `Offline`    — object is known but its backing store is not currently reachable.
//! - `Moved`      — object has been relocated to a new ID or path.
//! - `Ghosted`    — object payload is missing; ghost traces remain in metadata.
//! - `Lost`       — object payload is missing and unrecoverable via known paths.
//! - `Rebound`    — object was previously ghosted/moved and has been successfully
//!                   re-attached to new content via a recovery path.
//!
//! ## Recovery Strategies
//!
//! When an object is not in the `Resolved` state the engine attempts recovery in order:
//!
//! 1. **Cached content** — check the cache layer (`cache.rs`) for a valid artifact
//!    matching the object's invocation identity.
//! 2. **Historical paths** — search for alternate object IDs recorded in provenance
//!    under `historical_ids` or `moved_to`.
//! 3. **Partial reconstruction** — if multiple ghost artifacts exist with the same
//!    source, attempt to reconstruct by selecting the most-recently-created valid
//!    sibling that matches tool + source identity.
//! 4. **Content-addressed lookup** — if the object carries an `artifact_fingerprint`,
//!    scan all artifacts in the substrate for a matching fingerprint in any state.
//!
//! ## Ghost Integration
//!
//! Ghost artifacts created by `cache::invalidate_artifact` / `cache::ghost_artifact`
//! retain their full provenance metadata.  The resolution engine reads that metadata
//! to attempt recovery before declaring an artifact `Lost`.

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use serde::{Deserialize, Serialize};

use crate::cache::{
    lookup_source_centric, lookup_tool_centric, revalidate_artifact, ValidityState,
    ARTIFACT_OBJECT_KIND,
};
use crate::model::SemanticPayload;
use crate::store::AmsStore;

// ── Resolution state ──────────────────────────────────────────────────────────

/// All possible resolution states for an AMS object reference.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ResolutionState {
    /// Object is present and fully addressable.
    Resolved,
    /// Object reference is valid but content not yet loaded (lazy/deferred access).
    Deferred,
    /// Object is known but its backing store is not currently reachable.
    Offline,
    /// Object has been relocated; a forwarding reference exists.
    Moved,
    /// Object payload is missing; ghost traces remain in metadata.
    Ghosted,
    /// Object payload is missing and unrecoverable via known paths.
    Lost,
    /// Object was previously ghosted/moved and has been successfully re-attached.
    Rebound,
}

impl ResolutionState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ResolutionState::Resolved => "resolved",
            ResolutionState::Deferred => "deferred",
            ResolutionState::Offline => "offline",
            ResolutionState::Moved => "moved",
            ResolutionState::Ghosted => "ghosted",
            ResolutionState::Lost => "lost",
            ResolutionState::Rebound => "rebound",
        }
    }

    pub fn from_str(s: &str) -> Option<Self> {
        match s {
            "resolved" => Some(ResolutionState::Resolved),
            "deferred" => Some(ResolutionState::Deferred),
            "offline" => Some(ResolutionState::Offline),
            "moved" => Some(ResolutionState::Moved),
            "ghosted" => Some(ResolutionState::Ghosted),
            "lost" => Some(ResolutionState::Lost),
            "rebound" => Some(ResolutionState::Rebound),
            _ => None,
        }
    }

    /// True if the object can be used directly without recovery.
    pub fn is_usable(&self) -> bool {
        matches!(self, ResolutionState::Resolved | ResolutionState::Rebound)
    }
}

impl std::fmt::Display for ResolutionState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ── Resolution outcome ────────────────────────────────────────────────────────

/// Describes the path taken to reach the current resolution.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum RecoveryPath {
    /// No recovery was needed; the object was directly present.
    Direct,
    /// Recovered from cache layer.
    CachedContent,
    /// Recovered via a historical path / `moved_to` forwarding reference.
    HistoricalPath,
    /// Recovered by selecting the most recent valid sibling artifact.
    PartialReconstruction,
    /// Recovered by matching the artifact fingerprint to another object.
    ContentAddressedLookup,
}

impl RecoveryPath {
    pub fn as_str(&self) -> &'static str {
        match self {
            RecoveryPath::Direct => "direct",
            RecoveryPath::CachedContent => "cached_content",
            RecoveryPath::HistoricalPath => "historical_path",
            RecoveryPath::PartialReconstruction => "partial_reconstruction",
            RecoveryPath::ContentAddressedLookup => "content_addressed_lookup",
        }
    }
}

/// The result of a resolution attempt.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct ResolutionResult {
    /// The object ID that was requested.
    pub requested_object_id: String,
    /// Final resolution state.
    pub state: ResolutionState,
    /// The object ID that was ultimately resolved to (may differ from requested if moved/rebound).
    pub resolved_object_id: Option<String>,
    /// How the resolution was achieved.
    pub recovery_path: RecoveryPath,
    /// Human-readable explanation of what happened.
    pub explanation: String,
    /// Whether the resolved object was promoted back to `Valid` in the cache.
    pub revalidated: bool,
}

impl ResolutionResult {
    fn direct(object_id: &str) -> Self {
        Self {
            requested_object_id: object_id.to_string(),
            state: ResolutionState::Resolved,
            resolved_object_id: Some(object_id.to_string()),
            recovery_path: RecoveryPath::Direct,
            explanation: "object is present and valid".to_string(),
            revalidated: false,
        }
    }

    fn lost(object_id: &str, explanation: impl Into<String>) -> Self {
        Self {
            requested_object_id: object_id.to_string(),
            state: ResolutionState::Lost,
            resolved_object_id: None,
            recovery_path: RecoveryPath::Direct,
            explanation: explanation.into(),
            revalidated: false,
        }
    }
}

// ── Resolution request ────────────────────────────────────────────────────────

/// Parameters for a resolution attempt.
#[derive(Clone, Debug)]
pub struct ResolutionRequest {
    /// The AMS Object ID to resolve.
    pub object_id: String,
    /// If known, the tool_id that produced this artifact (assists recovery).
    pub tool_id: Option<String>,
    /// If known, the source_id that was the input to the producing tool.
    pub source_id: Option<String>,
    /// If known, the param_hash for the invocation that produced this artifact.
    pub param_hash: Option<String>,
    /// Whether to attempt cache recovery.
    pub try_cache: bool,
    /// Whether to attempt historical path recovery.
    pub try_historical: bool,
    /// Whether to attempt partial reconstruction.
    pub try_partial_reconstruction: bool,
    /// Whether to attempt content-addressed lookup.
    pub try_content_addressed: bool,
    /// If true and recovery succeeds, revalidate the artifact in-place.
    pub revalidate_on_recovery: bool,
}

impl ResolutionRequest {
    /// Creates a request with all recovery strategies enabled.
    pub fn full(object_id: impl Into<String>) -> Self {
        Self {
            object_id: object_id.into(),
            tool_id: None,
            source_id: None,
            param_hash: None,
            try_cache: true,
            try_historical: true,
            try_partial_reconstruction: true,
            try_content_addressed: true,
            revalidate_on_recovery: false,
        }
    }

    /// Creates a minimal request (direct lookup only, no recovery).
    pub fn direct(object_id: impl Into<String>) -> Self {
        Self {
            object_id: object_id.into(),
            tool_id: None,
            source_id: None,
            param_hash: None,
            try_cache: false,
            try_historical: false,
            try_partial_reconstruction: false,
            try_content_addressed: false,
            revalidate_on_recovery: false,
        }
    }
}

// ── Resolution engine ─────────────────────────────────────────────────────────

/// Attempt to resolve an object, trying recovery strategies in order.
///
/// The engine inspects the object's current validity state (from cache provenance
/// or object presence) and escalates through recovery paths until either a usable
/// object is found or all options are exhausted.
pub fn resolve_object(store: &mut AmsStore, request: &ResolutionRequest) -> ResolutionResult {
    let object_id = &request.object_id;

    // ── 1. Direct: check if the object is present and valid ───────────────────
    match current_validity(store, object_id) {
        Some(ValidityState::Valid) => return ResolutionResult::direct(object_id),

        Some(ValidityState::Invalidated) => {
            // Invalidated is a policy decision, not a missing payload.
            return ResolutionResult {
                requested_object_id: object_id.clone(),
                state: ResolutionState::Offline,
                resolved_object_id: Some(object_id.clone()),
                recovery_path: RecoveryPath::Direct,
                explanation: "artifact is present but invalidated by policy".to_string(),
                revalidated: false,
            };
        }

        Some(ValidityState::Stale) => {
            // Stale artifacts are present but not eligible for reuse.
            return ResolutionResult {
                requested_object_id: object_id.clone(),
                state: ResolutionState::Deferred,
                resolved_object_id: Some(object_id.clone()),
                recovery_path: RecoveryPath::Direct,
                explanation: "artifact is present but stale; refresh recommended".to_string(),
                revalidated: false,
            };
        }

        Some(ValidityState::Ghosted) => {
            // Fall through to recovery — ghost traces may help.
        }

        Some(ValidityState::Lost) => {
            // Previously declared lost — try recovery anyway in case state has changed.
        }

        None => {
            // Object not present in the store at all.
            if !is_artifact_object(store, object_id) {
                // Non-artifact objects (plain ObjectRecords without cache provenance) —
                // they are either present or absent.
                return if store.objects().contains_key(object_id) {
                    ResolutionResult::direct(object_id)
                } else {
                    ResolutionResult::lost(
                        object_id,
                        "object not found in substrate",
                    )
                };
            }
        }
    }

    // ── 2. Extract provenance from the (possibly ghosted) object ─────────────
    let prov = extract_provenance(store, object_id);
    let prov_tool_id = prov.as_ref()
        .and_then(|p| p.get("tool_id")).and_then(|v| v.as_str()).map(str::to_string);
    let prov_source_id = prov.as_ref()
        .and_then(|p| p.get("source_id")).and_then(|v| v.as_str()).map(str::to_string);
    let prov_fingerprint = prov.as_ref()
        .and_then(|p| p.get("artifact_fingerprint")).and_then(|v| v.as_str()).map(str::to_string);
    let prov_moved_to = prov.as_ref()
        .and_then(|p| p.get("moved_to")).and_then(|v| v.as_str()).map(str::to_string);

    // Overlay caller-supplied hints over provenance.
    let eff_tool_id = request.tool_id.as_ref().or(prov_tool_id.as_ref()).cloned();
    let eff_source_id = request.source_id.as_ref().or(prov_source_id.as_ref()).cloned();
    let eff_param_hash = request.param_hash.clone();
    let eff_fingerprint = prov_fingerprint.clone();

    // ── 3. Historical path / moved_to ────────────────────────────────────────
    if request.try_historical {
        // Check `moved_to` forwarding reference in provenance.
        if let Some(ref moved_id) = prov_moved_to {
            if store.objects().contains_key(moved_id) {
                if let Some(ValidityState::Valid) = current_validity(store, moved_id) {
                    let result = ResolutionResult {
                        requested_object_id: object_id.clone(),
                        state: ResolutionState::Moved,
                        resolved_object_id: Some(moved_id.clone()),
                        recovery_path: RecoveryPath::HistoricalPath,
                        explanation: format!("object moved to '{}'", moved_id),
                        revalidated: false,
                    };
                    return result;
                }
            }
        }

        // Check `historical_ids` array in provenance.
        if let Some(ref p) = prov {
            if let Some(ids_val) = p.get("historical_ids") {
                if let Some(ids) = ids_val.as_array() {
                    for id_val in ids.iter().rev() {
                        if let Some(hist_id) = id_val.as_str() {
                            if store.objects().contains_key(hist_id) {
                                if let Some(ValidityState::Valid) = current_validity(store, hist_id) {
                                    let mut result = ResolutionResult {
                                        requested_object_id: object_id.clone(),
                                        state: ResolutionState::Rebound,
                                        resolved_object_id: Some(hist_id.to_string()),
                                        recovery_path: RecoveryPath::HistoricalPath,
                                        explanation: format!("recovered via historical id '{}'", hist_id),
                                        revalidated: false,
                                    };
                                    if request.revalidate_on_recovery {
                                        if let Ok(()) = revalidate_artifact(store, object_id) {
                                            result.revalidated = true;
                                        }
                                    }
                                    return result;
                                }
                            }
                        }
                    }
                }
            }
        }
    }

    // ── 4. Cache-layer recovery ───────────────────────────────────────────────
    if request.try_cache {
        if let (Some(ref tid), Some(ref sid)) = (&eff_tool_id, &eff_source_id) {
            let hits = lookup_tool_centric(store, tid, sid, eff_param_hash.as_deref());
            if let Some(hit) = hits.into_iter().find(|h| h.metadata.artifact_id != *object_id) {
                let recovered_id = hit.artifact_id.clone();
                let mut result = ResolutionResult {
                    requested_object_id: object_id.clone(),
                    state: ResolutionState::Rebound,
                    resolved_object_id: Some(recovered_id.clone()),
                    recovery_path: RecoveryPath::CachedContent,
                    explanation: format!(
                        "recovered from cache: tool='{}' source='{}' -> artifact '{}'",
                        tid, sid, recovered_id
                    ),
                    revalidated: false,
                };
                if request.revalidate_on_recovery {
                    if let Ok(()) = revalidate_artifact(store, object_id) {
                        result.revalidated = true;
                    }
                }
                return result;
            }
        }
    }

    // ── 5. Partial reconstruction: most-recent valid sibling ─────────────────
    if request.try_partial_reconstruction {
        if let Some(ref sid) = eff_source_id {
            let hits = lookup_source_centric(store, sid, eff_tool_id.as_deref(), None);
            if let Some(hit) = hits.into_iter().find(|h| h.metadata.artifact_id != *object_id) {
                let recovered_id = hit.artifact_id.clone();
                let mut result = ResolutionResult {
                    requested_object_id: object_id.clone(),
                    state: ResolutionState::Rebound,
                    resolved_object_id: Some(recovered_id.clone()),
                    recovery_path: RecoveryPath::PartialReconstruction,
                    explanation: format!(
                        "partially reconstructed from sibling artifact '{}' for source '{}'",
                        recovered_id, sid
                    ),
                    revalidated: false,
                };
                if request.revalidate_on_recovery {
                    if let Ok(()) = revalidate_artifact(store, object_id) {
                        result.revalidated = true;
                    }
                }
                return result;
            }
        }
    }

    // ── 6. Content-addressed lookup ───────────────────────────────────────────
    if request.try_content_addressed {
        if let Some(ref fingerprint) = eff_fingerprint {
            if let Some(matching_id) = find_by_fingerprint(store, fingerprint, object_id) {
                let mut result = ResolutionResult {
                    requested_object_id: object_id.clone(),
                    state: ResolutionState::Rebound,
                    resolved_object_id: Some(matching_id.clone()),
                    recovery_path: RecoveryPath::ContentAddressedLookup,
                    explanation: format!(
                        "content-addressed match: fingerprint '{}' -> '{}'",
                        fingerprint, matching_id
                    ),
                    revalidated: false,
                };
                if request.revalidate_on_recovery {
                    if let Ok(()) = revalidate_artifact(store, object_id) {
                        result.revalidated = true;
                    }
                }
                return result;
            }
        }
    }

    // ── 7. All strategies exhausted → Lost ────────────────────────────────────
    ResolutionResult::lost(
        object_id,
        "all recovery strategies exhausted; artifact is lost",
    )
}

/// Batch-resolve multiple objects.  Returns one `ResolutionResult` per request.
pub fn resolve_objects(store: &mut AmsStore, requests: &[ResolutionRequest]) -> Vec<ResolutionResult> {
    requests.iter().map(|r| resolve_object(store, r)).collect()
}

// ── Inspect helpers ───────────────────────────────────────────────────────────

/// Read the current `ResolutionState` of an object from stored provenance.
///
/// Returns `None` if the object carries no resolution state annotation.
pub fn read_resolution_state(store: &AmsStore, object_id: &str) -> Option<ResolutionState> {
    let prov = extract_provenance(store, object_id)?;
    let state_str = prov.get("resolution_state")?.as_str()?;
    ResolutionState::from_str(state_str)
}

/// Write a `ResolutionState` annotation into an object's provenance.
pub fn annotate_resolution_state(
    store: &mut AmsStore,
    object_id: &str,
    state: &ResolutionState,
    explanation: Option<&str>,
) -> Result<()> {
    let obj = store
        .objects_mut()
        .get_mut(object_id)
        .ok_or_else(|| anyhow!("object '{}' not found", object_id))?;
    let prov = obj
        .semantic_payload
        .get_or_insert_with(SemanticPayload::default)
        .provenance
        .get_or_insert_with(BTreeMap::new);
    prov.insert("resolution_state".into(), serde_json::Value::String(state.as_str().into()));
    if let Some(exp) = explanation {
        prov.insert("resolution_explanation".into(), serde_json::Value::String(exp.into()));
    }
    Ok(())
}

// ── Internal helpers ──────────────────────────────────────────────────────────

/// Returns the `ValidityState` recorded in an artifact's provenance, or `None`
/// if the object is absent or has no validity annotation.
fn current_validity(store: &AmsStore, object_id: &str) -> Option<ValidityState> {
    let prov = extract_provenance(store, object_id)?;
    let state_str = prov.get("validity_state")?.as_str()?;
    match state_str {
        "valid" => Some(ValidityState::Valid),
        "invalidated" => Some(ValidityState::Invalidated),
        "ghosted" => Some(ValidityState::Ghosted),
        "lost" => Some(ValidityState::Lost),
        "stale" => Some(ValidityState::Stale),
        _ => None,
    }
}

/// Returns true if the object exists and has `object_kind == "cache_artifact"`.
fn is_artifact_object(store: &AmsStore, object_id: &str) -> bool {
    store.objects()
        .get(object_id)
        .map(|o| o.object_kind == ARTIFACT_OBJECT_KIND)
        .unwrap_or(false)
}

/// Extracts the provenance map from an object's semantic payload.
fn extract_provenance(
    store: &AmsStore,
    object_id: &str,
) -> Option<BTreeMap<String, serde_json::Value>> {
    let obj = store.objects().get(object_id)?;
    let sp = obj.semantic_payload.as_ref()?;
    sp.provenance.clone()
}

/// Scan all artifact objects for one with a matching `artifact_fingerprint`,
/// excluding `exclude_id`.  Returns the first matching object ID, if any.
fn find_by_fingerprint(store: &AmsStore, fingerprint: &str, exclude_id: &str) -> Option<String> {
    for (obj_id, obj) in store.objects() {
        if obj_id == exclude_id {
            continue;
        }
        if obj.object_kind != ARTIFACT_OBJECT_KIND {
            continue;
        }
        let Some(ref sp) = obj.semantic_payload else { continue };
        let Some(ref prov) = sp.provenance else { continue };
        let state = prov.get("validity_state").and_then(|v| v.as_str()).unwrap_or("valid");
        if state == "lost" {
            continue;
        }
        if let Some(fp) = prov.get("artifact_fingerprint").and_then(|v| v.as_str()) {
            if fp == fingerprint {
                return Some(obj_id.clone());
            }
        }
    }
    None
}

// ── Tests ─────────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::{
        fresh_cache_entry, ghost_artifact, invalidate_artifact, new_artifact_id,
        promote_artifact, register_source, register_tool, InvalidationRequest,
    };
    use crate::model::now_fixed;
    use crate::store::AmsStore;

    fn make_store() -> AmsStore {
        AmsStore::default()
    }

    fn setup_promoted(store: &mut AmsStore) -> (crate::cache::ToolIdentity, crate::cache::SourceIdentity, String) {
        let tool = register_tool(store, "test-tool", "1.0", None).unwrap();
        let source = register_source(store, "test-source", None, None).unwrap();
        let inv = crate::cache::InvocationIdentity::new(&tool, &source, "hash1");
        let pr = promote_artifact(store, &tool, &source, &inv, None, Some("fp:abc"), "agent", None).unwrap();
        (tool, source, pr.artifact_id)
    }

    #[test]
    fn direct_resolution_of_valid_artifact() {
        let mut store = make_store();
        let (_tool, _source, artifact_id) = setup_promoted(&mut store);

        let result = resolve_object(&mut store, &ResolutionRequest::direct(artifact_id.clone()));
        assert_eq!(result.state, ResolutionState::Resolved);
        assert_eq!(result.resolved_object_id.as_deref(), Some(artifact_id.as_str()));
        assert_eq!(result.recovery_path, RecoveryPath::Direct);
    }

    #[test]
    fn ghosted_artifact_triggers_cache_recovery() {
        let mut store = make_store();
        let (tool, source, artifact_id) = setup_promoted(&mut store);

        // Promote a second valid artifact.
        let inv2 = crate::cache::InvocationIdentity::new(&tool, &source, "hash1");
        let pr2 = promote_artifact(&mut store, &tool, &source, &inv2, None, None, "agent", None).unwrap();
        let _second_id = pr2.artifact_id;

        // Ghost the first one.
        ghost_artifact(&mut store, &artifact_id, "test ghost").unwrap();

        let req = ResolutionRequest {
            object_id: artifact_id.clone(),
            tool_id: Some(tool.tool_id.clone()),
            source_id: Some(source.source_id.clone()),
            param_hash: None,
            try_cache: true,
            try_historical: true,
            try_partial_reconstruction: true,
            try_content_addressed: true,
            revalidate_on_recovery: false,
        };
        let result = resolve_object(&mut store, &req);
        assert_eq!(result.state, ResolutionState::Rebound);
        assert_eq!(result.recovery_path, RecoveryPath::CachedContent);
    }

    #[test]
    fn lost_artifact_with_no_recovery() {
        let mut store = make_store();
        let (tool, source, artifact_id) = setup_promoted(&mut store);

        // Mark it lost with no sibling.
        invalidate_artifact(&mut store, &InvalidationRequest {
            artifact_id: artifact_id.clone(),
            new_state: ValidityState::Lost,
            reason: Some("gone".to_string()),
        }).unwrap();

        let req = ResolutionRequest::full(artifact_id.clone());
        let result = resolve_object(&mut store, &req);
        assert_eq!(result.state, ResolutionState::Lost);
        assert!(result.resolved_object_id.is_none());
    }

    #[test]
    fn stale_artifact_resolves_as_deferred() {
        let mut store = make_store();
        let (_tool, _source, artifact_id) = setup_promoted(&mut store);

        crate::cache::stale_artifact(&mut store, &artifact_id, "freshness expired").unwrap();

        let result = resolve_object(&mut store, &ResolutionRequest::direct(artifact_id.clone()));
        assert_eq!(result.state, ResolutionState::Deferred);
    }

    #[test]
    fn content_addressed_recovery() {
        let mut store = make_store();
        let (tool, source, artifact_id) = setup_promoted(&mut store);

        // Promote a second artifact with the same fingerprint.
        let inv2 = crate::cache::InvocationIdentity::new(&tool, &source, "hash2");
        let pr2 = promote_artifact(&mut store, &tool, &source, &inv2, None, Some("fp:abc"), "agent", None).unwrap();
        let second_id = pr2.artifact_id;

        // Ghost the first one (retains its fingerprint).
        ghost_artifact(&mut store, &artifact_id, "gone").unwrap();

        // Disable all strategies except content-addressed.
        let req = ResolutionRequest {
            object_id: artifact_id.clone(),
            tool_id: None,
            source_id: None,
            param_hash: None,
            try_cache: false,
            try_historical: false,
            try_partial_reconstruction: false,
            try_content_addressed: true,
            revalidate_on_recovery: false,
        };
        let result = resolve_object(&mut store, &req);
        assert_eq!(result.state, ResolutionState::Rebound);
        assert_eq!(result.recovery_path, RecoveryPath::ContentAddressedLookup);
        assert_eq!(result.resolved_object_id.as_deref(), Some(second_id.as_str()));
    }

    #[test]
    fn annotate_and_read_resolution_state() {
        let mut store = make_store();
        let (_tool, _source, artifact_id) = setup_promoted(&mut store);

        annotate_resolution_state(&mut store, &artifact_id, &ResolutionState::Ghosted, Some("test")).unwrap();
        let state = read_resolution_state(&store, &artifact_id);
        assert_eq!(state, Some(ResolutionState::Ghosted));
    }

    #[test]
    fn moved_to_forwarding_recovery() {
        let mut store = make_store();
        let (tool, source, artifact_id) = setup_promoted(&mut store);

        // Promote a second artifact to act as the moved-to target.
        let inv2 = crate::cache::InvocationIdentity::new(&tool, &source, "hash1");
        let pr2 = promote_artifact(&mut store, &tool, &source, &inv2, None, None, "agent", None).unwrap();
        let moved_id = pr2.artifact_id;

        // Ghost the original and record `moved_to` in its provenance.
        ghost_artifact(&mut store, &artifact_id, "moved").unwrap();
        {
            let obj = store.objects_mut().get_mut(&artifact_id).unwrap();
            let prov = obj
                .semantic_payload
                .get_or_insert_with(SemanticPayload::default)
                .provenance
                .get_or_insert_with(BTreeMap::new);
            prov.insert("moved_to".into(), serde_json::Value::String(moved_id.clone()));
        }

        let req = ResolutionRequest {
            object_id: artifact_id.clone(),
            try_historical: true,
            try_cache: false,
            try_partial_reconstruction: false,
            try_content_addressed: false,
            revalidate_on_recovery: false,
            ..ResolutionRequest::full(artifact_id.clone())
        };
        let result = resolve_object(&mut store, &req);
        assert_eq!(result.state, ResolutionState::Moved);
        assert_eq!(result.recovery_path, RecoveryPath::HistoricalPath);
        assert_eq!(result.resolved_object_id.as_deref(), Some(moved_id.as_str()));
    }
}
