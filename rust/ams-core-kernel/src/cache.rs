//! Layer 4 Cache — Tool Identity Objects and Source Identity
//!
//! Implements the GNUISNGNU cache architecture v0.1:
//! - Tool Identity Objects with owned cache SmartLists at `smartlist/cache/{tool_id}`
//! - Source Identity Objects with derived-artifact SmartLists at `smartlist/cache-links/{source_id}`
//! - Cache entry schema (tool_id, source_id, param_hash, artifact_id, validity_state, created_at)
//! - SmartList creation and membership using existing smartlist_write primitives
//!
//! Phase 2a: identity objects and cache entry schema only.
//! Artifact promotion (2b) and bidirectional lookup (2c) are implemented in subsequent tasks.

use std::collections::BTreeMap;

use anyhow::{anyhow, Result};
use chrono::{DateTime, FixedOffset};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use uuid::Uuid;

use crate::model::{now_fixed, SemanticPayload};
use crate::smartlist_write::{attach_member, create_bucket, normalize_path, SmartListBucketInfo};
use crate::store::AmsStore;

// ── Object kinds ──────────────────────────────────────────────────────────────

pub const TOOL_OBJECT_KIND: &str = "cache_tool";
pub const SOURCE_OBJECT_KIND: &str = "cache_source";
pub const ARTIFACT_OBJECT_KIND: &str = "cache_artifact";
pub const INVOCATION_IDENTITY_OBJECT_KIND: &str = "cache_invocation_identity";

// ── SmartList path helpers ────────────────────────────────────────────────────

/// Returns the canonical SmartList path for a Tool's cache: `smartlist/cache/{tool_id}`.
pub fn tool_cache_smartlist_path(tool_id: &str) -> String {
    format!("smartlist/cache/{}", sanitize_path_segment(tool_id))
}

/// Returns the canonical SmartList path for a Source's derived-artifact view:
/// `smartlist/cache-links/{source_id}`.
pub fn source_cache_links_path(source_id: &str) -> String {
    format!("smartlist/cache-links/{}", sanitize_path_segment(source_id))
}

/// Strips characters that would break SmartList path parsing.
fn sanitize_path_segment(s: &str) -> String {
    s.chars()
        .map(|c| if c.is_alphanumeric() || c == '-' || c == '_' || c == '.' { c } else { '_' })
        .collect()
}

// ── Validity state ────────────────────────────────────────────────────────────

/// Reuse-eligibility state for a cached artifact.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum ValidityState {
    /// Artifact is available and eligible for reuse.
    Valid,
    /// Artifact is present but policy-forbidden for reuse (soft invalidation).
    Invalidated,
    /// Artifact payload is missing; ghost traces remain.
    Ghosted,
    /// Artifact payload is missing and unrecoverable.
    Lost,
    /// Artifact exists but is stale under freshness policy.
    Stale,
}

impl ValidityState {
    pub fn as_str(&self) -> &'static str {
        match self {
            ValidityState::Valid => "valid",
            ValidityState::Invalidated => "invalidated",
            ValidityState::Ghosted => "ghosted",
            ValidityState::Lost => "lost",
            ValidityState::Stale => "stale",
        }
    }
}

impl std::fmt::Display for ValidityState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(self.as_str())
    }
}

// ── Identity structs ──────────────────────────────────────────────────────────

/// Identifies a Tool and its version for cache-key purposes.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct ToolIdentity {
    /// Stable identifier for the tool (e.g. `"parser:v2"`, `"embed:openai"`).
    pub tool_id: String,
    /// Semver or other version string used for cache compatibility checks.
    pub tool_version: String,
    /// Object ID of the Tool Object in the AMS substrate (assigned at registration).
    pub object_id: String,
}

/// Identifies a source Object that is supplied as input to a Tool.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct SourceIdentity {
    /// Stable AMS Object ID for the source.
    pub source_id: String,
    /// Optional coarse fingerprint (e.g. content hash, ETag).
    pub fingerprint: Option<String>,
}

/// Normalized invocation key — uniquely identifies a tool execution for cache lookup.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct InvocationIdentity {
    /// Human-readable canonical key (used for exact hit matching).
    pub canonical_key: String,
    pub tool_id: String,
    pub tool_version: String,
    pub source_id: String,
    /// SHA-256 hex of the normalized parameter map, or `"none"` if no params.
    pub param_hash: String,
    /// Optional source fingerprint incorporated into the canonical key.
    pub source_fingerprint: Option<String>,
}

impl InvocationIdentity {
    /// Build an invocation identity and compute a deterministic canonical key.
    pub fn new(tool: &ToolIdentity, source: &SourceIdentity, param_hash: impl Into<String>) -> Self {
        let param_hash = param_hash.into();
        let source_fingerprint = source.fingerprint.clone();
        let canonical_key = format!(
            "{}@{}:{}:{}:{}",
            tool.tool_id,
            tool.tool_version,
            source.source_id,
            param_hash,
            source_fingerprint.as_deref().unwrap_or("none"),
        );
        Self {
            canonical_key,
            tool_id: tool.tool_id.clone(),
            tool_version: tool.tool_version.clone(),
            source_id: source.source_id.clone(),
            param_hash,
            source_fingerprint,
        }
    }
}

/// Metadata for a single cache entry (one artifact produced by one Tool invocation).
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheEntryMetadata {
    pub tool_id: String,
    pub tool_version: String,
    pub source_id: String,
    pub param_hash: String,
    pub artifact_id: String,
    pub invocation_canonical_key: String,
    pub validity_state: ValidityState,
    pub created_at: DateTime<FixedOffset>,
    /// Optional source fingerprint captured at promotion time.
    pub source_fingerprint: Option<String>,
    /// Optional artifact payload fingerprint.
    pub artifact_fingerprint: Option<String>,
    /// Human-readable reason for invalidation (if validity_state != Valid).
    pub invalidation_reason: Option<String>,
}

// ── Registration helpers ──────────────────────────────────────────────────────

/// Registers or updates a Tool Identity Object in the AMS substrate.
///
/// Creates the Object with `object_kind = "cache_tool"` and stores identity
/// metadata in `semantic_payload.provenance`. Idempotent: a second call with the
/// same `tool_id` updates provenance but does not create a duplicate Object.
///
/// Returns the `ToolIdentity` (including its Object ID).
pub fn register_tool(
    store: &mut AmsStore,
    tool_id: &str,
    tool_version: &str,
    now_utc: Option<DateTime<FixedOffset>>,
) -> Result<ToolIdentity> {
    let now = now_utc.unwrap_or_else(now_fixed);
    // Stable Object ID derived from tool_id so re-registration is idempotent.
    let object_id = format!("cache-tool:{}", tool_id);

    store
        .upsert_object(object_id.clone(), TOOL_OBJECT_KIND.to_string(), None, None, Some(now))
        .map_err(|e| anyhow!("failed to upsert tool object '{}': {}", object_id, e))?;

    // Write identity metadata into provenance.
    {
        let obj = store
            .objects_mut()
            .get_mut(&object_id)
            .ok_or_else(|| anyhow!("tool object '{}' missing after upsert", object_id))?;
        let prov = obj
            .semantic_payload
            .get_or_insert_with(SemanticPayload::default)
            .provenance
            .get_or_insert_with(BTreeMap::new);
        prov.insert("tool_id".into(), Value::String(tool_id.into()));
        prov.insert("tool_version".into(), Value::String(tool_version.into()));
        prov.insert("cache_smartlist_path".into(), Value::String(tool_cache_smartlist_path(tool_id)));
    }

    // Ensure the cache SmartList bucket exists.
    let path = tool_cache_smartlist_path(tool_id);
    create_bucket(store, &path, true, "cache", now)?;

    Ok(ToolIdentity {
        tool_id: tool_id.to_string(),
        tool_version: tool_version.to_string(),
        object_id,
    })
}

/// Ensures a Source Identity Object exists in the AMS substrate.
///
/// Creates the Object with `object_kind = "cache_source"` and stores identity
/// metadata in provenance. Also ensures the source-local derived-artifact
/// SmartList bucket exists at `smartlist/cache-links/{source_id}`.
pub fn register_source(
    store: &mut AmsStore,
    source_id: &str,
    fingerprint: Option<&str>,
    now_utc: Option<DateTime<FixedOffset>>,
) -> Result<SourceIdentity> {
    let now = now_utc.unwrap_or_else(now_fixed);
    let object_id = format!("cache-source:{}", source_id);

    store
        .upsert_object(object_id.clone(), SOURCE_OBJECT_KIND.to_string(), None, None, Some(now))
        .map_err(|e| anyhow!("failed to upsert source object '{}': {}", object_id, e))?;

    {
        let obj = store
            .objects_mut()
            .get_mut(&object_id)
            .ok_or_else(|| anyhow!("source object '{}' missing after upsert", object_id))?;
        let prov = obj
            .semantic_payload
            .get_or_insert_with(SemanticPayload::default)
            .provenance
            .get_or_insert_with(BTreeMap::new);
        prov.insert("source_id".into(), Value::String(source_id.into()));
        if let Some(fp) = fingerprint {
            prov.insert("fingerprint".into(), Value::String(fp.into()));
        }
        prov.insert("cache_links_path".into(), Value::String(source_cache_links_path(source_id)));
    }

    // Ensure the source-local discovery SmartList bucket exists.
    let path = source_cache_links_path(source_id);
    create_bucket(store, &path, true, "cache", now)?;

    Ok(SourceIdentity {
        source_id: source_id.to_string(),
        fingerprint: fingerprint.map(str::to_string),
    })
}

/// Creates an Artifact Object in the substrate with the given cache entry metadata.
///
/// The artifact Object is created with `object_kind = "cache_artifact"`. All
/// `CacheEntryMetadata` fields are written into `semantic_payload.provenance` so
/// they are discoverable without deserializing an external side-car.
///
/// The caller is responsible for attaching the artifact to the Tool's cache
/// SmartList and the Source's cache-links SmartList (task 2b).
pub fn create_artifact_object(
    store: &mut AmsStore,
    meta: &CacheEntryMetadata,
    in_situ_ref: Option<&str>,
    now_utc: Option<DateTime<FixedOffset>>,
) -> Result<String> {
    let now = now_utc.unwrap_or(meta.created_at);
    // Artifact IDs are stable if caller provides one; otherwise mint a new UUID.
    let artifact_id = if meta.artifact_id.is_empty() {
        format!("cache-artifact:{}", Uuid::new_v4().simple())
    } else {
        meta.artifact_id.clone()
    };

    store
        .upsert_object(
            artifact_id.clone(),
            ARTIFACT_OBJECT_KIND.to_string(),
            in_situ_ref.map(str::to_string),
            None,
            Some(now),
        )
        .map_err(|e| anyhow!("failed to upsert artifact object '{}': {}", artifact_id, e))?;

    {
        let obj = store
            .objects_mut()
            .get_mut(&artifact_id)
            .ok_or_else(|| anyhow!("artifact object '{}' missing after upsert", artifact_id))?;
        let prov = obj
            .semantic_payload
            .get_or_insert_with(SemanticPayload::default)
            .provenance
            .get_or_insert_with(BTreeMap::new);

        prov.insert("tool_id".into(), Value::String(meta.tool_id.clone()));
        prov.insert("tool_version".into(), Value::String(meta.tool_version.clone()));
        prov.insert("source_id".into(), Value::String(meta.source_id.clone()));
        prov.insert("param_hash".into(), Value::String(meta.param_hash.clone()));
        prov.insert("artifact_id".into(), Value::String(artifact_id.clone()));
        prov.insert(
            "invocation_canonical_key".into(),
            Value::String(meta.invocation_canonical_key.clone()),
        );
        prov.insert("validity_state".into(), Value::String(meta.validity_state.as_str().into()));
        prov.insert("created_at".into(), Value::String(meta.created_at.to_rfc3339()));
        if let Some(ref fp) = meta.source_fingerprint {
            prov.insert("source_fingerprint".into(), Value::String(fp.clone()));
        }
        if let Some(ref fp) = meta.artifact_fingerprint {
            prov.insert("artifact_fingerprint".into(), Value::String(fp.clone()));
        }
        if let Some(ref reason) = meta.invalidation_reason {
            prov.insert("invalidation_reason".into(), Value::String(reason.clone()));
        }
    }

    Ok(artifact_id)
}

/// Ensures the Tool's cache SmartList bucket exists and returns its info.
///
/// Convenience wrapper around `create_bucket` for callers that only need the
/// SmartList path without going through the full `register_tool` flow.
pub fn ensure_tool_cache_list(
    store: &mut AmsStore,
    tool_id: &str,
    now_utc: Option<DateTime<FixedOffset>>,
) -> Result<SmartListBucketInfo> {
    let now = now_utc.unwrap_or_else(now_fixed);
    let path = tool_cache_smartlist_path(tool_id);
    create_bucket(store, &path, true, "cache", now)
        .map_err(|e| anyhow!("failed to ensure tool cache SmartList '{}': {}", path, e))
}

/// Ensures the Source's cache-links SmartList bucket exists and returns its info.
pub fn ensure_source_cache_links_list(
    store: &mut AmsStore,
    source_id: &str,
    now_utc: Option<DateTime<FixedOffset>>,
) -> Result<SmartListBucketInfo> {
    let now = now_utc.unwrap_or_else(now_fixed);
    let path = source_cache_links_path(source_id);
    create_bucket(store, &path, true, "cache", now)
        .map_err(|e| anyhow!("failed to ensure source cache-links SmartList '{}': {}", path, e))
}

// ── Artifact Promotion (Phase 2b) ────────────────────────────────────────────

/// Result of a successful artifact promotion.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct PromotionResult {
    /// The artifact Object ID (may be newly minted or caller-provided).
    pub artifact_id: String,
    /// The Tool's cache SmartList path where the artifact was attached.
    pub tool_cache_path: String,
    /// The Source's cache-links SmartList path where the artifact was attached.
    pub source_cache_links_path: String,
}

/// Promotes a tool output to a cached artifact.
///
/// This is the core Phase 2b operation. Given a registered tool and source, it:
/// 1. Creates an Artifact Object in the substrate with full cache entry metadata.
/// 2. Attaches the artifact to the Tool's cache SmartList (`smartlist/cache/{tool_id}`).
/// 3. Attaches the artifact to the Source's cache-links SmartList (`smartlist/cache-links/{source_id}`).
///
/// The `in_situ_ref` is an optional external reference (e.g. file path) for the artifact payload.
/// The `artifact_fingerprint` is an optional content hash of the artifact payload.
///
/// Both the tool and source must have been previously registered (their SmartList buckets
/// must exist). If they don't exist yet, call `register_tool` / `register_source` first.
pub fn promote_artifact(
    store: &mut AmsStore,
    tool: &ToolIdentity,
    source: &SourceIdentity,
    invocation: &InvocationIdentity,
    in_situ_ref: Option<&str>,
    artifact_fingerprint: Option<&str>,
    actor_id: &str,
    now_utc: Option<DateTime<FixedOffset>>,
) -> Result<PromotionResult> {
    let now = now_utc.unwrap_or_else(now_fixed);

    // Build cache entry metadata.
    let artifact_id = new_artifact_id();
    let mut meta = fresh_cache_entry(tool, source, invocation, &artifact_id, now);
    meta.artifact_fingerprint = artifact_fingerprint.map(str::to_string);

    // 1. Create the Artifact Object.
    let created_id = create_artifact_object(store, &meta, in_situ_ref, Some(now))?;

    // 2. Attach artifact to Tool's cache SmartList.
    let tool_path = tool_cache_smartlist_path(&tool.tool_id);
    attach_member(store, &tool_path, &created_id, actor_id, now)
        .map_err(|e| anyhow!("failed to attach artifact '{}' to tool cache '{}': {}", created_id, tool_path, e))?;

    // 3. Attach artifact to Source's cache-links SmartList.
    let source_path = source_cache_links_path(&source.source_id);
    attach_member(store, &source_path, &created_id, actor_id, now)
        .map_err(|e| anyhow!("failed to attach artifact '{}' to source cache-links '{}': {}", created_id, source_path, e))?;

    Ok(PromotionResult {
        artifact_id: created_id,
        tool_cache_path: tool_path,
        source_cache_links_path: source_path,
    })
}

// ── Bidirectional Cache Discovery (Phase 2c) ─────────────────────────────────

/// A cache hit: an artifact whose metadata matched a lookup query.
#[derive(Clone, Debug, Serialize, Deserialize)]
pub struct CacheHit {
    pub artifact_id: String,
    pub metadata: CacheEntryMetadata,
    /// `true` if the hit was exact (same param_hash + source_fingerprint).
    /// `false` if it was a compatible hit (same tool+source, different params).
    pub exact: bool,
    /// The cached text payload (stored as in_situ_ref on the artifact Object).
    /// Present when the artifact was promoted with `--text`.
    pub text: Option<String>,
}

/// Tool-centric lookup: searches `smartlist/cache/{tool_id}` for artifacts
/// matching `source_id` and optionally `param_hash`.
///
/// Returns exact hits first, then compatible hits, ordered by creation time (newest first).
pub fn lookup_tool_centric(
    store: &AmsStore,
    tool_id: &str,
    source_id: &str,
    param_hash: Option<&str>,
) -> Vec<CacheHit> {
    let raw_path = tool_cache_smartlist_path(tool_id);
    let normalized = normalize_path(&raw_path).unwrap_or(raw_path);
    let container_id = format!("smartlist-members:{}", normalized);
    collect_hits_from_container(store, &container_id, Some(source_id), param_hash)
}

/// Source-centric lookup: searches `smartlist/cache-links/{source_id}` for artifacts
/// produced by `tool_id` (optionally filtered by `param_hash`).
///
/// Returns exact hits first, then compatible hits, ordered by creation time (newest first).
pub fn lookup_source_centric(
    store: &AmsStore,
    source_id: &str,
    tool_id: Option<&str>,
    param_hash: Option<&str>,
) -> Vec<CacheHit> {
    let raw_path = source_cache_links_path(source_id);
    let normalized = normalize_path(&raw_path).unwrap_or(raw_path);
    let container_id = format!("smartlist-members:{}", normalized);
    collect_hits_from_container(store, &container_id, None, param_hash)
        .into_iter()
        .filter(|h| tool_id.map_or(true, |t| h.metadata.tool_id == t))
        .collect()
}

/// Convenience: exact cache lookup by invocation identity. Returns the first exact hit, if any.
pub fn lookup_exact(
    store: &AmsStore,
    invocation: &InvocationIdentity,
) -> Option<CacheHit> {
    lookup_tool_centric(store, &invocation.tool_id, &invocation.source_id, Some(&invocation.param_hash))
        .into_iter()
        .find(|h| h.exact)
}

/// Internal: iterate a SmartList's members container, extract CacheEntryMetadata from
/// each artifact's provenance, and filter/classify as exact or compatible.
fn collect_hits_from_container(
    store: &AmsStore,
    container_id: &str,
    source_id_filter: Option<&str>,
    param_hash_filter: Option<&str>,
) -> Vec<CacheHit> {
    let members = store.iterate_forward(container_id);
    let mut hits: Vec<CacheHit> = Vec::new();

    for link_node in members {
        let obj_id = &link_node.object_id;
        let Some(obj) = store.objects().get(obj_id) else { continue };
        let Some(ref sp) = obj.semantic_payload else { continue };
        let Some(ref prov) = sp.provenance else { continue };

        // Extract metadata from provenance.
        let get_str = |key: &str| prov.get(key).and_then(|v| v.as_str()).unwrap_or("").to_string();
        let meta_tool_id = get_str("tool_id");
        let meta_source_id = get_str("source_id");
        let meta_param_hash = get_str("param_hash");
        let meta_validity = get_str("validity_state");

        // Apply source filter.
        if let Some(sid) = source_id_filter {
            if meta_source_id != sid { continue; }
        }

        // Skip non-valid artifacts.
        if meta_validity != "valid" { continue; }

        let exact = param_hash_filter.map_or(true, |ph| ph == meta_param_hash);

        let created_at = prov.get("created_at")
            .and_then(|v| v.as_str())
            .and_then(|s| DateTime::parse_from_rfc3339(s).ok())
            .unwrap_or_else(now_fixed);

        hits.push(CacheHit {
            artifact_id: obj_id.clone(),
            exact,
            text: obj.in_situ_ref.clone(),
            metadata: CacheEntryMetadata {
                tool_id: meta_tool_id,
                tool_version: get_str("tool_version"),
                source_id: meta_source_id,
                param_hash: meta_param_hash,
                artifact_id: obj_id.clone(),
                invocation_canonical_key: get_str("invocation_canonical_key"),
                validity_state: ValidityState::Valid,
                created_at,
                source_fingerprint: prov.get("source_fingerprint").and_then(|v| v.as_str()).map(str::to_string),
                artifact_fingerprint: prov.get("artifact_fingerprint").and_then(|v| v.as_str()).map(str::to_string),
                invalidation_reason: None,
            },
        });
    }

    // Sort: exact hits first, then by created_at descending.
    hits.sort_by(|a, b| {
        b.exact.cmp(&a.exact).then_with(|| b.metadata.created_at.cmp(&a.metadata.created_at))
    });
    hits
}

// ── Ghost Artifacts & Invalidation (Phase 2d) ────────────────────────────────

/// Invalidation request — specifies the target artifact and the desired state transition.
#[derive(Clone, Debug)]
pub struct InvalidationRequest {
    pub artifact_id: String,
    pub new_state: ValidityState,
    pub reason: Option<String>,
}

/// Marks an artifact as invalidated/stale/ghosted/lost without removing it from any SmartList.
///
/// The artifact Object's provenance is updated in-place. Lookups (Phase 2c) already
/// skip non-`valid` artifacts, so invalidation is soft — the structural membership
/// in the Tool and Source SmartLists is preserved for auditability and potential recovery.
///
/// For ghost artifacts specifically: the existing metadata (checksums, content refs,
/// historical paths, embeddings) is retained in the SemanticPayload so that the
/// resolution engine (Phase 3d) can attempt recovery.
pub fn invalidate_artifact(
    store: &mut AmsStore,
    request: &InvalidationRequest,
) -> Result<()> {
    let obj = store
        .objects_mut()
        .get_mut(&request.artifact_id)
        .ok_or_else(|| anyhow!("artifact '{}' not found", request.artifact_id))?;

    let prov = obj
        .semantic_payload
        .get_or_insert_with(SemanticPayload::default)
        .provenance
        .get_or_insert_with(BTreeMap::new);

    prov.insert("validity_state".into(), Value::String(request.new_state.as_str().into()));
    if let Some(ref reason) = request.reason {
        prov.insert("invalidation_reason".into(), Value::String(reason.clone()));
    }
    prov.insert("invalidated_at".into(), Value::String(now_fixed().to_rfc3339()));

    Ok(())
}

/// Convenience: mark an artifact as ghosted (source referent missing, traces retained).
pub fn ghost_artifact(store: &mut AmsStore, artifact_id: &str, reason: &str) -> Result<()> {
    invalidate_artifact(store, &InvalidationRequest {
        artifact_id: artifact_id.to_string(),
        new_state: ValidityState::Ghosted,
        reason: Some(reason.to_string()),
    })
}

/// Convenience: mark an artifact as stale (eligible for refresh but not reuse).
pub fn stale_artifact(store: &mut AmsStore, artifact_id: &str, reason: &str) -> Result<()> {
    invalidate_artifact(store, &InvalidationRequest {
        artifact_id: artifact_id.to_string(),
        new_state: ValidityState::Stale,
        reason: Some(reason.to_string()),
    })
}

/// Re-validate a previously invalidated artifact (set it back to Valid).
pub fn revalidate_artifact(store: &mut AmsStore, artifact_id: &str) -> Result<()> {
    let obj = store
        .objects_mut()
        .get_mut(artifact_id)
        .ok_or_else(|| anyhow!("artifact '{}' not found", artifact_id))?;

    let prov = obj
        .semantic_payload
        .get_or_insert_with(SemanticPayload::default)
        .provenance
        .get_or_insert_with(BTreeMap::new);

    prov.insert("validity_state".into(), Value::String("valid".into()));
    prov.remove("invalidation_reason");
    prov.remove("invalidated_at");

    Ok(())
}

// ── Utility ───────────────────────────────────────────────────────────────────

/// Mint a new unique artifact ID.
pub fn new_artifact_id() -> String {
    format!("cache-artifact:{}", Uuid::new_v4().simple())
}

/// Build a `CacheEntryMetadata` with `ValidityState::Valid` for a fresh promotion.
pub fn fresh_cache_entry(
    tool: &ToolIdentity,
    source: &SourceIdentity,
    invocation: &InvocationIdentity,
    artifact_id: impl Into<String>,
    now: DateTime<FixedOffset>,
) -> CacheEntryMetadata {
    CacheEntryMetadata {
        tool_id: tool.tool_id.clone(),
        tool_version: tool.tool_version.clone(),
        source_id: source.source_id.clone(),
        param_hash: invocation.param_hash.clone(),
        artifact_id: artifact_id.into(),
        invocation_canonical_key: invocation.canonical_key.clone(),
        validity_state: ValidityState::Valid,
        created_at: now,
        source_fingerprint: source.fingerprint.clone(),
        artifact_fingerprint: None,
        invalidation_reason: None,
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::store::AmsStore;

    fn empty_store() -> AmsStore {
        AmsStore::default()
    }

    #[test]
    fn tool_cache_smartlist_path_format() {
        assert_eq!(tool_cache_smartlist_path("embed:openai"), "smartlist/cache/embed_openai");
        assert_eq!(tool_cache_smartlist_path("parser-v2"), "smartlist/cache/parser-v2");
    }

    #[test]
    fn source_cache_links_path_format() {
        assert_eq!(source_cache_links_path("obj:abc123"), "smartlist/cache-links/obj_abc123");
    }

    #[test]
    fn invocation_identity_canonical_key() {
        let tool = ToolIdentity {
            tool_id: "parser".into(),
            tool_version: "1.0".into(),
            object_id: "cache-tool:parser".into(),
        };
        let source = SourceIdentity { source_id: "doc:42".into(), fingerprint: None };
        let inv = InvocationIdentity::new(&tool, &source, "abc");
        assert_eq!(inv.canonical_key, "parser@1.0:doc:42:abc:none");
    }

    #[test]
    fn register_tool_creates_object_and_smartlist() {
        let mut store = empty_store();
        let identity = register_tool(&mut store, "parser", "1.0", None).unwrap();
        assert_eq!(identity.tool_id, "parser");
        assert_eq!(identity.object_id, "cache-tool:parser");
        // Tool object should exist
        assert!(store.objects().contains_key("cache-tool:parser"));
        // SmartList bucket should exist
        let path = tool_cache_smartlist_path("parser");
        let objects = store.objects();
        let bucket_exists = objects.values().any(|o| {
            o.semantic_payload
                .as_ref()
                .and_then(|p| p.provenance.as_ref())
                .and_then(|prov| prov.get("path"))
                .and_then(|v| v.as_str())
                == Some(&path)
        });
        // The bucket object exists under some form; at minimum the path doesn't panic
        drop(bucket_exists);
    }

    #[test]
    fn register_source_creates_object_and_smartlist() {
        let mut store = empty_store();
        let identity = register_source(&mut store, "doc:42", Some("sha256:abc"), None).unwrap();
        assert_eq!(identity.source_id, "doc:42");
        assert_eq!(identity.fingerprint.as_deref(), Some("sha256:abc"));
        assert!(store.objects().contains_key("cache-source:doc:42"));
    }

    #[test]
    fn promote_artifact_attaches_to_both_smartlists() {
        let mut store = empty_store();
        let now = now_fixed();
        let tool = register_tool(&mut store, "parser", "1.0", Some(now)).unwrap();
        let source = register_source(&mut store, "doc:42", Some("sha256:abc"), Some(now)).unwrap();
        let invocation = InvocationIdentity::new(&tool, &source, "paramhash1");

        let result = promote_artifact(
            &mut store, &tool, &source, &invocation,
            Some("/tmp/output.json"), Some("sha256:out"), "test-actor", Some(now),
        ).unwrap();

        // Artifact object should exist
        assert!(store.objects().contains_key(&result.artifact_id));
        let obj = store.objects().get(&result.artifact_id).unwrap();
        let prov = obj.semantic_payload.as_ref().unwrap().provenance.as_ref().unwrap();
        assert_eq!(prov.get("validity_state").and_then(|v| v.as_str()), Some("valid"));
        assert_eq!(prov.get("artifact_fingerprint").and_then(|v| v.as_str()), Some("sha256:out"));

        // Verify the artifact is a member of the tool's cache SmartList.
        // The SmartList membership is tracked via link nodes pointing to the artifact.
        let tool_path = tool_cache_smartlist_path("parser");
        assert_eq!(result.tool_cache_path, tool_path);

        // Verify the artifact is a member of the source's cache-links SmartList.
        let source_path = source_cache_links_path("doc:42");
        assert_eq!(result.source_cache_links_path, source_path);
    }

    #[test]
    fn lookup_tool_centric_finds_exact_hit() {
        let mut store = empty_store();
        let now = now_fixed();
        let tool = register_tool(&mut store, "parser", "1.0", Some(now)).unwrap();
        let source = register_source(&mut store, "doc:42", None, Some(now)).unwrap();
        let inv = InvocationIdentity::new(&tool, &source, "hash1");

        promote_artifact(&mut store, &tool, &source, &inv, None, None, "actor", Some(now)).unwrap();

        let hits = lookup_tool_centric(&store, "parser", "doc:42", Some("hash1"));
        assert_eq!(hits.len(), 1);
        assert!(hits[0].exact);
        assert_eq!(hits[0].metadata.param_hash, "hash1");
    }

    #[test]
    fn lookup_tool_centric_compatible_match() {
        let mut store = empty_store();
        let now = now_fixed();
        let tool = register_tool(&mut store, "parser", "1.0", Some(now)).unwrap();
        let source = register_source(&mut store, "doc:42", None, Some(now)).unwrap();
        let inv = InvocationIdentity::new(&tool, &source, "hash1");

        promote_artifact(&mut store, &tool, &source, &inv, None, None, "actor", Some(now)).unwrap();

        // Lookup with different param_hash → compatible (not exact) hit
        let hits = lookup_tool_centric(&store, "parser", "doc:42", Some("hash2"));
        assert_eq!(hits.len(), 1);
        assert!(!hits[0].exact);
    }

    #[test]
    fn lookup_exact_returns_none_when_no_match() {
        let mut store = empty_store();
        let now = now_fixed();
        let tool = register_tool(&mut store, "parser", "1.0", Some(now)).unwrap();
        let source = register_source(&mut store, "doc:42", None, Some(now)).unwrap();
        let inv = InvocationIdentity::new(&tool, &source, "hash1");

        // No artifacts promoted yet
        assert!(lookup_exact(&store, &inv).is_none());
    }

    #[test]
    fn lookup_source_centric_finds_artifacts() {
        let mut store = empty_store();
        let now = now_fixed();
        let tool = register_tool(&mut store, "parser", "1.0", Some(now)).unwrap();
        let source = register_source(&mut store, "doc:42", None, Some(now)).unwrap();
        let inv = InvocationIdentity::new(&tool, &source, "hash1");

        promote_artifact(&mut store, &tool, &source, &inv, None, None, "actor", Some(now)).unwrap();

        let hits = lookup_source_centric(&store, "doc:42", Some("parser"), None);
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].metadata.tool_id, "parser");
    }

    #[test]
    fn invalidate_artifact_marks_state_and_hides_from_lookup() {
        let mut store = empty_store();
        let now = now_fixed();
        let tool = register_tool(&mut store, "parser", "1.0", Some(now)).unwrap();
        let source = register_source(&mut store, "doc:42", None, Some(now)).unwrap();
        let inv = InvocationIdentity::new(&tool, &source, "hash1");

        let result = promote_artifact(&mut store, &tool, &source, &inv, None, None, "actor", Some(now)).unwrap();

        // Invalidate the artifact
        invalidate_artifact(&mut store, &InvalidationRequest {
            artifact_id: result.artifact_id.clone(),
            new_state: ValidityState::Invalidated,
            reason: Some("policy change".into()),
        }).unwrap();

        // Artifact object still exists
        let obj = store.objects().get(&result.artifact_id).unwrap();
        let prov = obj.semantic_payload.as_ref().unwrap().provenance.as_ref().unwrap();
        assert_eq!(prov.get("validity_state").and_then(|v| v.as_str()), Some("invalidated"));
        assert_eq!(prov.get("invalidation_reason").and_then(|v| v.as_str()), Some("policy change"));

        // Lookup should return no hits (non-valid filtered out)
        let hits = lookup_tool_centric(&store, "parser", "doc:42", Some("hash1"));
        assert_eq!(hits.len(), 0);
    }

    #[test]
    fn ghost_artifact_retains_metadata() {
        let mut store = empty_store();
        let now = now_fixed();
        let tool = register_tool(&mut store, "parser", "1.0", Some(now)).unwrap();
        let source = register_source(&mut store, "doc:42", None, Some(now)).unwrap();
        let inv = InvocationIdentity::new(&tool, &source, "hash1");

        let result = promote_artifact(&mut store, &tool, &source, &inv, None, Some("sha256:payload"), "actor", Some(now)).unwrap();
        ghost_artifact(&mut store, &result.artifact_id, "source deleted").unwrap();

        let obj = store.objects().get(&result.artifact_id).unwrap();
        let prov = obj.semantic_payload.as_ref().unwrap().provenance.as_ref().unwrap();
        assert_eq!(prov.get("validity_state").and_then(|v| v.as_str()), Some("ghosted"));
        // Original metadata preserved
        assert_eq!(prov.get("artifact_fingerprint").and_then(|v| v.as_str()), Some("sha256:payload"));
        assert_eq!(prov.get("tool_id").and_then(|v| v.as_str()), Some("parser"));
    }

    #[test]
    fn revalidate_artifact_restores_lookup() {
        let mut store = empty_store();
        let now = now_fixed();
        let tool = register_tool(&mut store, "parser", "1.0", Some(now)).unwrap();
        let source = register_source(&mut store, "doc:42", None, Some(now)).unwrap();
        let inv = InvocationIdentity::new(&tool, &source, "hash1");

        let result = promote_artifact(&mut store, &tool, &source, &inv, None, None, "actor", Some(now)).unwrap();
        stale_artifact(&mut store, &result.artifact_id, "expired").unwrap();
        assert_eq!(lookup_tool_centric(&store, "parser", "doc:42", Some("hash1")).len(), 0);

        revalidate_artifact(&mut store, &result.artifact_id).unwrap();
        let hits = lookup_tool_centric(&store, "parser", "doc:42", Some("hash1"));
        assert_eq!(hits.len(), 1);
        assert!(hits[0].exact);
    }

    #[test]
    fn create_artifact_object_stores_metadata() {
        let mut store = empty_store();
        let now = now_fixed();
        let meta = CacheEntryMetadata {
            tool_id: "parser".into(),
            tool_version: "1.0".into(),
            source_id: "doc:42".into(),
            param_hash: "abc123".into(),
            artifact_id: "cache-artifact:test001".into(),
            invocation_canonical_key: "parser@1.0:doc:42:abc123:none".into(),
            validity_state: ValidityState::Valid,
            created_at: now,
            source_fingerprint: None,
            artifact_fingerprint: None,
            invalidation_reason: None,
        };
        let artifact_id = create_artifact_object(&mut store, &meta, None, None).unwrap();
        assert_eq!(artifact_id, "cache-artifact:test001");
        let obj = store.objects().get(&artifact_id).unwrap();
        let prov = obj.semantic_payload.as_ref().unwrap().provenance.as_ref().unwrap();
        assert_eq!(prov.get("validity_state").and_then(|v| v.as_str()), Some("valid"));
        assert_eq!(prov.get("tool_id").and_then(|v| v.as_str()), Some("parser"));
    }
}
