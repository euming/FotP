//! Multi-scale Atlas abstraction — Layer 2.
//!
//! An **Atlas** is a coordinated family of nested SmartLists presenting the same
//! Objects at different levels of granularity (scales). Scale 0 is the coarsest
//! (highest-level) view; higher scale numbers present progressively finer grain.
//!
//! ## Storage
//!
//! Atlas definitions are stored as objects of kind `atlas_registry_entry` in the
//! AmsStore. The semantic payload's `provenance` map encodes:
//! - `atlas_name`  — unique atlas identifier
//! - `description` — human-readable description (optional)
//! - `scale_<N>`   — comma-separated list of SmartList bucket paths for scale level N
//!
//! ## APIs
//!
//! | Function               | Purpose                                          |
//! |------------------------|--------------------------------------------------|
//! | `atlas_define`         | Register or update a named atlas                 |
//! | `atlas_show`           | Describe a registered atlas and its scale levels |
//! | `atlas_list`           | Enumerate all registered atlases                 |
//! | `atlas_list_at_scale`  | List objects visible at a given scale            |
//! | `atlas_navigate`       | Coarse-to-fine navigation for one object         |

use anyhow::{bail, Result};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::model::SemanticPayload;
use crate::smartlist_write::get_bucket;
use crate::store::AmsStore;

// ── constants ─────────────────────────────────────────────────────────────────

pub const ATLAS_OBJECT_KIND: &str = "atlas_registry_entry";
const ATLAS_ID_PREFIX: &str = "atlas:registry:";

// ── public result types ───────────────────────────────────────────────────────

/// One registered scale level within an atlas.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtlasScaleLevel {
    /// 0 = coarsest; higher numbers = finer grain.
    pub scale: u32,
    /// SmartList bucket paths that form this scale level.
    pub bucket_paths: Vec<String>,
}

/// Full description of a registered atlas.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtlasInfo {
    /// Unique atlas name (slugified).
    pub atlas_name: String,
    /// Stable object ID inside the AmsStore.
    pub object_id: String,
    /// Optional human-readable description.
    pub description: Option<String>,
    /// Scale levels ordered from coarsest (0) to finest.
    pub scales: Vec<AtlasScaleLevel>,
}

/// One object entry returned by `atlas_list_at_scale`.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtlasScaleEntry {
    pub object_id: String,
    pub object_kind: String,
    /// The bucket path this entry was found in.
    pub bucket_path: String,
    /// Short summary from the object's SemanticPayload, if available.
    pub summary: Option<String>,
}

/// Navigation result: the same object viewed at each scale of an atlas.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtlasNavigationLevel {
    pub scale: u32,
    /// Whether this object (or a direct container of it) appears at this scale.
    pub visible: bool,
    /// Bucket paths at this scale that contain the object.
    pub containing_buckets: Vec<String>,
}

/// Full coarse-to-fine navigation for one object across an atlas.
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
pub struct AtlasNavigationResult {
    pub atlas_name: String,
    pub object_id: String,
    pub levels: Vec<AtlasNavigationLevel>,
}

// ── internal helpers ──────────────────────────────────────────────────────────

fn atlas_object_id(atlas_name: &str) -> String {
    format!("{}{}", ATLAS_ID_PREFIX, atlas_name)
}

/// Parse the `scale_<N>` provenance keys into a sorted list of AtlasScaleLevel.
fn parse_scales(prov: &crate::model::JsonMap) -> Vec<AtlasScaleLevel> {
    let mut scales: Vec<AtlasScaleLevel> = prov
        .iter()
        .filter_map(|(k, v)| {
            let n_str = k.strip_prefix("scale_")?;
            let scale: u32 = n_str.parse().ok()?;
            let paths_raw = v.as_str()?;
            let bucket_paths: Vec<String> = paths_raw
                .split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect();
            Some(AtlasScaleLevel { scale, bucket_paths })
        })
        .collect();
    scales.sort_by_key(|s| s.scale);
    scales
}

/// Collect an AtlasInfo from the store for a given object_id.
fn load_atlas_info(store: &AmsStore, object_id: &str) -> Option<AtlasInfo> {
    let obj = store.objects().get(object_id)?;
    if obj.object_kind != ATLAS_OBJECT_KIND {
        return None;
    }
    let prov = obj
        .semantic_payload
        .as_ref()
        .and_then(|sp| sp.provenance.as_ref())?;
    let atlas_name = prov
        .get("atlas_name")
        .and_then(|v| v.as_str())
        .unwrap_or(object_id)
        .to_string();
    let description = prov
        .get("description")
        .and_then(|v| v.as_str())
        .map(|s| s.to_string());
    let scales = parse_scales(prov);
    Some(AtlasInfo {
        atlas_name,
        object_id: object_id.to_string(),
        description,
        scales,
    })
}

/// Return object IDs that appear in a SmartList bucket (via its members container).
fn objects_in_bucket(store: &AmsStore, bucket_path: &str) -> Vec<String> {
    // The members container ID mirrors the convention in smartlist_write.
    let container_id = format!("smartlist-members:{bucket_path}");
    let container = match store.containers().get(&container_id) {
        Some(c) => c,
        None => return Vec::new(),
    };
    let mut ids = Vec::new();
    let mut cur = container.head_linknode_id.clone();
    let mut guard = 0usize;
    while let Some(ref ln_id) = cur.clone() {
        if guard >= 5000 {
            break;
        }
        guard += 1;
        let ln = match store.link_nodes().get(ln_id) {
            Some(ln) => ln,
            None => break,
        };
        ids.push(ln.object_id.clone());
        cur = ln.next_linknode_id.clone();
    }
    ids
}

// ── public API ────────────────────────────────────────────────────────────────

/// Register or update a named atlas with a set of scale levels.
///
/// `scale_levels` is a list of `(scale_index, bucket_paths)` pairs. Passing the
/// same `atlas_name` again replaces the existing definition entirely.
pub fn atlas_define(
    store: &mut AmsStore,
    atlas_name: &str,
    description: Option<&str>,
    scale_levels: &[(u32, Vec<String>)],
) -> Result<AtlasInfo> {
    let atlas_name = atlas_name.trim();
    if atlas_name.is_empty() {
        bail!("atlas_name is required");
    }
    if scale_levels.is_empty() {
        bail!("at least one scale level is required");
    }

    // Validate that all referenced bucket paths exist.
    for (scale, paths) in scale_levels {
        for path in paths {
            if get_bucket(store, path).is_none() {
                bail!(
                    "scale {scale}: bucket path '{}' does not exist; create it first with smartlist-create-bucket",
                    path
                );
            }
        }
    }

    let object_id = atlas_object_id(atlas_name);
    let now = crate::model::now_fixed();

    store
        .upsert_object(object_id.clone(), ATLAS_OBJECT_KIND.to_string(), None, None, Some(now))
        .map_err(|e| anyhow::anyhow!("{e}"))?;

    {
        let obj = store
            .objects_mut()
            .get_mut(&object_id)
            .ok_or_else(|| anyhow::anyhow!("failed to materialize atlas object '{object_id}'"))?;

        let sp = obj.semantic_payload.get_or_insert_with(SemanticPayload::default);
        sp.summary = Some(format!("Atlas: {atlas_name}"));
        sp.tags = Some(vec![ATLAS_OBJECT_KIND.to_string()]);

        let prov = sp.provenance.get_or_insert_with(Default::default);
        prov.insert("atlas_name".to_string(), Value::String(atlas_name.to_string()));
        if let Some(desc) = description {
            prov.insert("description".to_string(), Value::String(desc.to_string()));
        }
        // Remove existing scale_* keys before writing new ones.
        prov.retain(|k, _| !k.starts_with("scale_"));
        for (scale, paths) in scale_levels {
            let key = format!("scale_{scale}");
            prov.insert(key, Value::String(paths.join(",")));
        }
        prov.insert("updated_at".to_string(), Value::String(now.to_rfc3339()));

        obj.updated_at = now;
    }

    load_atlas_info(store, &object_id)
        .ok_or_else(|| anyhow::anyhow!("failed to read back atlas '{atlas_name}'"))
}

/// Retrieve a registered atlas by name.
pub fn atlas_show(store: &AmsStore, atlas_name: &str) -> Result<AtlasInfo> {
    let object_id = atlas_object_id(atlas_name);
    load_atlas_info(store, &object_id)
        .ok_or_else(|| anyhow::anyhow!("atlas '{atlas_name}' is not registered"))
}

/// List all registered atlases.
pub fn atlas_list(store: &AmsStore) -> Vec<AtlasInfo> {
    store
        .objects()
        .values()
        .filter(|o| o.object_kind == ATLAS_OBJECT_KIND)
        .filter_map(|o| load_atlas_info(store, &o.object_id))
        .collect()
}

/// List all objects visible at a given scale level of a named atlas.
///
/// Returns one entry per object, deduplicated across buckets at that scale.
pub fn atlas_list_at_scale(
    store: &AmsStore,
    atlas_name: &str,
    scale: u32,
) -> Result<Vec<AtlasScaleEntry>> {
    let info = atlas_show(store, atlas_name)?;

    let level = info
        .scales
        .iter()
        .find(|s| s.scale == scale)
        .ok_or_else(|| {
            anyhow::anyhow!(
                "atlas '{}' has no scale level {scale}; available: {:?}",
                atlas_name,
                info.scales.iter().map(|s| s.scale).collect::<Vec<_>>()
            )
        })?;

    let mut entries: Vec<AtlasScaleEntry> = Vec::new();
    let mut seen: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();

    for bucket_path in &level.bucket_paths {
        for object_id in objects_in_bucket(store, bucket_path) {
            if seen.contains(&object_id) {
                continue;
            }
            seen.insert(object_id.clone());
            let (object_kind, summary) = store
                .objects()
                .get(&object_id)
                .map(|o| {
                    let s = o
                        .semantic_payload
                        .as_ref()
                        .and_then(|sp| sp.summary.clone());
                    (o.object_kind.clone(), s)
                })
                .unwrap_or_else(|| ("?".to_string(), None));
            entries.push(AtlasScaleEntry {
                object_id,
                object_kind,
                bucket_path: bucket_path.clone(),
                summary,
            });
        }
    }

    Ok(entries)
}

/// Coarse-to-fine navigation: show at which scale levels an object appears in a
/// named atlas, from coarsest (scale 0) to finest.
pub fn atlas_navigate(
    store: &AmsStore,
    atlas_name: &str,
    object_id: &str,
) -> Result<AtlasNavigationResult> {
    // Resolve prefix if needed.
    let resolved = if store.objects().contains_key(object_id) {
        object_id.to_string()
    } else {
        let lower = object_id.to_lowercase();
        let matches: Vec<&str> = store
            .objects()
            .keys()
            .filter(|k| k.to_lowercase().starts_with(&lower))
            .map(String::as_str)
            .collect();
        match matches.len() {
            1 => matches[0].to_string(),
            0 => bail!("no object found for id '{object_id}'"),
            n => bail!("ambiguous prefix '{object_id}' matches {n} objects"),
        }
    };

    let info = atlas_show(store, atlas_name)?;

    let levels: Vec<AtlasNavigationLevel> = info
        .scales
        .iter()
        .map(|sl| {
            let containing_buckets: Vec<String> = sl
                .bucket_paths
                .iter()
                .filter(|bp| objects_in_bucket(store, bp).contains(&resolved))
                .cloned()
                .collect();
            let visible = !containing_buckets.is_empty();
            AtlasNavigationLevel {
                scale: sl.scale,
                visible,
                containing_buckets,
            }
        })
        .collect();

    Ok(AtlasNavigationResult {
        atlas_name: atlas_name.to_string(),
        object_id: resolved,
        levels,
    })
}

// ── text rendering helpers ────────────────────────────────────────────────────

/// Render an `AtlasInfo` as human-readable text.
pub fn render_atlas_info(info: &AtlasInfo) -> String {
    let mut out = String::new();
    out.push_str(&format!("=== atlas: {} ===\n", info.atlas_name));
    if let Some(ref desc) = info.description {
        out.push_str(&format!("description: {desc}\n"));
    }
    out.push_str(&format!("scales: {}\n", info.scales.len()));
    for sl in &info.scales {
        out.push_str(&format!("  scale {}: {}\n", sl.scale, sl.bucket_paths.join(", ")));
    }
    out
}

/// Render scale-level listing as text.
pub fn render_scale_listing(atlas_name: &str, scale: u32, entries: &[AtlasScaleEntry]) -> String {
    let mut out = String::new();
    out.push_str(&format!("=== atlas '{}' — scale {} ({} objects) ===\n", atlas_name, scale, entries.len()));
    for e in entries {
        let summary_str = e
            .summary
            .as_deref()
            .map(|s| {
                let truncated = if s.len() > 80 { &s[..80] } else { s };
                format!("  — {truncated}")
            })
            .unwrap_or_default();
        out.push_str(&format!("  [{}] {}{}\n", e.object_kind, e.object_id, summary_str));
    }
    out
}

/// Render a navigation result as text.
pub fn render_navigation(result: &AtlasNavigationResult) -> String {
    let mut out = String::new();
    out.push_str(&format!(
        "=== coarse-to-fine: {} in atlas '{}' ===\n",
        result.object_id, result.atlas_name
    ));
    for level in &result.levels {
        let marker = if level.visible { "✓" } else { "·" };
        if level.visible {
            out.push_str(&format!(
                "  {marker} scale {}: {}\n",
                level.scale,
                level.containing_buckets.join(", ")
            ));
        } else {
            out.push_str(&format!("  {marker} scale {}: (not visible)\n", level.scale));
        }
    }
    out
}

// ── unit tests ────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use crate::model::now_fixed;
    use crate::smartlist_write::create_bucket;
    use crate::store::AmsStore;

    fn fresh_store() -> AmsStore {
        AmsStore::new()
    }

    fn make_bucket(store: &mut AmsStore, path: &str) {
        create_bucket(store, path, false, "test", now_fixed()).unwrap();
    }

    #[test]
    fn define_and_show() {
        let mut store = fresh_store();
        make_bucket(&mut store, "smartlist/topics");
        make_bucket(&mut store, "smartlist/topics/memory");

        let info = atlas_define(
            &mut store,
            "knowledge",
            Some("test atlas"),
            &[
                (0, vec!["smartlist/topics".to_string()]),
                (1, vec!["smartlist/topics/memory".to_string()]),
            ],
        )
        .unwrap();

        assert_eq!(info.atlas_name, "knowledge");
        assert_eq!(info.scales.len(), 2);
        assert_eq!(info.scales[0].scale, 0);
        assert_eq!(info.scales[1].scale, 1);

        let shown = atlas_show(&store, "knowledge").unwrap();
        assert_eq!(shown.atlas_name, "knowledge");
        assert_eq!(shown.description, Some("test atlas".to_string()));
    }

    #[test]
    fn list_atlases() {
        let mut store = fresh_store();
        make_bucket(&mut store, "smartlist/a");
        make_bucket(&mut store, "smartlist/b");
        atlas_define(&mut store, "alpha", None, &[(0, vec!["smartlist/a".to_string()])]).unwrap();
        atlas_define(&mut store, "beta", None, &[(0, vec!["smartlist/b".to_string()])]).unwrap();

        let all = atlas_list(&store);
        let names: Vec<&str> = all.iter().map(|a| a.atlas_name.as_str()).collect();
        assert!(names.contains(&"alpha"));
        assert!(names.contains(&"beta"));
    }

    #[test]
    fn define_rejects_missing_bucket() {
        let mut store = fresh_store();
        let err = atlas_define(
            &mut store,
            "bad",
            None,
            &[(0, vec!["smartlist/nonexistent".to_string()])],
        )
        .unwrap_err();
        assert!(err.to_string().contains("does not exist"));
    }

    #[test]
    fn list_at_scale_returns_members() {
        let mut store = fresh_store();
        make_bucket(&mut store, "smartlist/coarse");
        make_bucket(&mut store, "smartlist/fine");
        make_bucket(&mut store, "smartlist/coarse/child");

        // Use a child bucket path as the member ref (bucket paths are valid member refs).
        crate::smartlist_write::attach_member(
            &mut store,
            "smartlist/coarse",
            "smartlist/coarse/child",
            "test",
            now_fixed(),
        )
        .unwrap();

        atlas_define(
            &mut store,
            "test",
            None,
            &[
                (0, vec!["smartlist/coarse".to_string()]),
                (1, vec!["smartlist/fine".to_string()]),
            ],
        )
        .unwrap();

        let entries = atlas_list_at_scale(&store, "test", 0).unwrap();
        assert_eq!(entries.len(), 1);
        assert_eq!(entries[0].bucket_path, "smartlist/coarse");

        let fine_entries = atlas_list_at_scale(&store, "test", 1).unwrap();
        assert_eq!(fine_entries.len(), 0);
    }

    #[test]
    fn navigate_shows_visibility() {
        let mut store = fresh_store();
        make_bucket(&mut store, "smartlist/coarse");
        make_bucket(&mut store, "smartlist/fine");
        make_bucket(&mut store, "smartlist/coarse/child");

        // Attach child bucket as a member of coarse.
        crate::smartlist_write::attach_member(
            &mut store,
            "smartlist/coarse",
            "smartlist/coarse/child",
            "test",
            now_fixed(),
        )
        .unwrap();

        // The child bucket object_id is what ends up in the members list.
        let child_bucket = crate::smartlist_write::get_bucket(&store, "smartlist/coarse/child").unwrap();
        let member_id = &child_bucket.object_id;

        atlas_define(
            &mut store,
            "nav",
            None,
            &[
                (0, vec!["smartlist/coarse".to_string()]),
                (1, vec!["smartlist/fine".to_string()]),
            ],
        )
        .unwrap();

        let nav = atlas_navigate(&store, "nav", member_id).unwrap();
        assert_eq!(nav.levels[0].visible, true);
        assert_eq!(nav.levels[1].visible, false);
    }
}
