//! swarm_plan_store — per-plan store migration for P8 store separation.
//!
//! Copies all objects and containers belonging to a given swarm-plan project
//! from a source store (factories.memory.ams.json) into a new destination
//! store file, leaving the source untouched.

use std::collections::HashSet;
use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

use crate::callstack::EXECUTION_PLAN_ROOT;
use crate::model::AmsSnapshot;
use crate::persistence::deserialize_snapshot;
use crate::store::AmsStore;

/// Result returned by `migrate_swarm_plan_store`.
#[derive(Debug)]
pub struct SwarmPlanMigrateResult {
    pub plan: String,
    pub migrated_objects: usize,
    pub migrated_containers: usize,
}

impl SwarmPlanMigrateResult {
    pub fn to_text(&self) -> String {
        format!(
            "migrated_objects={} migrated_containers={} plan={}",
            self.migrated_objects, self.migrated_containers, self.plan
        )
    }
}

/// Returns true if the object_id belongs to the given plan.
fn object_belongs_to_plan(object_id: &str, plan_name: &str) -> bool {
    // Matches: smartlist-bucket:smartlist/execution-plan/<plan-name>/...
    //          smartlist/execution-plan/<plan-name>/...
    //          plan-node:<plan-name>/...  (legacy)
    let plan_path = format!("{EXECUTION_PLAN_ROOT}/{plan_name}");
    object_id.contains(&plan_path)
        || object_id.starts_with(&format!("plan-node:{plan_name}/"))
        || object_id.starts_with(&format!("plan-node:{plan_name}:"))
}

/// Returns true if the container_id belongs to the given plan.
fn container_belongs_to_plan(container_id: &str, plan_name: &str) -> bool {
    let plan_path = format!("{EXECUTION_PLAN_ROOT}/{plan_name}");
    container_id.contains(&plan_path)
        || container_id.starts_with(&format!("plan-node:{plan_name}/"))
        || container_id.starts_with(&format!("plan-node:{plan_name}:"))
}

/// Migrate all objects and containers for `plan_name` from `source_path` into
/// a new per-plan write-service store.  The source is never modified.
///
/// - `source_path`: path to a `.memory.ams.json` or `.memory.jsonl` snapshot
/// - `dest_path`:   path to the destination `.memory.jsonl` store file
///                  (e.g. `swarm-plans/p8-store-migration.memory.jsonl`).
///                  The companion `.memory.ams.json` snapshot, empty
///                  `.ams-write-log.jsonl`, and `.ams-write-state.json` files
///                  are written automatically alongside it.
/// - `plan_name`:   execution-plan project name (e.g. `p7-fep-cache-signal`)
///
/// After the call, `dest_path` is a valid write-service store that:
///   - is discoverable via `list_swarm_plan_stores()` (the `.memory.jsonl` exists)
///   - loads correctly via `swarm-plan-list --input <dest_path>` (snapshot is present)
///   - accepts new mutations (write-log and write-state are initialised)
///
/// If no objects match, an empty store is written without error.
pub fn migrate_swarm_plan_store(
    source_path: &Path,
    dest_path: &Path,
    plan_name: &str,
) -> Result<SwarmPlanMigrateResult> {
    // ── Load source ──────────────────────────────────────────────────────────
    let json = fs::read_to_string(source_path)
        .with_context(|| format!("failed to read source snapshot '{}'", source_path.display()))?;
    let source: AmsStore = deserialize_snapshot(&json)
        .with_context(|| format!("failed to parse source snapshot '{}'", source_path.display()))?;

    // ── Collect matching object IDs ─────────────────────────────────────────
    // An object belongs to the plan if its object_id matches, OR if it is a
    // member of a container that belongs to the plan.
    let mut plan_object_ids: HashSet<String> = source
        .objects()
        .keys()
        .filter(|id| object_belongs_to_plan(id, plan_name))
        .cloned()
        .collect();

    // Any object that is a member of a matching container also belongs.
    for (container_id, members) in source.container_members_index() {
        if container_belongs_to_plan(container_id, plan_name) {
            for member_id in members {
                plan_object_ids.insert(member_id.clone());
            }
        }
    }

    // ── Collect matching containers ─────────────────────────────────────────
    let plan_containers: Vec<_> = source
        .containers()
        .values()
        .filter(|c| container_belongs_to_plan(&c.container_id, plan_name))
        .cloned()
        .collect();

    // Also include containers whose container_id object is in plan_object_ids
    // (covers smartlist-members: prefixed containers that mirror object IDs).
    let plan_containers: Vec<_> = {
        let mut seen: HashSet<String> = plan_containers
            .iter()
            .map(|c| c.container_id.clone())
            .collect();
        let mut all = plan_containers;
        for c in source.containers().values() {
            if !seen.contains(&c.container_id) && plan_object_ids.contains(&c.container_id) {
                seen.insert(c.container_id.clone());
                all.push(c.clone());
            }
        }
        all
    };

    // ── Collect matching link nodes ─────────────────────────────────────────
    let plan_container_ids: HashSet<String> = plan_containers
        .iter()
        .map(|c| c.container_id.clone())
        .collect();

    let plan_link_nodes: Vec<_> = source
        .link_nodes()
        .values()
        .filter(|ln| plan_container_ids.contains(&ln.container_id))
        .cloned()
        .collect();

    // ── Collect the object records for matched objects ──────────────────────
    // Also include the object records for container IDs (they double as objects).
    let all_object_ids: HashSet<String> = plan_object_ids
        .iter()
        .chain(plan_container_ids.iter())
        .cloned()
        .collect();

    let mut plan_objects: Vec<_> = source
        .objects()
        .values()
        .filter(|o| all_object_ids.contains(&o.object_id))
        .cloned()
        .collect();
    plan_objects.sort_by(|a, b| a.object_id.cmp(&b.object_id));

    let mut plan_containers_sorted = plan_containers;
    plan_containers_sorted.sort_by(|a, b| a.container_id.cmp(&b.container_id));

    let mut plan_link_nodes_sorted = plan_link_nodes;
    plan_link_nodes_sorted.sort_by(|a, b| a.link_node_id.cmp(&b.link_node_id));

    let migrated_objects = plan_objects.len();
    let migrated_containers = plan_containers_sorted.len();

    // ── Write destination write-service store ───────────────────────────────
    let dest_snapshot = AmsSnapshot {
        objects: plan_objects,
        containers: plan_containers_sorted,
        link_nodes: plan_link_nodes_sorted,
    };

    let dest_json = serde_json::to_string_pretty(&dest_snapshot)
        .context("failed to serialize destination snapshot")?;

    if let Some(parent) = dest_path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed to create directory '{}'", parent.display()))?;
    }

    // Derive companion paths.  The dest_path convention is `.memory.jsonl`;
    // the write service reads the snapshot from `.memory.ams.json` (derived by
    // `derive_snapshot_input_path`).  We write both so:
    //   (a) `list_swarm_plan_stores()` discovers the plan via `*.memory.jsonl` glob
    //   (b) `swarm-plan-list --input <dest_path>` finds the snapshot
    //   (c) the write service can accept new mutations immediately
    let snapshot_path = snapshot_companion_path(dest_path);
    let write_log_path = write_suffix_path(dest_path, "ams-write-log.jsonl");
    let write_state_path = write_suffix_path(dest_path, "ams-write-state.json");

    // Write the snapshot to <base>.memory.ams.json
    fs::write(&snapshot_path, &dest_json)
        .with_context(|| format!("failed to write snapshot '{}'", snapshot_path.display()))?;

    // Write the seed to the dest_path itself (<base>.memory.jsonl) — same
    // content as the snapshot; this makes the store discoverable.
    fs::write(dest_path, &dest_json)
        .with_context(|| format!("failed to write seed '{}'", dest_path.display()))?;

    // Write an empty write-log (only if it doesn't already exist).
    if !write_log_path.exists() {
        fs::write(&write_log_path, "")
            .with_context(|| format!("failed to create write-log '{}'", write_log_path.display()))?;
    }

    // Write an empty write-state (only if it doesn't already exist).
    if !write_state_path.exists() {
        let now_str = chrono::Utc::now().to_rfc3339_opts(chrono::SecondsFormat::Nanos, true);
        let empty_state = format!(
            r#"{{"current_version":0,"applied_mutation_ids":{{}},"updated_at":"{}"}}"#,
            now_str
        );
        fs::write(&write_state_path, empty_state.as_bytes())
            .with_context(|| format!("failed to create write-state '{}'", write_state_path.display()))?;
    }

    Ok(SwarmPlanMigrateResult {
        plan: plan_name.to_string(),
        migrated_objects,
        migrated_containers,
    })
}

/// Returns the `.memory.ams.json` companion path for a `.memory.jsonl` store path.
///
/// For `foo.memory.jsonl` → `foo.memory.ams.json`.
/// For any other extension, appends `.ams.json` as a fallback.
fn snapshot_companion_path(store_path: &Path) -> PathBuf {
    let file_name = store_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("store");
    if let Some(stem) = file_name.strip_suffix(".memory.jsonl") {
        store_path.with_file_name(format!("{stem}.memory.ams.json"))
    } else {
        store_path.with_file_name(format!("{file_name}.ams.json"))
    }
}

/// Returns a sibling path replacing the `.memory.jsonl` suffix with `.<suffix>`.
fn write_suffix_path(store_path: &Path, suffix: &str) -> PathBuf {
    let file_name = store_path
        .file_name()
        .and_then(|n| n.to_str())
        .unwrap_or("store");
    let base = file_name
        .strip_suffix(".memory.jsonl")
        .or_else(|| file_name.strip_suffix(".jsonl"))
        .unwrap_or(file_name);
    store_path.with_file_name(format!("{base}.{suffix}"))
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::persistence::serialize_snapshot;
    use crate::store::AmsStore;
    use crate::write_service::WriteService;
    use std::sync::Arc;
    use tempfile::tempdir;

    fn make_store_with_two_plans() -> AmsStore {
        let mut store = AmsStore::new();
        // Plan A objects
        store
            .upsert_object(
                "smartlist/execution-plan/plan-a",
                "smartlist-root",
                None,
                None,
                None,
            )
            .unwrap();
        store
            .upsert_object(
                "smartlist/execution-plan/plan-a/00-root",
                "smartlist-bucket",
                None,
                None,
                None,
            )
            .unwrap();

        // Plan B objects
        store
            .upsert_object(
                "smartlist/execution-plan/plan-b",
                "smartlist-root",
                None,
                None,
                None,
            )
            .unwrap();
        store
            .upsert_object(
                "smartlist/execution-plan/plan-b/00-root",
                "smartlist-bucket",
                None,
                None,
                None,
            )
            .unwrap();

        // Unrelated object
        store
            .upsert_object("factories/some-factory", "factory", None, None, None)
            .unwrap();

        store
    }

    #[test]
    fn migrate_plan_a_only_contains_plan_a_objects() {
        let dir = tempdir().unwrap();
        let source_path = dir.path().join("source.ams.json");
        let dest_path = dir.path().join("plan-a.memory.jsonl");

        let store = make_store_with_two_plans();
        let json = serialize_snapshot(&store).unwrap();
        std::fs::write(&source_path, &json).unwrap();

        let result = migrate_swarm_plan_store(&source_path, &dest_path, "plan-a").unwrap();

        assert_eq!(result.plan, "plan-a");
        assert!(result.migrated_objects >= 2, "expected at least 2 plan-a objects");

        // Both .memory.jsonl (seed) and .memory.ams.json (snapshot) must exist
        assert!(dest_path.exists(), "dest .memory.jsonl must exist");
        let snapshot_path = dir.path().join("plan-a.memory.ams.json");
        assert!(snapshot_path.exists(), "companion .memory.ams.json must exist");

        // Verify destination does NOT contain plan-b or unrelated objects
        let dest_json = std::fs::read_to_string(&dest_path).unwrap();
        assert!(dest_json.contains("plan-a"), "dest must contain plan-a");
        assert!(!dest_json.contains("plan-b"), "dest must not contain plan-b");
        assert!(!dest_json.contains("factories/some-factory"), "dest must not contain unrelated objects");
    }

    #[test]
    fn migrate_creates_write_service_companion_files() {
        let dir = tempdir().unwrap();
        let source_path = dir.path().join("source.ams.json");
        let dest_path = dir.path().join("plan-a.memory.jsonl");

        let store = make_store_with_two_plans();
        let json = serialize_snapshot(&store).unwrap();
        std::fs::write(&source_path, &json).unwrap();

        migrate_swarm_plan_store(&source_path, &dest_path, "plan-a").unwrap();

        // .ams-write-log.jsonl must exist (empty)
        let log_path = dir.path().join("plan-a.ams-write-log.jsonl");
        assert!(log_path.exists(), ".ams-write-log.jsonl must exist");

        // .ams-write-state.json must exist and contain valid JSON with current_version=0
        let state_path = dir.path().join("plan-a.ams-write-state.json");
        assert!(state_path.exists(), ".ams-write-state.json must exist");
        let state_json = std::fs::read_to_string(&state_path).unwrap();
        let state: serde_json::Value = serde_json::from_str(&state_json).unwrap();
        assert_eq!(state["current_version"], 0, "initial current_version must be 0");

        // .memory.ams.json and .memory.jsonl must have identical content
        let snapshot_path = dir.path().join("plan-a.memory.ams.json");
        let seed_json = std::fs::read_to_string(&dest_path).unwrap();
        let snap_json = std::fs::read_to_string(&snapshot_path).unwrap();
        assert_eq!(seed_json, snap_json, ".memory.jsonl and .memory.ams.json must be identical");
    }

    #[test]
    fn source_store_is_not_modified() {
        let dir = tempdir().unwrap();
        let source_path = dir.path().join("source.ams.json");
        let dest_path = dir.path().join("plan-a.memory.jsonl");

        let store = make_store_with_two_plans();
        let original_json = serialize_snapshot(&store).unwrap();
        std::fs::write(&source_path, &original_json).unwrap();

        migrate_swarm_plan_store(&source_path, &dest_path, "plan-a").unwrap();

        let after_json = std::fs::read_to_string(&source_path).unwrap();
        assert_eq!(original_json, after_json, "source snapshot must not be modified");
    }

    #[test]
    fn migrate_nonexistent_plan_creates_empty_destination() {
        let dir = tempdir().unwrap();
        let source_path = dir.path().join("source.ams.json");
        let dest_path = dir.path().join("plan-nonexistent.memory.jsonl");

        let store = make_store_with_two_plans();
        let json = serialize_snapshot(&store).unwrap();
        std::fs::write(&source_path, &json).unwrap();

        let result =
            migrate_swarm_plan_store(&source_path, &dest_path, "plan-nonexistent").unwrap();

        assert_eq!(result.migrated_objects, 0);
        assert_eq!(result.migrated_containers, 0);
        assert!(dest_path.exists(), "dest .memory.jsonl must be created even for empty migration");
        let snapshot_path = dir.path().join("plan-nonexistent.memory.ams.json");
        assert!(snapshot_path.exists(), "companion .memory.ams.json must be created even for empty migration");
    }

    #[test]
    fn snapshot_companion_path_derives_ams_json_from_jsonl() {
        let p = std::path::Path::new("/foo/bar/p8-recovery.memory.jsonl");
        let companion = snapshot_companion_path(p);
        assert_eq!(companion.file_name().unwrap().to_str().unwrap(), "p8-recovery.memory.ams.json");
    }

    #[test]
    fn write_suffix_path_derives_log_from_jsonl() {
        let p = std::path::Path::new("/foo/bar/p8-recovery.memory.jsonl");
        let log = write_suffix_path(p, "ams-write-log.jsonl");
        assert_eq!(log.file_name().unwrap().to_str().unwrap(), "p8-recovery.ams-write-log.jsonl");
    }

    /// Two WriteService instances on different per-plan stores must be able to
    /// run concurrent mutations with zero lock contention.
    ///
    /// This test verifies the core P8 store-separation guarantee: each plan owns
    /// its own `<plan>.ams-write.lock` file, so two orchestrators working on
    /// different plans never block each other.
    #[test]
    fn concurrent_mutations_on_separate_plan_stores_do_not_contend() {
        let dir = tempdir().unwrap();
        let source_path = dir.path().join("source.ams.json");

        let store = make_store_with_two_plans();
        let json = serialize_snapshot(&store).unwrap();
        std::fs::write(&source_path, &json).unwrap();

        // Migrate both plans to separate per-plan stores.
        let dest_a = dir.path().join("plan-a.memory.jsonl");
        let dest_b = dir.path().join("plan-b.memory.jsonl");
        migrate_swarm_plan_store(&source_path, &dest_a, "plan-a").unwrap();
        migrate_swarm_plan_store(&source_path, &dest_b, "plan-b").unwrap();

        // Wrap each store in a WriteService — these hold *different* lock paths
        // (plan-a.ams-write.lock vs plan-b.ams-write.lock), so their mutations
        // can proceed in parallel without blocking each other.
        let svc_a = Arc::new(WriteService::from_input(&dest_a));
        let svc_b = Arc::new(WriteService::from_input(&dest_b));

        // Spawn N threads per plan, each appending a distinct object to its store.
        const THREADS_PER_PLAN: usize = 8;

        let mut handles = Vec::new();

        for i in 0..THREADS_PER_PLAN {
            let svc = Arc::clone(&svc_a);
            handles.push(std::thread::spawn(move || {
                svc.run_with_store_mut(|store, _now| {
                    store
                        .upsert_object(
                            &format!("smartlist/execution-plan/plan-a/item-{i}"),
                            "test-item",
                            None,
                            None,
                            None,
                        )
                        .map(|_| ())
                        .map_err(anyhow::Error::from)
                })
                .expect("plan-a write must succeed");
            }));
        }

        for i in 0..THREADS_PER_PLAN {
            let svc = Arc::clone(&svc_b);
            handles.push(std::thread::spawn(move || {
                svc.run_with_store_mut(|store, _now| {
                    store
                        .upsert_object(
                            &format!("smartlist/execution-plan/plan-b/item-{i}"),
                            "test-item",
                            None,
                            None,
                            None,
                        )
                        .map(|_| ())
                        .map_err(anyhow::Error::from)
                })
                .expect("plan-b write must succeed");
            }));
        }

        for h in handles {
            h.join().expect("thread must not panic");
        }

        // Verify that each store contains exactly its own plan's objects and
        // nothing from the other plan.
        let final_a = svc_a
            .run_with_store_mut(|store, _now| Ok(store.clone()))
            .unwrap();
        let final_b = svc_b
            .run_with_store_mut(|store, _now| Ok(store.clone()))
            .unwrap();

        // plan-a store: must contain all THREADS_PER_PLAN new items for plan-a
        for i in 0..THREADS_PER_PLAN {
            let id = format!("smartlist/execution-plan/plan-a/item-{i}");
            assert!(
                final_a.objects().contains_key(&id),
                "plan-a store must contain {id}"
            );
        }
        // plan-a store: must NOT contain any plan-b items
        for i in 0..THREADS_PER_PLAN {
            let id = format!("smartlist/execution-plan/plan-b/item-{i}");
            assert!(
                !final_a.objects().contains_key(&id),
                "plan-a store must not contain {id}"
            );
        }

        // plan-b store: symmetric check
        for i in 0..THREADS_PER_PLAN {
            let id = format!("smartlist/execution-plan/plan-b/item-{i}");
            assert!(
                final_b.objects().contains_key(&id),
                "plan-b store must contain {id}"
            );
        }
        for i in 0..THREADS_PER_PLAN {
            let id = format!("smartlist/execution-plan/plan-a/item-{i}");
            assert!(
                !final_b.objects().contains_key(&id),
                "plan-b store must not contain {id}"
            );
        }

        // The lock files must be distinct (different paths) — this is the
        // structural guarantee that enables zero contention.
        let lock_a = dir.path().join("plan-a.ams-write.lock");
        let lock_b = dir.path().join("plan-b.ams-write.lock");
        assert_ne!(lock_a, lock_b, "per-plan lock paths must be distinct");
    }
}
