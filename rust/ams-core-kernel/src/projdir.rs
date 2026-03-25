//! P4 — Project Directory Atlas
//!
//! Implements GNUISNGNU Atlas-backed project directory indexing to replace the
//! old `build_proj_dir.py` / `proj_dir.db` SQLite approach.
//!
//! # P4-A1: File Object Ingestor
//!
//! `projdir_ingest` walks all git-tracked files in the repo, creates or updates
//! a file Object for each, and is fully cache-aware:
//!
//! - Fingerprint = (path, mtime_unix_secs, size_bytes)
//! - Cache key: tool=projdir-ingestor:v1, source=file:<normalized-path>
//! - On cache hit with validity=valid: skip (O(1) re-run)
//! - On miss/stale: read first 50 lines, upsert Object, promote cache artifact

use std::collections::{BTreeMap, BTreeSet};
use std::fs;
use std::io::{BufRead, BufReader};
use std::path::Path;
use std::process::Command;
use std::time::UNIX_EPOCH;

use anyhow::{anyhow, Context, Result};
use serde_json::Value;

use crate::atlas_multi_scale::atlas_define;
use crate::cache::{
    lookup_tool_centric, promote_artifact, register_tool, InvocationIdentity, SourceIdentity,
};
use crate::model::{now_fixed, SemanticPayload};
use crate::smartlist_write::{attach_member, create_bucket, get_bucket};
use crate::store::AmsStore;

// ── Constants ──────────────────────────────────────────────────────────────

pub const TOOL_ID: &str = "projdir-ingestor:v1";
pub const TOOL_VERSION: &str = "1";
pub const FILE_OBJECT_KIND: &str = "file";
pub const DIRECTORY_OBJECT_KIND: &str = "directory";
pub const ACTOR_ID: &str = "projdir-ingestor";
pub const ACTOR_BUILDER_ID: &str = "projdir-builder";
pub const PROJDIR_SMARTLIST_ROOT: &str = "smartlist/projdir";
pub const HEAD_LINES: usize = 50;

/// Extensions treated as binary — head content is not read for these.
const BINARY_EXTS: &[&str] = &[
    "exe", "dll", "pdb", "obj", "bin", "zip", "gz", "tar",
    "png", "jpg", "jpeg", "gif", "ico", "bmp",
    "woff", "woff2", "ttf", "eot",
    "db", "sqlite", "lock",
    "mp3", "mp4", "avi", "mov", "pdf",
    "nupkg", "snupkg",
];

// ── Helpers ────────────────────────────────────────────────────────────────

/// Normalize a repo-relative path to lowercase forward-slash form, stripping leading `./`.
pub fn normalize_file_path(path: &str) -> String {
    let p = path.replace('\\', "/");
    let p = p.strip_prefix("./").unwrap_or(&p);
    p.to_lowercase()
}

/// Return `true` if this extension should be treated as binary (no head content).
fn is_binary_ext(ext: &str) -> bool {
    BINARY_EXTS.contains(&ext.to_lowercase().as_str())
}

/// Compute the fingerprint string for a file: `"{mtime_secs}:{size}"`.
fn fingerprint(mtime_secs: u64, size: u64) -> String {
    format!("{}:{}", mtime_secs, size)
}

/// Get mtime (seconds since UNIX epoch) and size for a file.
fn file_meta(path: &Path) -> Option<(u64, u64)> {
    let meta = fs::metadata(path).ok()?;
    let mtime = meta
        .modified()
        .ok()?
        .duration_since(UNIX_EPOCH)
        .ok()?
        .as_secs();
    let size = meta.len();
    Some((mtime, size))
}

/// Read the first `n` lines of a UTF-8 file (lossy). Returns `None` for binary extensions
/// or on read errors.
fn read_head(path: &Path, n: usize) -> Option<String> {
    let ext = path
        .extension()
        .and_then(|e| e.to_str())
        .unwrap_or("");
    if is_binary_ext(ext) {
        return None;
    }
    let file = fs::File::open(path).ok()?;
    let reader = BufReader::new(file);
    let mut lines = Vec::with_capacity(n);
    for line in reader.lines().take(n) {
        match line {
            Ok(l) => lines.push(l),
            Err(_) => break,
        }
    }
    if lines.is_empty() {
        None
    } else {
        Some(lines.join("\n"))
    }
}

/// Run `git ls-files` in `repo_root` and return the list of relative paths.
fn git_ls_files(repo_root: &Path) -> Result<Vec<String>> {
    let output = Command::new("git")
        .arg("ls-files")
        .current_dir(repo_root)
        .output()
        .context("failed to run 'git ls-files'")?;
    if !output.status.success() {
        let stderr = String::from_utf8_lossy(&output.stderr);
        return Err(anyhow!("'git ls-files' failed: {}", stderr));
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    Ok(stdout
        .lines()
        .map(|l| l.to_string())
        .filter(|l| !l.is_empty())
        .collect())
}

// ── File Object Ingestor ───────────────────────────────────────────────────

/// Result of a `projdir_ingest` run.
#[derive(Debug, Default)]
pub struct IngestResult {
    pub ingested: usize,
    pub skipped: usize,
    pub total: usize,
}

/// Ingest all git-tracked files in `repo_root` into the AMS store.
///
/// For each file:
/// 1. Compute fingerprint = (mtime_secs, size).
/// 2. Check Layer 4 cache (`tool=projdir-ingestor:v1, source=file:<path>`).
/// 3. On cache hit with `validity=valid` **and matching fingerprint**: skip.
/// 4. On miss or stale: read first 50 lines, upsert Object, promote cache artifact.
pub fn projdir_ingest(store: &mut AmsStore, repo_root: &Path) -> Result<IngestResult> {
    let now = now_fixed();

    // Register the tool (idempotent).
    let tool = register_tool(store, TOOL_ID, TOOL_VERSION, Some(now))?;

    let files = git_ls_files(repo_root)?;
    let mut result = IngestResult {
        total: files.len(),
        ..Default::default()
    };

    for rel_path in &files {
        let norm = normalize_file_path(rel_path);
        let source_id = format!("file:{}", norm);
        let abs_path = repo_root.join(rel_path);

        // Get filesystem metadata.
        let (mtime, size) = match file_meta(&abs_path) {
            Some(m) => m,
            None => {
                // File listed by git but not stat-able (e.g. deleted); skip.
                result.skipped += 1;
                continue;
            }
        };
        let fp = fingerprint(mtime, size);

        // Check cache — look for a valid hit whose source_fingerprint matches.
        let hits = lookup_tool_centric(store, TOOL_ID, &source_id, None);
        let cache_hit = hits.iter().find(|h| {
            h.exact || h.metadata.source_fingerprint.as_deref() == Some(&fp)
        });
        if cache_hit.is_some() {
            result.skipped += 1;
            continue;
        }

        // Cache miss — read head content.
        let ext = Path::new(rel_path)
            .extension()
            .and_then(|e| e.to_str())
            .unwrap_or("")
            .to_string();
        let depth = norm.split('/').count();
        let head = read_head(&abs_path, HEAD_LINES);

        // Build provenance metadata.
        let mut prov: BTreeMap<String, Value> = BTreeMap::new();
        prov.insert("path".into(), Value::String(norm.clone()));
        prov.insert("ext".into(), Value::String(ext.clone()));
        prov.insert("size".into(), Value::Number(size.into()));
        prov.insert("mtime".into(), Value::Number(mtime.into()));
        prov.insert("depth".into(), Value::Number(depth.into()));

        let semantic = SemanticPayload {
            provenance: Some(prov),
            ..SemanticPayload::default()
        };

        let object_id = format!("file:{}", norm);
        store
            .upsert_object(
                object_id.clone(),
                FILE_OBJECT_KIND.to_string(),
                head.clone(),
                Some(semantic),
                Some(now),
            )
            .map_err(|e| anyhow!("failed to upsert file object '{}': {}", object_id, e))?;

        // Promote cache artifact with the fingerprint as source_fingerprint.
        let source = SourceIdentity {
            source_id: source_id.clone(),
            fingerprint: Some(fp.clone()),
        };
        let invocation = InvocationIdentity::new(&tool, &source, "none");

        // Artifact payload: JSON of {path, mtime, size, ext, depth}
        let artifact_payload = serde_json::json!({
            "path": norm,
            "mtime": mtime,
            "size": size,
            "ext": ext,
            "depth": depth,
        })
        .to_string();

        promote_artifact(
            store,
            &tool,
            &source,
            &invocation,
            Some(&artifact_payload),
            None,
            ACTOR_ID,
            Some(now),
        )
        .map_err(|e| anyhow!("failed to promote cache artifact for '{}': {}", source_id, e))?;

        result.ingested += 1;
    }

    Ok(result)
}

// ── Extension Stats ────────────────────────────────────────────────────────

pub const STATS_OBJECT_ID: &str = "projdir-stats:overview";
pub const STATS_BUCKET_PATH: &str = "smartlist/projdir-stats";
pub const EXT_STATS_ACTOR: &str = "projdir-stats";

/// Aggregated counts for a single file extension.
#[derive(Debug, Clone)]
pub struct ExtStat {
    pub ext: String,
    pub count: usize,
    pub total_size: u64,
}

/// Reads all file Objects (object_kind='file') from the store, aggregates counts
/// and total sizes by extension, writes a stats Object at `projdir-stats:overview`,
/// and updates a SmartList at `smartlist/projdir-stats` with one member per extension.
///
/// Returns the formatted stats table string (also printed by the CLI).
pub fn projdir_stats(store: &mut AmsStore) -> Result<Vec<ExtStat>> {
    let now = now_fixed();

    // Collect stats from all file Objects.
    let mut by_ext: BTreeMap<String, (usize, u64)> = BTreeMap::new();
    let file_objects: Vec<_> = store
        .objects()
        .values()
        .filter(|o| o.object_kind == FILE_OBJECT_KIND)
        .map(|o| {
            let ext = o
                .semantic_payload
                .as_ref()
                .and_then(|p| p.provenance.as_ref())
                .and_then(|p| p.get("ext"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let size = o
                .semantic_payload
                .as_ref()
                .and_then(|p| p.provenance.as_ref())
                .and_then(|p| p.get("size"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            (ext, size)
        })
        .collect();

    for (ext, size) in &file_objects {
        let entry = by_ext.entry(ext.clone()).or_insert((0, 0));
        entry.0 += 1;
        entry.1 += size;
    }

    // Sort by count descending, then ext ascending for stable ordering.
    let mut stats: Vec<ExtStat> = by_ext
        .into_iter()
        .map(|(ext, (count, total_size))| ExtStat { ext, count, total_size })
        .collect();
    stats.sort_by(|a, b| b.count.cmp(&a.count).then(a.ext.cmp(&b.ext)));

    // Build the formatted table (top-20 by count).
    let top20 = stats.iter().take(20);
    let mut table_lines = vec![
        format!("{:<20} {:>8} {:>14}", "ext", "count", "size (bytes)"),
        format!("{:-<20} {:->8} {:->14}", "", "", ""),
    ];
    for s in top20 {
        table_lines.push(format!("{:<20} {:>8} {:>14}", s.ext, s.count, s.total_size));
    }
    let table = table_lines.join("\n");

    // Write the stats overview Object.
    let semantic = SemanticPayload {
        summary: Some(table.clone()),
        ..SemanticPayload::default()
    };
    store
        .upsert_object(
            STATS_OBJECT_ID.to_string(),
            "projdir-stats".to_string(),
            Some(table.clone()),
            Some(semantic),
            Some(now),
        )
        .map_err(|e| anyhow!("failed to upsert stats object: {}", e))?;

    // Build SmartList at smartlist/projdir-stats with one Object per extension.
    // We clear and re-attach all members by recreating each ext-stats Object.
    // create_bucket is idempotent.
    create_bucket(store, STATS_BUCKET_PATH, true, EXT_STATS_ACTOR, now)
        .context("failed to create projdir-stats bucket")?;

    // Upsert one ext-stats Object per extension and attach to the bucket.
    // Process in count-descending order so the bucket reflects ranking.
    for s in &stats {
        let object_id = format!("ext-stats:{}", s.ext);
        let in_situ_ref = format!("{}: {} files, {} bytes", s.ext, s.count, s.total_size);
        let mut prov: BTreeMap<String, Value> = BTreeMap::new();
        prov.insert("ext".into(), Value::String(s.ext.clone()));
        prov.insert("count".into(), Value::Number(s.count.into()));
        prov.insert("total_size".into(), Value::Number(s.total_size.into()));
        let semantic = SemanticPayload {
            provenance: Some(prov),
            ..SemanticPayload::default()
        };
        store
            .upsert_object(
                object_id.clone(),
                "ext-stats".to_string(),
                Some(in_situ_ref),
                Some(semantic),
                Some(now),
            )
            .map_err(|e| anyhow!("failed to upsert ext-stats object '{}': {}", object_id, e))?;

        attach_member(store, STATS_BUCKET_PATH, &object_id, EXT_STATS_ACTOR, now)
            .context("failed to attach ext-stats member")?;
    }

    // Also attach the overview Object to the bucket.
    attach_member(store, STATS_BUCKET_PATH, STATS_OBJECT_ID, EXT_STATS_ACTOR, now)
        .context("failed to attach overview to projdir-stats bucket")?;

    Ok(stats)
}

/// Format the stats table for printing to stdout (matches `ams.bat proj-dir stats` layout).
pub fn format_stats_table(stats: &[ExtStat]) -> String {
    let mut lines = vec![
        format!("{:<20} {:>8} {:>14}", "ext", "count", "size (bytes)"),
        format!("{:-<20} {:->8} {:->14}", "", "", ""),
    ];
    for s in stats.iter().take(20) {
        lines.push(format!("{:<20} {:>8} {:>14}", s.ext, s.count, s.total_size));
    }
    lines.join("\n")
}

// ── Directory SmartList Builder ────────────────────────────────────────────

/// Result of a `projdir_build_dirs` run.
#[derive(Debug, Default)]
pub struct BuildDirsResult {
    pub dirs_created: usize,
    pub files_attached: usize,
}

/// Build the SmartList directory hierarchy from existing file Objects in the store.
///
/// For each unique directory derived from file Object IDs:
/// 1. Create a SmartList bucket at `smartlist/projdir/<dir-path>` (idempotent).
/// 2. Create a directory Object with `object_kind='directory'`, `object_id='dir:<path>'`.
/// 3. Attach each file Object to its parent directory SmartList.
/// 4. Attach each subdirectory Object to its parent directory SmartList.
pub fn projdir_build_dirs(store: &mut AmsStore) -> Result<BuildDirsResult> {
    let now = now_fixed();

    // Collect all file object IDs and extract their normalized paths.
    // object_id format: "file:<normalized-path>"
    let file_paths: Vec<String> = store
        .objects()
        .iter()
        .filter(|(_, obj)| obj.object_kind == FILE_OBJECT_KIND)
        .filter_map(|(id, _)| id.strip_prefix("file:").map(|p| p.to_string()))
        .collect();

    // Build the set of all unique directory paths needed.
    let mut all_dirs: BTreeSet<String> = BTreeSet::new();
    for path in &file_paths {
        let mut parts: Vec<&str> = path.split('/').collect();
        // Remove the filename (last component); keep directory ancestors.
        parts.pop();
        let mut dir_accum = String::new();
        for part in &parts {
            if !dir_accum.is_empty() {
                dir_accum.push('/');
            }
            dir_accum.push_str(part);
            all_dirs.insert(dir_accum.clone());
        }
        // Files in the repo root have no directory component — use "" to mean root.
        if parts.is_empty() {
            all_dirs.insert(String::new());
        }
    }

    let mut result = BuildDirsResult::default();

    // Create SmartList buckets and directory Objects for every directory.
    for dir in &all_dirs {
        let bucket_path = if dir.is_empty() {
            PROJDIR_SMARTLIST_ROOT.to_string()
        } else {
            format!("{}/{}", PROJDIR_SMARTLIST_ROOT, dir)
        };
        create_bucket(store, &bucket_path, true, ACTOR_BUILDER_ID, now)
            .map_err(|e| anyhow!("failed to create bucket '{}': {}", bucket_path, e))?;

        // Create directory Object (idempotent via upsert_object).
        let dir_name = dir.rsplit('/').next().unwrap_or(dir.as_str());
        let dir_object_id = if dir.is_empty() {
            "dir:.".to_string()
        } else {
            format!("dir:{}", dir)
        };
        store
            .upsert_object(
                dir_object_id,
                DIRECTORY_OBJECT_KIND.to_string(),
                Some(dir_name.to_string()),
                None,
                Some(now),
            )
            .map_err(|e| anyhow!("failed to upsert directory object '{}': {}", dir, e))?;

        result.dirs_created += 1;
    }

    // Attach file Objects to their parent directory SmartList.
    for path in &file_paths {
        let mut parts: Vec<&str> = path.split('/').collect();
        parts.pop(); // remove filename
        let parent_dir: String = parts.join("/");
        let bucket_path = if parent_dir.is_empty() {
            PROJDIR_SMARTLIST_ROOT.to_string()
        } else {
            format!("{}/{}", PROJDIR_SMARTLIST_ROOT, parent_dir)
        };
        let file_object_id = format!("file:{}", path);
        attach_member(store, &bucket_path, &file_object_id, ACTOR_BUILDER_ID, now)
            .map_err(|e| anyhow!("failed to attach '{}' to '{}': {}", file_object_id, bucket_path, e))?;
        result.files_attached += 1;
    }

    // Attach subdirectory Objects to their parent directory SmartList.
    for dir in &all_dirs {
        if dir.is_empty() {
            continue; // root has no parent
        }
        let mut parts: Vec<&str> = dir.split('/').collect();
        parts.pop(); // remove this dir's name to get parent
        let parent_dir: String = parts.join("/");
        let parent_bucket = if parent_dir.is_empty() {
            PROJDIR_SMARTLIST_ROOT.to_string()
        } else {
            format!("{}/{}", PROJDIR_SMARTLIST_ROOT, parent_dir)
        };
        let dir_object_id = format!("dir:{}", dir);
        attach_member(store, &parent_bucket, &dir_object_id, ACTOR_BUILDER_ID, now)
            .map_err(|e| anyhow!("failed to attach dir '{}' to '{}': {}", dir_object_id, parent_bucket, e))?;
    }

    Ok(result)
}

// ── P4-B3: Scale-2 File Pages ─────────────────────────────────────────────

pub const PROJDIR_FILE_PAGES_ROOT: &str = "smartlist/projdir-file";
pub const FILE_PAGES_ACTOR: &str = "projdir-file-pages";

/// Result of a `projdir_build_file_pages` run.
#[derive(Debug, Default)]
pub struct BuildFilePagesResult {
    /// Number of per-file SmartList buckets created or confirmed.
    pub pages_created: usize,
    /// Number of file objects attached to their page bucket.
    pub files_attached: usize,
}

/// Build per-file SmartList "page" buckets from existing file Objects in the store.
///
/// For each `file:<path>` Object in the store:
/// 1. Create a SmartList bucket at `smartlist/projdir-file/<path>` (idempotent).
/// 2. Attach the file Object as the sole member of that bucket.
///
/// This is the data foundation for Atlas scale 2 (individual file pages).
pub fn projdir_build_file_pages(store: &mut AmsStore) -> Result<BuildFilePagesResult> {
    let now = now_fixed();

    // Collect all file object IDs (format: "file:<path>").
    let file_paths: Vec<String> = store
        .objects()
        .keys()
        .filter(|k| k.starts_with("file:"))
        .filter_map(|k| k.strip_prefix("file:").map(|p| p.to_string()))
        .collect();

    let mut result = BuildFilePagesResult::default();

    for path in &file_paths {
        let bucket_path = format!("{}/{}", PROJDIR_FILE_PAGES_ROOT, path);
        let object_id = format!("file:{}", path);

        create_bucket(store, &bucket_path, true, FILE_PAGES_ACTOR, now)
            .map_err(|e| anyhow!("failed to create file-page bucket '{}': {}", bucket_path, e))?;
        result.pages_created += 1;

        attach_member(store, &bucket_path, &object_id, FILE_PAGES_ACTOR, now)
            .map_err(|e| anyhow!("failed to attach '{}' to '{}': {}", object_id, bucket_path, e))?;
        result.files_attached += 1;
    }

    Ok(result)
}

// ── P4-B1: Scale-0 Atlas Root ─────────────────────────────────────────────

pub const PROJDIR_ATLAS_NAME: &str = "projdir";
pub const PROJDIR_ATLAS_DESCRIPTION: &str =
    "Project directory Atlas (scale 0=repo overview, scale 1=directories, scale 2=files)";

/// Result of `projdir_register_atlas`.
#[derive(Debug)]
pub struct AtlasRegistrationResult {
    /// Atlas name that was registered.
    pub atlas_name: String,
    /// Number of scale levels registered.
    pub scale_count: usize,
    /// SmartList bucket paths enrolled at scale 0.
    pub scale0_buckets: Vec<String>,
    /// SmartList bucket paths enrolled at scale 1.
    pub scale1_buckets: Vec<String>,
    /// SmartList bucket paths enrolled at scale 2.
    pub scale2_buckets: Vec<String>,
}

/// Register (or refresh) the `projdir` Atlas with three scale levels.
///
/// # Scale levels
///
/// | Scale | Bucket(s)                               | Granularity              |
/// |-------|-----------------------------------------|--------------------------|
/// | 0     | `smartlist/projdir`                     | Repo overview (root)     |
/// | 1     | `smartlist/projdir/<top-level-dir>`     | Top-level directories    |
/// | 2     | `smartlist/projdir-file/<path>` (each)  | Individual file pages    |
///
/// Scale 0 is the root SmartList created by `projdir_build_dirs`, which contains
/// top-level directory Objects and any root-level file Objects.
///
/// Scale 1 includes only the *direct children* of the root (depth == 1), giving a
/// mid-resolution view of the repository's top-level layout.
///
/// Scale 2 is built from per-file SmartList buckets (one bucket per file Object),
/// created by calling `projdir_build_file_pages` internally. Each file is
/// individually reachable at this finest-grain scale.
///
/// Prerequisite: `projdir_ingest` and `projdir_build_dirs` must have been run first
/// so that `smartlist/projdir` and its sub-buckets exist in the store.
pub fn projdir_register_atlas(store: &mut AmsStore) -> Result<AtlasRegistrationResult> {
    // Collect all smartlist/projdir/* bucket paths that are currently in the store.
    let all_projdir_buckets: Vec<String> = {
        let prefix = format!("{}/", PROJDIR_SMARTLIST_ROOT); // "smartlist/projdir/"
        let mut paths: Vec<String> = store
            .objects()
            .keys()
            .filter_map(|k| k.strip_prefix("smartlist-bucket:").map(|s| s.to_string()))
            .filter(|path| path == PROJDIR_SMARTLIST_ROOT || path.starts_with(&prefix))
            .collect();
        paths.sort();
        paths
    };

    // Scale 0: just the root bucket.
    let scale0_buckets = if all_projdir_buckets.contains(&PROJDIR_SMARTLIST_ROOT.to_string()) {
        vec![PROJDIR_SMARTLIST_ROOT.to_string()]
    } else {
        // Root doesn't exist yet — caller should run projdir_build_dirs first.
        return Err(anyhow!(
            "smartlist/projdir bucket not found; run projdir-build-dirs first"
        ));
    };

    // Scale 1: direct children of the root (single path component after "smartlist/projdir/").
    let scale1_buckets: Vec<String> = all_projdir_buckets
        .iter()
        .filter(|p| {
            if let Some(suffix) = p.strip_prefix(&format!("{}/", PROJDIR_SMARTLIST_ROOT)) {
                // Direct child: suffix contains no additional '/'
                !suffix.contains('/')
            } else {
                false
            }
        })
        .cloned()
        .collect();

    // Scale 2: per-file SmartList pages (one bucket per file Object).
    // Build file-page buckets first (idempotent), then collect them.
    projdir_build_file_pages(store)
        .context("failed to build file-page buckets for scale 2")?;

    let file_pages_prefix = format!("{}/", PROJDIR_FILE_PAGES_ROOT);
    let mut scale2_buckets: Vec<String> = store
        .objects()
        .keys()
        .filter_map(|k| k.strip_prefix("smartlist-bucket:").map(|s| s.to_string()))
        .filter(|path| path.starts_with(&file_pages_prefix))
        .collect();
    scale2_buckets.sort();

    // Validate that each bucket we reference actually exists.
    // (get_bucket returns None if not present; atlas_define will also validate.)
    for path in scale0_buckets
        .iter()
        .chain(scale1_buckets.iter())
        .chain(scale2_buckets.iter())
    {
        if get_bucket(store, path).is_none() {
            return Err(anyhow!("expected bucket '{}' not found in store", path));
        }
    }

    // Build scale_levels vector — only include non-empty scales.
    let mut scale_levels: Vec<(u32, Vec<String>)> = vec![(0, scale0_buckets.clone())];
    if !scale1_buckets.is_empty() {
        scale_levels.push((1, scale1_buckets.clone()));
    }
    if !scale2_buckets.is_empty() {
        scale_levels.push((2, scale2_buckets.clone()));
    }

    let info = atlas_define(
        store,
        PROJDIR_ATLAS_NAME,
        Some(PROJDIR_ATLAS_DESCRIPTION),
        &scale_levels,
    )?;

    Ok(AtlasRegistrationResult {
        atlas_name: info.atlas_name,
        scale_count: info.scales.len(),
        scale0_buckets,
        scale1_buckets,
        scale2_buckets,
    })
}

// ── P4-B3: projdir-doc ────────────────────────────────────────────────────

/// Result of a `projdir_doc` lookup.
#[derive(Debug)]
pub struct ProjdirDocResult {
    /// Normalized file path (e.g. `src/lib.rs`).
    pub path: String,
    /// Head content from the file Object's `in_situ_ref`, or `None` if the file
    /// has no stored content (binary file or not yet ingested).
    pub content: Option<String>,
}

/// Look up a file Object by relative path and return its stored head content.
///
/// `rel_path` is normalized via [`normalize_file_path`] before lookup.
/// Returns an error if the object does not exist in the store.
///
/// This replaces `ams.bat proj-dir doc <path>`.
pub fn projdir_doc(store: &AmsStore, rel_path: &str) -> Result<ProjdirDocResult> {
    let norm = normalize_file_path(rel_path);
    let object_id = format!("file:{}", norm);
    let obj = store
        .objects()
        .get(&object_id)
        .ok_or_else(|| anyhow!("file not found in projdir index: '{}' (looked up as '{}')", rel_path, object_id))?;
    Ok(ProjdirDocResult {
        path: norm,
        content: obj.in_situ_ref.clone(),
    })
}

// ── P4-B2: Scale-1 Directory Tree Renderer ────────────────────────────────

/// Result of a `projdir_tree` call.
#[derive(Debug, Default)]
pub struct TreeResult {
    /// Lines of the indented tree, ready to join with '\n'.
    pub lines: Vec<String>,
}

/// List the object IDs that are members of a SmartList bucket.
///
/// Returns IDs in list order (head → tail). Returns an empty vec if the
/// bucket or its members container does not exist.
fn bucket_member_ids(store: &AmsStore, bucket_path: &str) -> Vec<String> {
    let container_id = format!("smartlist-members:{}", bucket_path);
    store
        .iterate_forward(&container_id)
        .into_iter()
        .map(|ln| ln.object_id.clone())
        .collect()
}

/// Recursively render the directory tree rooted at `bucket_path`.
///
/// `current_depth` starts at 1 for the first level of children; rendering
/// stops when `current_depth > max_depth`.
fn render_tree(
    store: &AmsStore,
    bucket_path: &str,
    current_depth: usize,
    max_depth: usize,
    indent: &str,
    lines: &mut Vec<String>,
) {
    if current_depth > max_depth {
        return;
    }

    let members = bucket_member_ids(store, bucket_path);

    // Partition into directories and files so dirs are printed first.
    let mut dirs: Vec<(String, String)> = Vec::new(); // (object_id, sub_bucket_path)
    let mut files: Vec<String> = Vec::new();

    for obj_id in &members {
        if let Some(obj) = store.objects().get(obj_id) {
            match obj.object_kind.as_str() {
                kind if kind == DIRECTORY_OBJECT_KIND => {
                    if let Some(dir_path) = obj_id.strip_prefix("dir:") {
                        let sub_bucket = if dir_path == "." {
                            PROJDIR_SMARTLIST_ROOT.to_string()
                        } else {
                            format!("{}/{}", PROJDIR_SMARTLIST_ROOT, dir_path)
                        };
                        dirs.push((obj_id.clone(), sub_bucket));
                    }
                }
                kind if kind == FILE_OBJECT_KIND => {
                    files.push(obj_id.clone());
                }
                _ => {}
            }
        }
    }

    // Directories first, alphabetical.
    dirs.sort_by(|a, b| a.0.cmp(&b.0));
    for (obj_id, sub_bucket) in &dirs {
        let dir_path = obj_id.strip_prefix("dir:").unwrap_or(obj_id.as_str());
        let base = dir_path.rsplit('/').next().unwrap_or(dir_path);
        lines.push(format!("{}{}/", indent, base));
        let child_indent = format!("{}  ", indent);
        render_tree(store, sub_bucket, current_depth + 1, max_depth, &child_indent, lines);
    }

    // Files after dirs, alphabetical.
    files.sort();
    for obj_id in &files {
        if let Some(obj) = store.objects().get(obj_id) {
            let file_path = obj_id.strip_prefix("file:").unwrap_or(obj_id.as_str());
            let base = file_path.rsplit('/').next().unwrap_or(file_path);
            let size = obj
                .semantic_payload
                .as_ref()
                .and_then(|sp| sp.provenance.as_ref())
                .and_then(|p| p.get("size"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            lines.push(format!("{}{} [{}]", indent, base, size));
        }
    }
}

/// Render a directory tree from the projdir Atlas SmartLists.
///
/// # Arguments
///
/// * `path` — repo-relative directory to start from (e.g. `"scripts"`). Pass
///   `None` or `Some(".")` to start from the repo root.
/// * `max_depth` — maximum recursion depth. `1` shows only immediate children;
///   `3` (default) shows three levels.
///
/// The output format matches `ams.bat proj-dir tree`: an indented tree where
/// directories are labelled `<name>/` and files are labelled `<name> [<size>]`.
pub fn projdir_tree(store: &AmsStore, path: Option<&str>, max_depth: usize) -> TreeResult {
    let (root_label, bucket_path) = match path {
        None | Some("") | Some(".") => (
            ".".to_string(),
            PROJDIR_SMARTLIST_ROOT.to_string(),
        ),
        Some(p) => {
            let norm = normalize_file_path(p);
            (norm.clone(), format!("{}/{}", PROJDIR_SMARTLIST_ROOT, norm))
        }
    };

    let mut lines = vec![format!("{}/", root_label)];
    render_tree(store, &bucket_path, 1, max_depth, "  ", &mut lines);
    TreeResult { lines }
}

/// Format a `TreeResult` as a single string (lines joined by `\n`).
pub fn format_tree(result: &TreeResult) -> String {
    result.lines.join("\n")
}

// ── P4-D2: Compact Context Dump ───────────────────────────────────────────

/// Result of a `projdir_context` call.
#[derive(Debug, Default)]
pub struct ContextResult {
    /// Indented tree lines (same format as `projdir_tree`).
    pub tree_lines: Vec<String>,
    /// Extension statistics (same format as `projdir_stats`).
    pub stats: Vec<ExtStat>,
    /// Repo-relative paths of key markdown docs (depth <= 2).
    pub key_docs: Vec<String>,
}

/// Produce a compact onboarding context dump.
///
/// Combines:
/// 1. Directory tree up to `max_depth` levels.
/// 2. Extension file stats (top 15 by count).
/// 3. Markdown docs at depth ≤ 2.
///
/// Replaces `ams.bat proj-dir context`.
pub fn projdir_context(store: &AmsStore, max_depth: usize) -> ContextResult {
    // 1. Tree
    let tree = projdir_tree(store, None, max_depth);

    // 2. Stats
    let stats: Vec<ExtStat> = {
        let mut by_ext: BTreeMap<String, (usize, u64)> = BTreeMap::new();
        for obj in store.objects().values() {
            if obj.object_kind != FILE_OBJECT_KIND {
                continue;
            }
            let ext = obj
                .semantic_payload
                .as_ref()
                .and_then(|p| p.provenance.as_ref())
                .and_then(|p| p.get("ext"))
                .and_then(|v| v.as_str())
                .unwrap_or("")
                .to_string();
            let size = obj
                .semantic_payload
                .as_ref()
                .and_then(|p| p.provenance.as_ref())
                .and_then(|p| p.get("size"))
                .and_then(|v| v.as_u64())
                .unwrap_or(0);
            let entry = by_ext.entry(ext).or_insert((0, 0));
            entry.0 += 1;
            entry.1 += size;
        }
        let mut v: Vec<ExtStat> = by_ext
            .into_iter()
            .map(|(ext, (count, total_size))| ExtStat { ext, count, total_size })
            .collect();
        v.sort_by(|a, b| b.count.cmp(&a.count).then(a.ext.cmp(&b.ext)));
        v.truncate(15);
        v
    };

    // 3. Key docs: markdown files at depth <= 2
    let mut key_docs: Vec<String> = store
        .objects()
        .values()
        .filter(|obj| {
            if obj.object_kind != FILE_OBJECT_KIND {
                return false;
            }
            let prov = obj
                .semantic_payload
                .as_ref()
                .and_then(|p| p.provenance.as_ref());
            let ext = prov
                .and_then(|p| p.get("ext"))
                .and_then(|v| v.as_str())
                .unwrap_or("");
            let depth = prov
                .and_then(|p| p.get("depth"))
                .and_then(|v| v.as_u64())
                .unwrap_or(u64::MAX);
            ext == "md" && depth <= 2
        })
        .filter_map(|obj| {
            obj.object_id.strip_prefix("file:").map(|p| p.to_string())
        })
        .collect();
    key_docs.sort();

    ContextResult {
        tree_lines: tree.lines,
        stats,
        key_docs,
    }
}

/// Format a `ContextResult` as a multi-section string matching `ams.bat proj-dir context`.
pub fn format_context(result: &ContextResult, max_depth: usize) -> String {
    let total_files: usize = result.stats.iter().map(|s| s.count).sum();
    let mut lines = Vec::new();

    // Section 1: tree
    lines.push(format!("## Project Tree (depth <= {})", max_depth));
    lines.push(String::new());
    lines.extend(result.tree_lines.iter().cloned());
    lines.push(String::new());

    // Section 2: stats
    lines.push(format!("## File Stats ({} files)", total_files));
    lines.push(String::new());
    for s in &result.stats {
        lines.push(format!("  .{}: {}", s.ext, s.count));
    }
    lines.push(String::new());

    // Section 3: key docs
    if !result.key_docs.is_empty() {
        lines.push("## Key Docs".to_string());
        lines.push(String::new());
        for doc in &result.key_docs {
            lines.push(format!("  {}", doc));
        }
        lines.push(String::new());
    }

    lines.join("\n")
}

// ── P4-C1: Projdir Search ──────────────────────────────────────────────────

/// A single result entry from `projdir_search`.
#[derive(Debug)]
pub struct SearchHit {
    /// Normalized path of the matched file or directory.
    pub path: String,
    /// The kind of object (`file` or `directory`).
    pub object_kind: String,
    /// Relevance score (higher is more relevant).
    pub score: i32,
    /// Optional snippet of matching head content (up to 5 lines containing the term).
    pub snippet: Option<String>,
}

/// Result of a `projdir_search` call.
#[derive(Debug, Default)]
pub struct SearchResult {
    /// Ranked list of matching objects (up to 20, ordered by score descending).
    pub hits: Vec<SearchHit>,
    /// Query terms used.
    pub query: Vec<String>,
}

/// Perform keyword search over file and directory Objects in the projdir Atlas.
///
/// Scoring per object per term:
/// - exact path match (path == term): +10
/// - path contains term: +5
/// - extension equals term: +1
/// - head content contains term: +2
///
/// Returns up to 20 results ordered by total score descending.
pub fn projdir_search(store: &AmsStore, query: &[&str]) -> SearchResult {
    let terms: Vec<String> = query.iter().map(|t| t.to_lowercase()).collect();

    let mut hits: Vec<SearchHit> = store
        .objects()
        .iter()
        .filter_map(|(obj_id, obj)| {
            let kind = obj.object_kind.as_str();
            if kind != FILE_OBJECT_KIND && kind != DIRECTORY_OBJECT_KIND {
                return None;
            }

            let path = obj_id
                .strip_prefix("file:")
                .or_else(|| obj_id.strip_prefix("dir:"))
                .unwrap_or(obj_id.as_str())
                .to_string();
            let path_lower = path.to_lowercase();

            let ext = path_lower.rsplit('.').next().unwrap_or("").to_string();
            let content_lower = obj
                .in_situ_ref
                .as_deref()
                .unwrap_or("")
                .to_lowercase();

            let mut score = 0i32;

            for term in &terms {
                if path_lower == *term {
                    score += 10;
                } else if path_lower.contains(term.as_str()) {
                    score += 5;
                }
                if ext == *term {
                    score += 1;
                }
                if content_lower.contains(term.as_str()) {
                    score += 2;
                }
            }

            if score == 0 {
                return None;
            }

            // Build snippet: up to 5 lines from head content that contain any term.
            let snippet = obj.in_situ_ref.as_deref().and_then(|content| {
                let matching: Vec<&str> = content
                    .lines()
                    .filter(|line| {
                        let ll = line.to_lowercase();
                        terms.iter().any(|t| ll.contains(t.as_str()))
                    })
                    .take(5)
                    .collect();
                if matching.is_empty() {
                    None
                } else {
                    Some(matching.join("\n"))
                }
            });

            Some(SearchHit {
                path,
                object_kind: kind.to_string(),
                score,
                snippet,
            })
        })
        .collect();

    // Sort by score descending, then path ascending for stable ordering.
    hits.sort_by(|a, b| b.score.cmp(&a.score).then_with(|| a.path.cmp(&b.path)));
    hits.truncate(20);

    SearchResult {
        hits,
        query: terms,
    }
}

/// Format a `SearchResult` as a multi-line string matching `ams.bat proj-dir search` output.
///
/// Each hit is rendered as:
/// ```text
/// --- <path>
/// <snippet lines, if any>
/// ```
pub fn format_search(result: &SearchResult) -> String {
    if result.hits.is_empty() {
        return "(no results)".to_string();
    }
    let mut lines = Vec::new();
    for hit in &result.hits {
        lines.push(format!("--- {}", hit.path));
        if let Some(snippet) = &hit.snippet {
            lines.push(snippet.clone());
        }
    }
    lines.join("\n")
}

// ── Tests ──────────────────────────────────────────────────────────────────

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::Write as _;
    use std::path::PathBuf;

    fn make_store() -> AmsStore {
        AmsStore::new()
    }

    fn write_temp_file(dir: &Path, name: &str, content: &str) -> PathBuf {
        let path = dir.join(name);
        let mut f = fs::File::create(&path).unwrap();
        f.write_all(content.as_bytes()).unwrap();
        path
    }

    // Helper: fake projdir_ingest that takes an explicit file list instead of
    // running git, so tests work without a git repo.
    fn ingest_files(
        store: &mut AmsStore,
        repo_root: &Path,
        files: &[&str],
    ) -> IngestResult {
        let now = now_fixed();
        let tool = register_tool(store, TOOL_ID, TOOL_VERSION, Some(now)).unwrap();
        let mut result = IngestResult {
            total: files.len(),
            ..Default::default()
        };

        for rel_path in files {
            let norm = normalize_file_path(rel_path);
            let source_id = format!("file:{}", norm);
            let abs_path = repo_root.join(rel_path);

            let (mtime, size) = match file_meta(&abs_path) {
                Some(m) => m,
                None => { result.skipped += 1; continue; }
            };
            let fp = fingerprint(mtime, size);

            let hits = lookup_tool_centric(store, TOOL_ID, &source_id, None);
            let cache_hit = hits.iter().find(|h| {
                h.metadata.source_fingerprint.as_deref() == Some(&fp)
            });
            if cache_hit.is_some() {
                result.skipped += 1;
                continue;
            }

            let ext = Path::new(rel_path)
                .extension()
                .and_then(|e| e.to_str())
                .unwrap_or("")
                .to_string();
            let depth = norm.split('/').count();
            let head = read_head(&abs_path, HEAD_LINES);

            let mut prov: BTreeMap<String, Value> = BTreeMap::new();
            prov.insert("path".into(), Value::String(norm.clone()));
            prov.insert("ext".into(), Value::String(ext.clone()));
            prov.insert("size".into(), Value::Number(size.into()));
            prov.insert("mtime".into(), Value::Number(mtime.into()));
            prov.insert("depth".into(), Value::Number(depth.into()));

            let semantic = SemanticPayload {
                provenance: Some(prov),
                ..SemanticPayload::default()
            };

            let object_id = format!("file:{}", norm);
            store
                .upsert_object(
                    object_id.clone(),
                    FILE_OBJECT_KIND.to_string(),
                    head,
                    Some(semantic),
                    Some(now),
                )
                .unwrap();

            let source = SourceIdentity {
                source_id: source_id.clone(),
                fingerprint: Some(fp),
            };
            let invocation = InvocationIdentity::new(&tool, &source, "none");
            let artifact_payload = serde_json::json!({
                "path": norm, "mtime": mtime, "size": size, "ext": ext, "depth": depth,
            }).to_string();

            promote_artifact(
                store,
                &tool,
                &source,
                &invocation,
                Some(&artifact_payload),
                None,
                ACTOR_ID,
                Some(now),
            ).unwrap();

            result.ingested += 1;
        }

        result
    }

    #[test]
    fn test_ingest_creates_objects() {
        let dir = tempfile::tempdir().unwrap();
        write_temp_file(dir.path(), "foo.txt", "hello world");
        write_temp_file(dir.path(), "bar.rs", "fn main() {}");
        write_temp_file(dir.path(), "baz.py", "print('hi')");

        let mut store = make_store();
        let files = &["foo.txt", "bar.rs", "baz.py"];
        let result = ingest_files(&mut store, dir.path(), files);

        assert_eq!(result.ingested, 3);
        assert_eq!(result.skipped, 0);
        assert_eq!(result.total, 3);

        // All three file objects must exist.
        assert!(store.objects().contains_key("file:foo.txt"));
        assert!(store.objects().contains_key("file:bar.rs"));
        assert!(store.objects().contains_key("file:baz.py"));
    }

    #[test]
    fn test_second_ingest_all_skipped() {
        let dir = tempfile::tempdir().unwrap();
        write_temp_file(dir.path(), "a.txt", "aaa");
        write_temp_file(dir.path(), "b.txt", "bbb");

        let mut store = make_store();
        let files = &["a.txt", "b.txt"];

        let r1 = ingest_files(&mut store, dir.path(), files);
        assert_eq!(r1.ingested, 2);

        // Second run — nothing changed, all skipped.
        let r2 = ingest_files(&mut store, dir.path(), files);
        assert_eq!(r2.ingested, 0);
        assert_eq!(r2.skipped, 2);
    }

    #[test]
    fn test_modified_file_reingested() {
        let dir = tempfile::tempdir().unwrap();
        write_temp_file(dir.path(), "x.txt", "original content");
        write_temp_file(dir.path(), "y.txt", "unchanged");

        let mut store = make_store();
        let files = &["x.txt", "y.txt"];

        let r1 = ingest_files(&mut store, dir.path(), files);
        assert_eq!(r1.ingested, 2);

        // Modify x.txt (change content and ensure mtime changes by sleeping briefly
        // or overwriting with different size).
        // We overwrite with longer content to change size, which changes fingerprint.
        std::thread::sleep(std::time::Duration::from_millis(10));
        {
            let mut f = fs::OpenOptions::new()
                .write(true)
                .truncate(true)
                .open(dir.path().join("x.txt"))
                .unwrap();
            f.write_all(b"modified content that is longer than the original").unwrap();
        }

        let r2 = ingest_files(&mut store, dir.path(), files);
        assert_eq!(r2.ingested, 1, "only x.txt should be re-ingested");
        assert_eq!(r2.skipped, 1, "y.txt should be skipped");
    }

    #[test]
    fn test_binary_ext_no_head() {
        let dir = tempfile::tempdir().unwrap();
        // Write a fake .db file.
        write_temp_file(dir.path(), "data.db", "binary data here");

        let mut store = make_store();
        let files = &["data.db"];
        let result = ingest_files(&mut store, dir.path(), files);

        assert_eq!(result.ingested, 1);
        let obj = store.objects().get("file:data.db").unwrap();
        // Binary extension — in_situ_ref must be None.
        assert!(obj.in_situ_ref.is_none(), "binary files must not have head content");
    }

    #[test]
    fn test_normalize_file_path() {
        assert_eq!(normalize_file_path("src/foo.rs"), "src/foo.rs");
        assert_eq!(normalize_file_path("./src/foo.rs"), "src/foo.rs");
        assert_eq!(normalize_file_path("src\\foo.rs"), "src/foo.rs");
        assert_eq!(normalize_file_path("SRC/Foo.RS"), "src/foo.rs");
    }

    // ── P4-A2: Directory SmartList Builder tests ────────────────────────────

    /// Seed a store with file Objects at given normalized paths (no filesystem needed).
    fn seed_file_objects(store: &mut AmsStore, paths: &[&str]) {
        let now = now_fixed();
        for path in paths {
            let object_id = format!("file:{}", path);
            store
                .upsert_object(object_id, FILE_OBJECT_KIND.to_string(), Some(path.to_string()), None, Some(now))
                .unwrap();
        }
    }

    #[test]
    fn test_build_dirs_creates_directory_smartlists() {
        // 4 files in 2 directories.
        let mut store = make_store();
        seed_file_objects(&mut store, &[
            "src/a.rs",
            "src/b.rs",
            "tests/c.rs",
            "tests/d.rs",
        ]);

        let result = projdir_build_dirs(&mut store).unwrap();
        assert_eq!(result.dirs_created, 2, "should create 2 directory objects (src, tests)");
        assert_eq!(result.files_attached, 4, "all 4 files should be attached");

        // Both directory SmartList buckets must exist (stored as smartlist-bucket:<path>).
        assert!(store.objects().contains_key("smartlist-bucket:smartlist/projdir/src"),
            "smartlist/projdir/src bucket must exist");
        assert!(store.objects().contains_key("smartlist-bucket:smartlist/projdir/tests"),
            "smartlist/projdir/tests bucket must exist");

        // Both directory Objects must exist.
        assert!(store.objects().contains_key("dir:src"), "dir:src object must exist");
        assert!(store.objects().contains_key("dir:tests"), "dir:tests object must exist");
    }

    #[test]
    fn test_build_dirs_correct_file_membership() {
        let mut store = make_store();
        seed_file_objects(&mut store, &[
            "alpha/x.txt",
            "alpha/y.txt",
            "beta/z.txt",
        ]);

        projdir_build_dirs(&mut store).unwrap();

        // Verify file objects exist in the store.
        assert!(store.objects().contains_key("file:alpha/x.txt"));
        assert!(store.objects().contains_key("file:alpha/y.txt"));
        assert!(store.objects().contains_key("file:beta/z.txt"));

        // Verify directory objects exist.
        assert!(store.objects().contains_key("dir:alpha"));
        assert!(store.objects().contains_key("dir:beta"));
    }

    #[test]
    fn test_build_dirs_subdirectory_in_parent() {
        // src/lib/mod.rs → dir:src/lib should appear in smartlist/projdir/src
        let mut store = make_store();
        seed_file_objects(&mut store, &[
            "src/lib/mod.rs",
            "src/main.rs",
        ]);

        projdir_build_dirs(&mut store).unwrap();

        // Three directories: "src", "src/lib", and root files go in root bucket.
        assert!(store.objects().contains_key("dir:src"), "dir:src must exist");
        assert!(store.objects().contains_key("dir:src/lib"), "dir:src/lib must exist");

        // The smartlist/projdir/src bucket must exist.
        assert!(store.objects().contains_key("smartlist-bucket:smartlist/projdir/src"));
        // The smartlist/projdir/src/lib bucket must exist.
        assert!(store.objects().contains_key("smartlist-bucket:smartlist/projdir/src/lib"));
    }

    #[test]
    fn test_build_dirs_idempotent() {
        let mut store = make_store();
        seed_file_objects(&mut store, &["foo/a.rs", "foo/b.rs"]);

        let r1 = projdir_build_dirs(&mut store).unwrap();
        let r2 = projdir_build_dirs(&mut store).unwrap();

        assert_eq!(r1.dirs_created, r2.dirs_created, "second run must create same dir count");
        assert_eq!(r1.files_attached, r2.files_attached, "second run must attach same file count");
    }

    #[test]
    fn test_projdir_stats_counts() {
        let dir = tempfile::tempdir().unwrap();
        write_temp_file(dir.path(), "a.rs", "fn main() {}");
        write_temp_file(dir.path(), "b.rs", "fn foo() {}");
        write_temp_file(dir.path(), "c.py", "print('hi')");
        write_temp_file(dir.path(), "d.py", "x = 1");
        write_temp_file(dir.path(), "e.md", "# title");

        let mut store = make_store();
        let files = &["a.rs", "b.rs", "c.py", "d.py", "e.md"];
        ingest_files(&mut store, dir.path(), files);

        let stats = projdir_stats(&mut store).unwrap();

        assert_eq!(stats.len(), 3);

        let rs = stats.iter().find(|s| s.ext == "rs").unwrap();
        assert_eq!(rs.count, 2);

        let py = stats.iter().find(|s| s.ext == "py").unwrap();
        assert_eq!(py.count, 2);

        let md = stats.iter().find(|s| s.ext == "md").unwrap();
        assert_eq!(md.count, 1);

        assert!(store.objects().contains_key(STATS_OBJECT_ID));
        assert!(store.objects().contains_key("ext-stats:rs"));
        assert!(store.objects().contains_key("ext-stats:py"));
        assert!(store.objects().contains_key("ext-stats:md"));
    }

    #[test]
    fn test_projdir_stats_table_format() {
        let dir = tempfile::tempdir().unwrap();
        write_temp_file(dir.path(), "a.rs", "fn main() {}");
        write_temp_file(dir.path(), "b.rs", "fn foo() {}");
        write_temp_file(dir.path(), "c.py", "print('hi')");

        let mut store = make_store();
        ingest_files(&mut store, dir.path(), &["a.rs", "b.rs", "c.py"]);

        let stats = projdir_stats(&mut store).unwrap();
        let table = format_stats_table(&stats);

        assert!(table.contains("ext"));
        assert!(table.contains("count"));
        assert!(table.contains("size (bytes)"));
        assert!(table.contains("rs"));
        assert!(table.contains("py"));
    }

    // ── P4-B1: Scale-0 Atlas Root tests ────────────────────────────────────

    #[test]
    fn test_register_atlas_requires_build_dirs_first() {
        let mut store = make_store();
        // No buckets at all — should fail with a clear error.
        let err = projdir_register_atlas(&mut store).unwrap_err();
        assert!(
            err.to_string().contains("smartlist/projdir bucket not found"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_register_atlas_scale0_only_when_no_subdirs() {
        let mut store = make_store();
        // Seed only root-level files so projdir_build_dirs creates only the root bucket.
        seed_file_objects(&mut store, &["readme.md", "cargo.toml"]);
        projdir_build_dirs(&mut store).unwrap();

        let result = projdir_register_atlas(&mut store).unwrap();
        assert_eq!(result.atlas_name, "projdir");
        // Scale 0 must be present.
        assert_eq!(result.scale0_buckets, vec!["smartlist/projdir"]);
        // No subdirectories → scale 1 empty.
        assert!(result.scale1_buckets.is_empty(), "no top-level dirs expected");
        // Scale 2 has per-file pages for the 2 root-level files.
        // normalize_path replaces '.' → '-' in bucket paths.
        assert_eq!(result.scale2_buckets.len(), 2, "expected one page per file");
        assert!(result.scale2_buckets.contains(&"smartlist/projdir-file/readme-md".to_string()));
        assert!(result.scale2_buckets.contains(&"smartlist/projdir-file/cargo-toml".to_string()));
        // scale_count reflects non-empty scales (scale 0 + scale 2, no scale 1).
        assert_eq!(result.scale_count, 2);
    }

    #[test]
    fn test_register_atlas_three_scales_with_nested_dirs() {
        let mut store = make_store();
        // Two top-level dirs, one with a nested subdir.
        seed_file_objects(&mut store, &[
            "src/main.rs",
            "src/lib/mod.rs",
            "tests/foo.rs",
        ]);
        projdir_build_dirs(&mut store).unwrap();

        let result = projdir_register_atlas(&mut store).unwrap();
        assert_eq!(result.atlas_name, "projdir");
        assert_eq!(result.scale0_buckets, vec!["smartlist/projdir"]);

        // Scale 1: top-level dirs only (src, tests).
        assert!(result.scale1_buckets.contains(&"smartlist/projdir/src".to_string()));
        assert!(result.scale1_buckets.contains(&"smartlist/projdir/tests".to_string()));
        assert!(!result.scale1_buckets.contains(&"smartlist/projdir/src/lib".to_string()),
            "nested dir must not appear at scale 1");

        // Scale 2: per-file pages — one bucket per file Object.
        // normalize_path replaces '.' → '-' in bucket paths.
        assert!(result.scale2_buckets.contains(&"smartlist/projdir-file/src/main-rs".to_string()));
        assert!(result.scale2_buckets.contains(&"smartlist/projdir-file/src/lib/mod-rs".to_string()));
        assert!(result.scale2_buckets.contains(&"smartlist/projdir-file/tests/foo-rs".to_string()));
        // Directory buckets must NOT appear at scale 2.
        assert!(!result.scale2_buckets.iter().any(|b| b.starts_with("smartlist/projdir/")),
            "directory buckets must not appear at scale 2");

        assert_eq!(result.scale_count, 3);
    }

    #[test]
    fn test_register_atlas_idempotent() {
        let mut store = make_store();
        seed_file_objects(&mut store, &["src/a.rs", "tests/b.rs"]);
        projdir_build_dirs(&mut store).unwrap();

        let r1 = projdir_register_atlas(&mut store).unwrap();
        let r2 = projdir_register_atlas(&mut store).unwrap();

        assert_eq!(r1.atlas_name, r2.atlas_name);
        assert_eq!(r1.scale_count, r2.scale_count);
        assert_eq!(r1.scale0_buckets, r2.scale0_buckets);
        assert_eq!(r1.scale1_buckets, r2.scale1_buckets);
        assert_eq!(r1.scale2_buckets, r2.scale2_buckets);
    }

    // ── P4-B3: File Pages tests ─────────────────────────────────────────────

    #[test]
    fn test_build_file_pages_creates_buckets() {
        let mut store = make_store();
        seed_file_objects(&mut store, &["src/main.rs", "src/lib.rs", "readme.md"]);

        let result = projdir_build_file_pages(&mut store).unwrap();
        assert_eq!(result.pages_created, 3);
        assert_eq!(result.files_attached, 3);

        // normalize_path (smartlist_write) replaces '.' with '-', so "main.rs" → "main-rs".
        assert!(store.objects().contains_key("smartlist-bucket:smartlist/projdir-file/src/main-rs"));
        assert!(store.objects().contains_key("smartlist-bucket:smartlist/projdir-file/src/lib-rs"));
        assert!(store.objects().contains_key("smartlist-bucket:smartlist/projdir-file/readme-md"));
    }

    #[test]
    fn test_build_file_pages_idempotent() {
        let mut store = make_store();
        seed_file_objects(&mut store, &["a.rs", "b.rs"]);

        let r1 = projdir_build_file_pages(&mut store).unwrap();
        let r2 = projdir_build_file_pages(&mut store).unwrap();

        assert_eq!(r1.pages_created, r2.pages_created);
        assert_eq!(r1.files_attached, r2.files_attached);
    }

    // ── P4-B3: projdir_doc tests ────────────────────────────────────────────

    #[test]
    fn test_projdir_doc_returns_head_content() {
        let dir = tempfile::tempdir().unwrap();
        write_temp_file(dir.path(), "hello.rs", "fn main() {}\n// line 2");

        let mut store = make_store();
        ingest_files(&mut store, dir.path(), &["hello.rs"]);

        let result = projdir_doc(&store, "hello.rs").unwrap();
        assert_eq!(result.path, "hello.rs");
        assert!(result.content.is_some(), "text file must have head content");
        let content = result.content.unwrap();
        assert!(content.contains("fn main()"), "content must include first line");
    }

    #[test]
    fn test_projdir_doc_path_normalization() {
        let dir = tempfile::tempdir().unwrap();
        write_temp_file(dir.path(), "foo.txt", "hello");

        let mut store = make_store();
        ingest_files(&mut store, dir.path(), &["foo.txt"]);

        // Both forms should resolve to the same object.
        let r1 = projdir_doc(&store, "foo.txt").unwrap();
        let r2 = projdir_doc(&store, "./foo.txt").unwrap();
        assert_eq!(r1.path, r2.path);
        assert_eq!(r1.content, r2.content);
    }

    #[test]
    fn test_projdir_doc_missing_file_returns_error() {
        let store = make_store();
        let err = projdir_doc(&store, "nonexistent.rs").unwrap_err();
        assert!(
            err.to_string().contains("file not found in projdir index"),
            "unexpected error: {err}"
        );
    }

    #[test]
    fn test_projdir_doc_binary_file_no_content() {
        let dir = tempfile::tempdir().unwrap();
        write_temp_file(dir.path(), "data.db", "fake binary");

        let mut store = make_store();
        ingest_files(&mut store, dir.path(), &["data.db"]);

        let result = projdir_doc(&store, "data.db").unwrap();
        assert!(result.content.is_none(), "binary file should have no head content");
    }

    // ── P4-B2: projdir_tree tests ───────────────────────────────────────────

    /// Helper: seed file Objects with provenance (size field) so the tree renderer
    /// can print sizes.
    fn seed_file_objects_with_size(store: &mut AmsStore, paths_and_sizes: &[(&str, u64)]) {
        let now = now_fixed();
        for (path, size) in paths_and_sizes {
            let object_id = format!("file:{}", path);
            let mut prov: BTreeMap<String, Value> = BTreeMap::new();
            prov.insert("path".into(), Value::String(path.to_string()));
            prov.insert("ext".into(), Value::String(String::new()));
            prov.insert("size".into(), Value::Number((*size).into()));
            prov.insert("mtime".into(), Value::Number(0u64.into()));
            prov.insert("depth".into(), Value::Number(path.split('/').count().into()));
            let semantic = SemanticPayload { provenance: Some(prov), ..SemanticPayload::default() };
            store
                .upsert_object(object_id, FILE_OBJECT_KIND.to_string(), Some(path.to_string()), Some(semantic), Some(now))
                .unwrap();
        }
    }

    #[test]
    fn test_projdir_tree_root_with_files_and_dirs() {
        let mut store = make_store();
        seed_file_objects_with_size(&mut store, &[
            ("src/main.rs", 100),
            ("src/lib.rs", 200),
            ("readme.md", 50),
        ]);
        projdir_build_dirs(&mut store).unwrap();

        let result = projdir_tree(&store, None, 3);
        let output = format_tree(&result);

        // Root label.
        assert!(output.contains("./"), "root label missing");
        // Directory listed.
        assert!(output.contains("src/"), "src/ directory missing");
        // File at root with size.
        assert!(output.contains("readme.md [50]"), "root file with size missing: {output}");
        // File inside src/ with size.
        assert!(output.contains("main.rs [100]"), "main.rs missing: {output}");
        assert!(output.contains("lib.rs [200]"), "lib.rs missing: {output}");
    }

    #[test]
    fn test_projdir_tree_depth_limit() {
        let mut store = make_store();
        seed_file_objects_with_size(&mut store, &[
            ("a/b/deep.rs", 10),
            ("a/top.rs", 20),
        ]);
        projdir_build_dirs(&mut store).unwrap();

        // Depth 1: only sees direct children of root (dir a/).
        let result = projdir_tree(&store, None, 1);
        let output = format_tree(&result);
        assert!(output.contains("a/"), "should see a/");
        // deep.rs is at depth 3 — must not appear at max_depth=1.
        assert!(!output.contains("deep.rs"), "deep.rs should be hidden at depth 1: {output}");
        // top.rs is inside a/ at depth 2 — also hidden.
        assert!(!output.contains("top.rs"), "top.rs should be hidden at depth 1: {output}");

        // Depth 2: sees a/ and its direct children (top.rs, b/) but not deep.rs.
        let result2 = projdir_tree(&store, None, 2);
        let output2 = format_tree(&result2);
        assert!(output2.contains("top.rs [20]"), "top.rs visible at depth 2: {output2}");
        assert!(!output2.contains("deep.rs"), "deep.rs hidden at depth 2: {output2}");

        // Depth 3: sees everything.
        let result3 = projdir_tree(&store, None, 3);
        let output3 = format_tree(&result3);
        assert!(output3.contains("deep.rs [10]"), "deep.rs visible at depth 3: {output3}");
    }

    #[test]
    fn test_projdir_tree_subpath() {
        let mut store = make_store();
        seed_file_objects_with_size(&mut store, &[
            ("scripts/build.sh", 300),
            ("scripts/run.sh", 150),
            ("src/main.rs", 100),
        ]);
        projdir_build_dirs(&mut store).unwrap();

        // Tree rooted at scripts/.
        let result = projdir_tree(&store, Some("scripts"), 3);
        let output = format_tree(&result);

        assert!(output.contains("scripts/"), "root label");
        assert!(output.contains("build.sh [300]"), "build.sh: {output}");
        assert!(output.contains("run.sh [150]"), "run.sh: {output}");
        // src/ contents must NOT appear.
        assert!(!output.contains("main.rs"), "main.rs should not appear: {output}");
    }

    #[test]
    fn test_projdir_tree_empty_bucket() {
        let store = make_store();
        // No files ingested; tree should return just the root label line.
        let result = projdir_tree(&store, None, 3);
        assert_eq!(result.lines.len(), 1, "only root label expected");
        assert!(result.lines[0].contains("./"), "root label: {:?}", result.lines);
    }

    // ── P4-C1: projdir_search tests ────────────────────────────────────────

    fn seed_file_with_content(store: &mut AmsStore, path: &str, content: &str) {
        let now = now_fixed();
        store
            .upsert_object(
                format!("file:{}", path),
                FILE_OBJECT_KIND.to_string(),
                Some(content.to_string()),
                None,
                Some(now),
            )
            .unwrap();
    }

    #[test]
    fn test_projdir_search_content_match() {
        let mut store = make_store();
        // Use a term that appears in the path of one file but not the other.
        // "alpha_unique" appears in path of src/alpha_unique.rs (+5) and content of both (+2 each).
        // src/alpha_unique.rs: path+content = 7; docs/readme.md: content only = 2.
        seed_file_with_content(&mut store, "src/alpha_unique.rs", "fn alpha_unique() {}");
        seed_file_with_content(&mut store, "src/beta.rs", "fn beta_func() {}");
        seed_file_with_content(&mut store, "src/gamma.rs", "// unrelated content");
        seed_file_with_content(&mut store, "docs/readme.md", "alpha_unique is documented here");
        seed_file_with_content(&mut store, "tests/test.rs", "// nothing relevant");

        let result = projdir_search(&store, &["alpha_unique"]);

        // The two files matching "alpha_unique" should be returned
        assert_eq!(result.hits.len(), 2, "expected 2 hits: {:?}", result.hits.iter().map(|h| &h.path).collect::<Vec<_>>());

        // src/alpha_unique.rs ranks first (path match bonus gives score 7 vs 2)
        assert_eq!(result.hits[0].path, "src/alpha_unique.rs");
        assert_eq!(result.hits[1].path, "docs/readme.md");
    }

    #[test]
    fn test_projdir_search_extension_match() {
        let mut store = make_store();
        seed_file_with_content(&mut store, "src/main.rs", "fn main() {}");
        seed_file_with_content(&mut store, "src/lib.rs", "pub fn lib() {}");
        seed_file_with_content(&mut store, "scripts/run.py", "def run(): pass");
        seed_file_with_content(&mut store, "readme.md", "# docs");

        let result = projdir_search(&store, &["rs"]);
        // Files with .rs extension: main.rs and lib.rs each get +5 (path contains "rs") + +1 (ext==rs)
        // readme.md: no match
        // run.py: no match

        let paths: Vec<&str> = result.hits.iter().map(|h| h.path.as_str()).collect();
        assert!(paths.contains(&"src/main.rs"), "main.rs should be a hit: {paths:?}");
        assert!(paths.contains(&"src/lib.rs"), "lib.rs should be a hit: {paths:?}");
        assert!(!paths.contains(&"readme.md"), "readme.md should not match: {paths:?}");
    }

    #[test]
    fn test_projdir_search_no_results() {
        let mut store = make_store();
        seed_file_with_content(&mut store, "src/foo.rs", "fn foo() {}");

        let result = projdir_search(&store, &["xyzzy_nonexistent"]);
        assert!(result.hits.is_empty(), "expected no hits");
    }

    #[test]
    fn test_projdir_search_top20_limit() {
        let mut store = make_store();
        // Insert 25 files all matching the term.
        for i in 0..25 {
            seed_file_with_content(&mut store, &format!("src/file{i}.rs"), "needle content");
        }

        let result = projdir_search(&store, &["needle"]);
        assert!(result.hits.len() <= 20, "should return at most 20 hits, got {}", result.hits.len());
    }

    #[test]
    fn test_projdir_search_format_output() {
        let mut store = make_store();
        seed_file_with_content(&mut store, "src/alpha.rs", "fn alpha() {}");

        let result = projdir_search(&store, &["alpha"]);
        let output = format_search(&result);
        assert!(output.contains("--- src/alpha.rs"), "output: {output}");
    }

    // ── P4-D2: projdir_context tests ───────────────────────────────────────

    #[test]
    fn test_projdir_context_empty_store() {
        let store = make_store();
        let result = projdir_context(&store, 2);
        // Empty store: tree has only root line, no stats, no key docs.
        assert_eq!(result.tree_lines.len(), 1, "empty store: expected only root label");
        assert!(result.stats.is_empty(), "empty store: expected no stats");
        assert!(result.key_docs.is_empty(), "empty store: expected no key docs");
    }

    #[test]
    fn test_projdir_context_format_sections() {
        let store = make_store();
        let result = projdir_context(&store, 2);
        let output = format_context(&result, 2);
        // Output should contain the three section headers.
        assert!(output.contains("## Project Tree (depth <= 2)"), "missing tree header: {output}");
        assert!(output.contains("## File Stats"), "missing stats header: {output}");
        // Key docs section is omitted when empty — that's fine.
    }
}
