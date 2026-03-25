//! Agent Knowledge Cache (AKC) — semantic knowledge layer on top of proj-dir/Atlas.
//!
//! Agents write curated entries (purpose, api, data-model, failure-modes, decision,
//! prerequisites, test-guide) after doing research, so future agents can skip redundant
//! cold-start research and get injected answers instead.
//!
//! New AMS object kind: `knowledge-entry`
//! New commands: `ams.bat ke write/read/search/context/bootstrap`

use std::collections::{BTreeMap, BTreeSet};
use std::path::Path;
use std::fs;
use std::io::{BufRead, BufReader, Write as IoWrite};
use std::process::{Command, Stdio};

use anyhow::{anyhow, Context, Result};
use serde_json::Value;

use crate::model::{now_fixed, SemanticPayload};
use crate::smartlist_write::{attach_member, browse_bucket, create_bucket, normalize_path};
use crate::store::AmsStore;
use crate::projdir::FILE_OBJECT_KIND;

// ── Constants ──────────────────────────────────────────────────────────────

pub const KE_OBJECT_KIND: &str = "knowledge-entry";
pub const KE_ROOT_BUCKET: &str = "smartlist/knowledge-entries";
pub const KE_SCOPE_PREFIX: &str = "smartlist/ke";
pub const KE_KIND_PREFIX: &str = "smartlist/ke-kind";
pub const KE_SOURCE_PREFIX: &str = "smartlist/ke-source";
pub const KE_STALE_BUCKET: &str = "smartlist/ke-stale";
pub const VALID_KINDS: &[&str] = &[
    "purpose",
    "api",
    "data-model",
    "failure-modes",
    "decision",
    "prerequisites",
    "test-guide",
];

// ── Structs ────────────────────────────────────────────────────────────────

#[derive(Debug, Clone)]
pub struct KeWriteRequest {
    pub scope: String,
    pub kind: String,
    pub text: String,
    pub summary: Option<String>,
    pub tags: Vec<String>,
    pub confidence: f64,
    pub author_agent_id: String,
    pub watch_paths: Vec<String>,
    pub bootstrap_source: Option<String>,
}

#[derive(Debug, Clone)]
pub struct KeWriteResult {
    pub object_id: String,
    pub scope: String,
    pub kind: String,
    pub was_update: bool,
    pub stale_fingerprints: Vec<String>,
}

#[derive(Debug, Clone)]
pub struct KeEntry {
    pub object_id: String,
    pub scope: String,
    pub kind: String,
    pub text: String,
    pub summary: Option<String>,
    pub tags: Vec<String>,
    pub confidence: f64,
    pub author_agent_id: String,
    pub written_at: String,
    pub is_stale: bool,
    pub stale_paths: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct KeReadResult {
    pub scope: String,
    pub entries: Vec<KeEntry>,
}

#[derive(Debug, Clone)]
pub struct KeSearchHit {
    pub entry: KeEntry,
    pub score: i32,
}

#[derive(Debug, Clone, Default)]
pub struct KeSearchResult {
    pub hits: Vec<KeSearchHit>,
    pub query: Vec<String>,
}

#[derive(Debug, Clone, Default)]
pub struct KeContextResult {
    pub entries: Vec<KeEntry>,
    pub stale_count: usize,
    pub total_count: usize,
}

#[derive(Debug, Clone, Default)]
pub struct KeBootstrapResult {
    pub docs_scanned: usize,
    pub entries_written: usize,
    pub skipped_existing: usize,
}

// ── Helpers ────────────────────────────────────────────────────────────────

/// Normalize a scope: strip leading `./`, lowercase, forward slashes.
fn normalize_scope(scope: &str) -> String {
    let s = scope.replace('\\', "/");
    let s = s.strip_prefix("./").unwrap_or(&s);
    s.to_lowercase()
}

/// Extract a field from a provenance map.
fn prov_str<'a>(prov: &'a BTreeMap<String, Value>, key: &str) -> Option<&'a str> {
    prov.get(key).and_then(|v| v.as_str())
}

fn prov_f64(prov: &BTreeMap<String, Value>, key: &str) -> Option<f64> {
    prov.get(key).and_then(|v| v.as_f64())
}

fn prov_strings(prov: &BTreeMap<String, Value>, key: &str) -> Vec<String> {
    prov.get(key)
        .and_then(|v| v.as_array())
        .map(|arr| {
            arr.iter()
                .filter_map(|x| x.as_str().map(|s| s.to_string()))
                .collect()
        })
        .unwrap_or_default()
}

fn looks_like_file_path(path: &str) -> bool {
    !path.starts_with("concept:") && (path.contains('/') || path.contains('.'))
}

fn collect_source_paths(req: &KeWriteRequest, scope: &str) -> Vec<String> {
    let mut paths = BTreeSet::new();
    if looks_like_file_path(scope) {
        paths.insert(normalize_scope(scope));
    }
    for path in &req.watch_paths {
        if looks_like_file_path(path) {
            paths.insert(normalize_scope(path));
        }
    }
    if let Some(ref bootstrap_source) = req.bootstrap_source {
        if looks_like_file_path(bootstrap_source) {
            paths.insert(normalize_scope(bootstrap_source));
        }
    }
    paths.into_iter().collect()
}

fn ensure_ke_file_ref_object(
    store: &mut AmsStore,
    path: &str,
    created_by: &str,
    now_utc: chrono::DateTime<chrono::FixedOffset>,
) -> Result<String> {
    let normalized = normalize_scope(path);
    let object_id = format!("file:{}", normalized);
    let display_name = Path::new(&normalized)
        .file_name()
        .and_then(|value| value.to_str())
        .unwrap_or(&normalized)
        .to_string();

    let mut prov: BTreeMap<String, Value> = BTreeMap::new();
    prov.insert("path".into(), Value::String(normalized.clone()));
    prov.insert("created_by".into(), Value::String(created_by.to_string()));
    prov.insert("created_at".into(), Value::String(now_utc.to_rfc3339()));
    prov.insert("updated_at".into(), Value::String(now_utc.to_rfc3339()));
    prov.insert("source".into(), Value::String("ke-source-ref".to_string()));

    let semantic = SemanticPayload {
        summary: Some(display_name),
        tags: Some(vec!["file-ref".to_string(), "ke-source-ref".to_string()]),
        provenance: Some(prov),
        ..SemanticPayload::default()
    };

    store
        .upsert_object(
            object_id.clone(),
            FILE_OBJECT_KIND.to_string(),
            Some(normalized),
            Some(semantic),
            Some(now_utc),
        )
        .map_err(|e| anyhow!("failed to upsert ke file-ref object '{}': {}", object_id, e))?;

    Ok(object_id)
}

/// Build a KeEntry from a store object's ID and fields.
fn build_ke_entry(store: &AmsStore, object_id: &str) -> Option<KeEntry> {
    let obj = store.objects().get(object_id)?;
    if obj.object_kind != KE_OBJECT_KIND {
        return None;
    }

    let text = obj.in_situ_ref.clone().unwrap_or_default();
    let prov = obj
        .semantic_payload
        .as_ref()
        .and_then(|p| p.provenance.as_ref())
        .cloned()
        .unwrap_or_default();

    let summary = obj
        .semantic_payload
        .as_ref()
        .and_then(|p| p.summary.clone());

    let tags = obj
        .semantic_payload
        .as_ref()
        .and_then(|p| p.tags.clone())
        .unwrap_or_default();

    let scope = prov_str(&prov, "scope").unwrap_or("").to_string();
    let kind = prov_str(&prov, "kind").unwrap_or("").to_string();
    let confidence = prov_f64(&prov, "confidence").unwrap_or(0.0);
    let author_agent_id = prov_str(&prov, "author_agent_id").unwrap_or("").to_string();
    let written_at = prov_str(&prov, "written_at_ts").unwrap_or("").to_string();

    let (is_stale, stale_paths) = ke_check_freshness(store, &KeEntry {
        object_id: object_id.to_string(),
        scope: scope.clone(),
        kind: kind.clone(),
        text: text.clone(),
        summary: summary.clone(),
        tags: tags.clone(),
        confidence,
        author_agent_id: author_agent_id.clone(),
        written_at: written_at.clone(),
        is_stale: false,
        stale_paths: vec![],
    });

    Some(KeEntry {
        object_id: object_id.to_string(),
        scope,
        kind,
        text,
        summary,
        tags,
        confidence,
        author_agent_id,
        written_at,
        is_stale,
        stale_paths,
    })
}

// ── Functions ──────────────────────────────────────────────────────────────

/// Check freshness of a KeEntry by comparing stored fingerprints to current file metadata.
///
/// Returns `(is_stale, stale_paths)`.
pub fn ke_check_freshness(store: &AmsStore, entry: &KeEntry) -> (bool, Vec<String>) {
    let obj = match store.objects().get(&entry.object_id) {
        Some(o) => o,
        None => return (false, vec![]),
    };

    let prov = match obj
        .semantic_payload
        .as_ref()
        .and_then(|p| p.provenance.as_ref())
    {
        Some(p) => p,
        None => return (false, vec![]),
    };

    let fingerprints = prov_strings(prov, "freshness_fingerprints");
    if fingerprints.is_empty() {
        return (false, vec![]);
    }

    let mut stale_paths = Vec::new();
    for fp_entry in &fingerprints {
        // Format: "file:<path>:<mtime>:<size>"
        let parts: Vec<&str> = fp_entry.splitn(4, ':').collect();
        if parts.len() != 4 || parts[0] != "file" {
            continue;
        }
        let path = parts[1];
        let stored_mtime: u64 = parts[2].parse().unwrap_or(0);
        let stored_size: u64 = parts[3].parse().unwrap_or(0);

        // Look up the file object in the store.
        let file_obj_id = format!("file:{}", path);
        let current_fp = store
            .objects()
            .get(&file_obj_id)
            .and_then(|o| o.semantic_payload.as_ref())
            .and_then(|p| p.provenance.as_ref())
            .and_then(|p| {
                let mtime = p.get("mtime").and_then(|v| v.as_u64()).unwrap_or(0);
                let size = p.get("size").and_then(|v| v.as_u64()).unwrap_or(0);
                Some((mtime, size))
            });

        match current_fp {
            Some((mtime, size)) => {
                if mtime != stored_mtime || size != stored_size {
                    stale_paths.push(path.to_string());
                }
            }
            None => {
                // File object not found — treat as stale.
                stale_paths.push(path.to_string());
            }
        }
    }

    let is_stale = !stale_paths.is_empty();
    (is_stale, stale_paths)
}

/// Try to generate an embedding for the given text using `ams-embed` subprocess.
/// Uses OpenAI by default (reads OPENAI_API_KEY from env). Falls back gracefully
/// if ams-embed is not on PATH or the provider is unreachable.
fn try_embed_text(text: &str) -> Option<Vec<f32>> {
    let mut child = Command::new("ams-embed")
        .arg("--provider")
        .arg(std::env::var("AMS_EMBED_PROVIDER").unwrap_or_else(|_| "openai".into()))
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .spawn()
        .ok()?;

    let stdin = child.stdin.take()?;
    let input = serde_json::json!({ "text": text });
    let input_bytes = serde_json::to_vec(&input).ok()?;

    // Write input in a thread to avoid deadlock.
    let (tx, rx) = std::sync::mpsc::channel::<()>();
    std::thread::spawn(move || {
        let mut s = stdin;
        let _ = s.write_all(&input_bytes);
        drop(s);
        let _ = tx.send(());
    });
    let _ = rx.recv();

    let output = child.wait_with_output().ok()?;
    if !output.status.success() {
        return None;
    }

    #[derive(serde::Deserialize)]
    struct EmbedOut { embedding: Vec<f32> }

    let parsed: EmbedOut = serde_json::from_slice(&output.stdout).ok()?;
    Some(parsed.embedding)
}

/// Write a knowledge entry into the AMS store.
pub fn ke_write(store: &mut AmsStore, req: KeWriteRequest) -> Result<KeWriteResult> {
    // Validate kind.
    if !VALID_KINDS.contains(&req.kind.as_str()) {
        return Err(anyhow!(
            "invalid kind '{}'; valid kinds: {}",
            req.kind,
            VALID_KINDS.join(", ")
        ));
    }

    let now = now_fixed();
    let scope = normalize_scope(&req.scope);
    let object_id = format!("ke:{}:{}", scope, req.kind);

    // Check if this is an update.
    let was_update = store.objects().contains_key(&object_id);

    let source_paths = collect_source_paths(&req, &scope);

    // Collect freshness fingerprints for source paths when file objects exist in the store.
    let mut freshness_fingerprints: Vec<String> = Vec::new();
    for path in &source_paths {
        let file_obj_id = format!("file:{}", path);
        if let Some(obj) = store.objects().get(&file_obj_id) {
            if let Some(prov) = obj.semantic_payload.as_ref().and_then(|p| p.provenance.as_ref()) {
                let mtime = prov.get("mtime").and_then(|v| v.as_u64()).unwrap_or(0);
                let size = prov.get("size").and_then(|v| v.as_u64()).unwrap_or(0);
                freshness_fingerprints.push(format!("file:{}:{}:{}", path, mtime, size));
            }
        }
    }

    // Build provenance.
    let mut prov: BTreeMap<String, Value> = BTreeMap::new();
    prov.insert("scope".into(), Value::String(scope.clone()));
    prov.insert("kind".into(), Value::String(req.kind.clone()));
    prov.insert("confidence".into(), Value::from(req.confidence));
    prov.insert("author_agent_id".into(), Value::String(req.author_agent_id.clone()));
    prov.insert(
        "written_at_ts".into(),
        Value::String(now.to_rfc3339()),
    );
    if !freshness_fingerprints.is_empty() {
        prov.insert(
            "freshness_fingerprints".into(),
            Value::Array(
                freshness_fingerprints
                    .iter()
                    .map(|s| Value::String(s.clone()))
                    .collect(),
            ),
        );
    }
    if !source_paths.is_empty() {
        prov.insert(
            "source_paths".into(),
            Value::Array(source_paths.iter().map(|path| Value::String(path.clone())).collect()),
        );
    }
    if let Some(ref bs) = req.bootstrap_source {
        prov.insert("bootstrap_source".into(), Value::String(bs.clone()));
    }

    // Attempt write-time embedding (graceful degradation: skip if ams-embed unavailable).
    let embed_text = {
        let summary_part = req.summary.as_deref().unwrap_or("");
        format!("{} {} {} {}", scope, req.kind, summary_part, req.text)
    };
    let embedding = try_embed_text(&embed_text);
    if embedding.is_none() {
        eprintln!("[ke-write] ams-embed not available or Ollama unreachable — writing entry without embedding");
    }

    let semantic = SemanticPayload {
        embedding,
        summary: req.summary.clone(),
        tags: if req.tags.is_empty() {
            None
        } else {
            Some(req.tags.clone())
        },
        provenance: Some(prov),
        ..SemanticPayload::default()
    };

    // Upsert the object.
    store
        .upsert_object(
            object_id.clone(),
            KE_OBJECT_KIND.to_string(),
            Some(req.text.clone()),
            Some(semantic),
            Some(now),
        )
        .map_err(|e| anyhow!("failed to upsert ke object '{}': {}", object_id, e))?;

    // Ensure root bucket.
    create_bucket(store, KE_ROOT_BUCKET, true, &req.author_agent_id, now)
        .context("failed to create ke root bucket")?;
    attach_member(store, KE_ROOT_BUCKET, &object_id, &req.author_agent_id, now)
        .context("failed to attach ke object to root bucket")?;

    // Ensure scope bucket.
    let scope_bucket = format!("{}/{}", KE_SCOPE_PREFIX, scope);
    create_bucket(store, &scope_bucket, true, &req.author_agent_id, now)
        .context("failed to create ke scope bucket")?;
    attach_member(store, &scope_bucket, &object_id, &req.author_agent_id, now)
        .context("failed to attach ke object to scope bucket")?;

    // Ensure kind bucket.
    let kind_bucket = format!("{}/{}", KE_KIND_PREFIX, req.kind);
    create_bucket(store, &kind_bucket, true, &req.author_agent_id, now)
        .context("failed to create ke kind bucket")?;
    attach_member(store, &kind_bucket, &object_id, &req.author_agent_id, now)
        .context("failed to attach ke object to kind bucket")?;

    if !source_paths.is_empty() {
        let source_bucket = format!("{}/{}/{}", KE_SOURCE_PREFIX, scope, req.kind);
        create_bucket(store, &source_bucket, true, &req.author_agent_id, now)
            .context("failed to create ke source bucket")?;
        attach_member(store, &source_bucket, &object_id, &req.author_agent_id, now)
            .context("failed to attach ke object to source bucket")?;
        for path in &source_paths {
            let file_ref_id = ensure_ke_file_ref_object(store, path, &req.author_agent_id, now)?;
            attach_member(store, &source_bucket, &file_ref_id, &req.author_agent_id, now)
                .context("failed to attach file-ref object to ke source bucket")?;
        }
        let normalized_bucket = normalize_path(&source_bucket)
            .context("failed to normalize ke source bucket path")?;
        let obj = store
            .objects_mut()
            .get_mut(&object_id)
            .ok_or_else(|| anyhow!("failed to materialize ke object '{}'", object_id))?;
        let prov = obj
            .semantic_payload
            .as_mut()
            .and_then(|payload| payload.provenance.as_mut())
            .ok_or_else(|| anyhow!("missing provenance for ke object '{}'", object_id))?;
        prov.insert("source_bucket_path".into(), Value::String(normalized_bucket));
    }

    Ok(KeWriteResult {
        object_id,
        scope,
        kind: req.kind,
        was_update,
        stale_fingerprints: freshness_fingerprints,
    })
}

/// Read all knowledge entries for a given scope.
pub fn ke_read(store: &AmsStore, scope: &str, include_stale: bool) -> KeReadResult {
    let scope_norm = normalize_scope(scope);
    let scope_bucket = format!("{}/{}", KE_SCOPE_PREFIX, scope_norm);

    let browse_items = match browse_bucket(store, &scope_bucket) {
        Ok(items) => items,
        Err(_) => {
            return KeReadResult {
                scope: scope_norm,
                entries: vec![],
            }
        }
    };

    let mut entries: Vec<KeEntry> = browse_items
        .iter()
        .filter_map(|item| build_ke_entry(store, &item.object_id))
        .filter(|e| include_stale || !e.is_stale)
        .collect();

    entries.sort_by(|a, b| a.kind.cmp(&b.kind));

    KeReadResult {
        scope: scope_norm,
        entries,
    }
}

/// Search knowledge entries by query terms.
pub fn ke_search(
    store: &AmsStore,
    query: &[&str],
    top: usize,
    scope_filter: Option<&str>,
    kind_filter: Option<&str>,
) -> KeSearchResult {
    let scope_filter_norm = scope_filter.map(normalize_scope);
    let kind_filter_lower = kind_filter.map(|k| k.to_lowercase());

    let ke_object_ids: Vec<String> = store
        .objects()
        .iter()
        .filter(|(_, obj)| obj.object_kind == KE_OBJECT_KIND)
        .map(|(id, _)| id.clone())
        .collect();

    let mut hits: Vec<KeSearchHit> = ke_object_ids
        .iter()
        .filter_map(|id| build_ke_entry(store, id))
        .filter(|e| {
            if let Some(ref sf) = scope_filter_norm {
                if !e.scope.starts_with(sf.as_str()) {
                    return false;
                }
            }
            if let Some(ref kf) = kind_filter_lower {
                if &e.kind != kf {
                    return false;
                }
            }
            true
        })
        .map(|entry| {
            let mut score: i32 = 0;
            for term in query {
                let term_lower = term.to_lowercase();
                if entry.scope.contains(&term_lower) {
                    score += 5;
                }
                if entry.kind.contains(&term_lower) {
                    score += 4;
                }
                if let Some(ref summary) = entry.summary {
                    if summary.to_lowercase().contains(&term_lower) {
                        score += 3;
                    }
                }
                for tag in &entry.tags {
                    if tag.to_lowercase().contains(&term_lower) {
                        score += 3;
                    }
                }
                if entry.text.to_lowercase().contains(&term_lower) {
                    score += 2;
                }
            }
            if entry.is_stale {
                score -= 2;
            }
            KeSearchHit { entry, score }
        })
        .filter(|h| h.score > 0)
        .collect();

    hits.sort_by(|a, b| b.score.cmp(&a.score));
    hits.truncate(top);

    KeSearchResult {
        hits,
        query: query.iter().map(|s| s.to_string()).collect(),
    }
}

/// Build a compact context block for injection into agent prompts.
pub fn ke_context(
    store: &AmsStore,
    scope_filter: Option<&str>,
    max_entries: usize,
    max_chars: usize,
) -> String {
    let scope_filter_norm = scope_filter.map(normalize_scope);

    let mut entries: Vec<KeEntry> = store
        .objects()
        .iter()
        .filter(|(_, obj)| obj.object_kind == KE_OBJECT_KIND)
        .filter_map(|(id, _)| build_ke_entry(store, id))
        .filter(|e| !e.is_stale)
        .filter(|e| {
            if let Some(ref sf) = scope_filter_norm {
                e.scope.starts_with(sf.as_str())
            } else {
                true
            }
        })
        .collect();

    // Sort by confidence descending.
    entries.sort_by(|a, b| {
        b.confidence
            .partial_cmp(&a.confidence)
            .unwrap_or(std::cmp::Ordering::Equal)
    });
    entries.truncate(max_entries);

    if entries.is_empty() {
        return String::new();
    }

    let scope_label = scope_filter_norm.as_deref().unwrap_or("all");
    let mut lines = vec![format!("## Knowledge Cache [scope: {}]", scope_label)];

    for entry in &entries {
        let truncated: String = entry.text.chars().take(200).collect();
        let ellipsis = if entry.text.len() > 200 { "…" } else { "" };
        lines.push(format!("[{}] {}{}", entry.kind, truncated, ellipsis));
    }

    let mut output = lines.join("\n");
    if output.len() > max_chars {
        output.truncate(max_chars);
    }
    output
}

/// Bootstrap knowledge entries from markdown files in the repo (depth ≤ 3).
pub fn ke_bootstrap(store: &mut AmsStore, repo_root: &Path) -> Result<KeBootstrapResult> {
    let mut result = KeBootstrapResult::default();

    // Collect all file objects with object_kind == "file", ext == "md", depth <= 3.
    let md_files: Vec<(String, String, usize)> = store
        .objects()
        .iter()
        .filter(|(_, obj)| obj.object_kind == FILE_OBJECT_KIND)
        .filter_map(|(id, obj)| {
            let prov = obj
                .semantic_payload
                .as_ref()
                .and_then(|p| p.provenance.as_ref())?;
            let ext = prov_str(prov, "ext")?;
            if ext != "md" {
                return None;
            }
            let depth = prov.get("depth").and_then(|v| v.as_u64()).unwrap_or(0) as usize;
            if depth > 3 {
                return None;
            }
            let path = prov_str(prov, "path")?.to_string();
            let object_id = id.clone();
            Some((object_id, path, depth))
        })
        .collect();

    for (_object_id, path, _depth) in &md_files {
        result.docs_scanned += 1;

        // Determine scope: directory containing the file, or file path for root-level docs.
        let scope = {
            let p = path.as_str();
            if let Some(slash) = p.rfind('/') {
                let dir = &p[..slash];
                if dir.is_empty() {
                    p.to_string()
                } else {
                    dir.to_string()
                }
            } else {
                // File is at repo root — use the file path itself as scope.
                p.to_string()
            }
        };

        // Check if a non-stale purpose entry already exists for this scope.
        let read_result = ke_read(store, &scope, false);
        let already_has_purpose = read_result.entries.iter().any(|e| e.kind == "purpose");
        if already_has_purpose {
            result.skipped_existing += 1;
            continue;
        }

        // Read the file from disk and extract the first non-empty paragraph.
        let abs_path = repo_root.join(path.as_str());
        let first_paragraph = read_first_paragraph(&abs_path);
        let text = match first_paragraph {
            Some(t) if !t.trim().is_empty() => t,
            _ => continue,
        };

        let req = KeWriteRequest {
            scope: scope.clone(),
            kind: "purpose".to_string(),
            text,
            summary: None,
            tags: vec!["bootstrap".to_string(), "markdown".to_string()],
            confidence: 0.7,
            author_agent_id: "ke-bootstrap".to_string(),
            watch_paths: vec![path.clone()],
            bootstrap_source: Some(path.clone()),
        };

        ke_write(store, req).context(format!("failed to write bootstrap entry for {}", scope))?;
        result.entries_written += 1;
    }

    Ok(result)
}

/// Read the first non-empty paragraph from a markdown file.
fn read_first_paragraph(path: &Path) -> Option<String> {
    let file = fs::File::open(path).ok()?;
    let reader = BufReader::new(file);
    let mut paragraph_lines: Vec<String> = Vec::new();
    let mut in_paragraph = false;

    for line in reader.lines().take(100) {
        let line = line.ok()?;
        let trimmed = line.trim();

        if trimmed.is_empty() {
            if in_paragraph {
                break; // End of first paragraph.
            }
        } else {
            // Skip markdown headings for the purpose of extracting prose.
            if trimmed.starts_with('#') && paragraph_lines.is_empty() {
                continue;
            }
            in_paragraph = true;
            paragraph_lines.push(trimmed.to_string());
        }
    }

    if paragraph_lines.is_empty() {
        None
    } else {
        Some(paragraph_lines.join(" "))
    }
}
