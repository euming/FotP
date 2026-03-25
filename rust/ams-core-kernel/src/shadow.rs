use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

use anyhow::{anyhow, bail, Context, Result};
use regex::Regex;
use serde::Serialize;

use crate::agent_query::{run_agent_query, AgentQueryRequest, AgentQueryResult};
use crate::parity::{load_parity_cases, ParityCase};
use crate::route_memory::{load_route_replay_records, RouteMemoryBiasOptions, RouteMemoryStore};
use crate::{import_materialized_corpus, CardState, QueryContextOptions};

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ShadowSurfaceSummary {
    pub lesson_count: usize,
    pub top_lesson_title: Option<String>,
    pub lesson_titles: Vec<String>,
    pub short_term_count: usize,
    pub top_short_term_ref: Option<String>,
    pub short_term_refs: Vec<String>,
    pub fallback_count: usize,
    pub top_fallback_ref: Option<String>,
    pub fallback_refs: Vec<String>,
    pub weak_result: bool,
    pub scope_lens: String,
    pub lane: String,
    pub reroute: String,
}

#[derive(Clone, Debug, PartialEq, Serialize)]
pub struct ShadowValidationReport {
    pub case_name: String,
    pub input: String,
    pub passed: bool,
    pub rust: ShadowSurfaceSummary,
    pub csharp: ShadowSurfaceSummary,
    pub differences: Vec<String>,
    pub unsupported: Vec<String>,
}

pub fn load_shadow_cases(path: &Path) -> Result<Vec<ParityCase>> {
    load_parity_cases(path)
}

pub fn run_shadow_validation(
    default_input: &Path,
    cases: &[ParityCase],
    cases_root: &Path,
    memoryctl_override: Option<&Path>,
) -> Result<Vec<ShadowValidationReport>> {
    let memoryctl = resolve_memoryctl_runner(memoryctl_override)?;
    let mut reports = Vec::with_capacity(cases.len());
    for case in cases {
        let input_path = case
            .input
            .as_deref()
            .map(|input| resolve_relative_path(cases_root, input))
            .unwrap_or_else(|| default_input.to_path_buf());
        let mut corpus = import_materialized_corpus(&input_path)?;
        let route_memory = match case.route_replay.as_deref() {
            Some(relative_path) => {
                let replay_path = resolve_relative_path(cases_root, relative_path);
                let replay_records = load_route_replay_records(&replay_path)?;
                Some(RouteMemoryStore::from_replay_records(&replay_records))
            }
            None => None,
        };

        let rust_result = run_agent_query(
            &mut corpus,
            &AgentQueryRequest {
                query: case.query.clone(),
                top: case.top.max(1),
                binder_filters: parse_multi_value(case.binder.as_deref()),
                seed_card: case.seed_card.clone(),
                state_filter: case.state.as_deref().map(CardState::parse).transpose()?,
                include_retracted: case.include_retracted,
                explain: false,
                context_options: QueryContextOptions {
                    current_node_id: case.current_node.clone(),
                    parent_node_id: case.parent_node.clone(),
                    grandparent_node_id: case.grandparent_node.clone(),
                    agent_role: case.role.clone(),
                    mode: case.mode.clone(),
                    failure_bucket: case.failure_bucket.clone(),
                    active_artifacts: parse_multi_value(case.artifact.as_deref()),
                    traversal_budget: case.traversal_budget.max(1),
                    no_active_thread_context: case.no_active_thread_context,
                },
                route_memory,
                route_memory_bias_options: RouteMemoryBiasOptions::default(),
                include_latent: false,
                touch: true,
            },
        )?;
        let rust_summary = summarize_rust_result(&rust_result);

        let mut unsupported = Vec::new();
        if case.route_replay.is_some() {
            unsupported.push("route_replay_not_supported_by_csharp_shadow".to_string());
        }
        let csharp_summary = match run_csharp_agent_query(&memoryctl, &input_path, case) {
            Ok(output) => parse_csharp_agent_query_output(&output)?,
            Err(err) => {
                unsupported.push(format!("csharp_query_failed:{}", err.to_string().replace('\n', " ")));
                ShadowSurfaceSummary {
                    lesson_count: 0,
                    top_lesson_title: None,
                    lesson_titles: Vec::new(),
                    short_term_count: 0,
                    top_short_term_ref: None,
                    short_term_refs: Vec::new(),
                    fallback_count: 0,
                    top_fallback_ref: None,
                    fallback_refs: Vec::new(),
                    weak_result: true,
                    scope_lens: "unknown".to_string(),
                    lane: "unsupported".to_string(),
                    reroute: "unsupported".to_string(),
                }
            }
        };
        let differences = compare_shadow_summaries(&rust_summary, &csharp_summary, &unsupported);

        reports.push(ShadowValidationReport {
            case_name: case.name.clone(),
            input: input_path.display().to_string(),
            passed: differences.is_empty(),
            rust: rust_summary,
            csharp: csharp_summary,
            differences,
            unsupported,
        });
    }
    Ok(reports)
}

pub fn write_shadow_reports(path: &Path, reports: &[ShadowValidationReport]) -> Result<()> {
    let mut lines = String::new();
    for report in reports {
        lines.push_str(&serde_json::to_string(report)?);
        lines.push('\n');
    }
    fs::write(path, lines).with_context(|| format!("failed to write shadow report '{}'", path.display()))
}

fn summarize_rust_result(result: &AgentQueryResult) -> ShadowSurfaceSummary {
    ShadowSurfaceSummary {
        lesson_count: result.hits.len(),
        top_lesson_title: result.hits.first().map(|hit| hit.title.clone()),
        lesson_titles: result.hits.iter().map(|hit| hit.title.clone()).collect(),
        short_term_count: result.short_term.len(),
        top_short_term_ref: result.short_term.first().map(|hit| hit.source_ref.clone()),
        short_term_refs: result.short_term.iter().map(|hit| hit.source_ref.clone()).collect(),
        fallback_count: result.fallback.len(),
        top_fallback_ref: result.fallback.first().map(|hit| hit.source_ref.clone()),
        fallback_refs: result.fallback.iter().map(|hit| hit.source_ref.clone()).collect(),
        weak_result: result.weak_result,
        scope_lens: result.diagnostics.scope_lens.clone(),
        lane: result.diagnostics.scoring_lane.clone(),
        reroute: result.diagnostics.routing_decision.clone(),
    }
}

fn run_csharp_agent_query(runner: &MemoryCtlRunner, input: &Path, case: &ParityCase) -> Result<String> {
    let mut args = vec![
        "agent-query".to_string(),
        "--db".to_string(),
        input.display().to_string(),
        "--q".to_string(),
        case.query.clone(),
        "--top".to_string(),
        case.top.max(1).to_string(),
    ];
    if let Some(binder) = case.binder.as_deref() {
        args.push("--binder".to_string());
        args.push(binder.to_string());
    }
    if let Some(seed_card) = case.seed_card.as_deref() {
        args.push("--seed-card".to_string());
        args.push(seed_card.to_string());
    }
    if let Some(state) = case.state.as_deref() {
        args.push("--state".to_string());
        args.push(state.to_string());
    }
    if case.include_retracted {
        args.push("--include-retracted".to_string());
    }
    if let Some(current_node) = case.current_node.as_deref() {
        args.push("--current-node".to_string());
        args.push(current_node.to_string());
    }
    if let Some(parent_node) = case.parent_node.as_deref() {
        args.push("--parent-node".to_string());
        args.push(parent_node.to_string());
    }
    if let Some(grandparent_node) = case.grandparent_node.as_deref() {
        args.push("--grandparent-node".to_string());
        args.push(grandparent_node.to_string());
    }
    if let Some(role) = case.role.as_deref() {
        args.push("--role".to_string());
        args.push(role.to_string());
    }
    if let Some(mode) = case.mode.as_deref() {
        args.push("--mode".to_string());
        args.push(mode.to_string());
    }
    if let Some(failure_bucket) = case.failure_bucket.as_deref() {
        args.push("--failure-bucket".to_string());
        args.push(failure_bucket.to_string());
    }
    if let Some(artifact) = case.artifact.as_deref() {
        args.push("--artifact".to_string());
        args.push(artifact.to_string());
    }
    args.push("--traversal-budget".to_string());
    args.push(case.traversal_budget.max(1).to_string());
    if case.no_active_thread_context {
        args.push("--no-active-thread-context".to_string());
    }

    let output = runner.run(&args)?;
    if !output.status.success() {
        bail!(
            "csharp agent-query failed (exit={}): {}",
            output.status.code().unwrap_or(-1),
            String::from_utf8_lossy(&output.stderr)
        );
    }
    Ok(String::from_utf8(output.stdout).context("csharp agent-query emitted non-utf8 output")?)
}

fn parse_csharp_agent_query_output(markdown: &str) -> Result<ShadowSurfaceSummary> {
    let lesson_rx = Regex::new(r"^\d+\.\s+(?P<title>.+?)\s+\(score=").unwrap();
    let short_term_ref_rx = Regex::new(r"^- ref:\s+(?P<ref>.+?)\s+\(session=").unwrap();
    let summary_ref_rx = Regex::new(r"^- (?P<ref>[^\s].+)$").unwrap();

    let mut section = "";
    let mut lesson_titles = Vec::new();
    let mut short_term_refs = Vec::new();
    let mut fallback_refs = Vec::new();
    let mut diagnostics_line = None::<String>;

    for raw_line in markdown.lines() {
        let line = raw_line.trim_end();
        match line {
            "## Lessons" => {
                section = "lessons";
                continue;
            }
            "## Short-Term Memory" => {
                section = "short-term";
                continue;
            }
            "## Cross-Agent Summaries (fallback)" => {
                section = "fallback";
                continue;
            }
            "# Diagnostics" => {
                section = "diagnostics";
                continue;
            }
            _ => {}
        }

        match section {
            "lessons" => {
                if let Some(captures) = lesson_rx.captures(line) {
                    lesson_titles.push(captures["title"].to_string());
                }
            }
            "short-term" => {
                if let Some(captures) = short_term_ref_rx.captures(line) {
                    short_term_refs.push(captures["ref"].to_string());
                }
            }
            "fallback" => {
                if line == "- No fallback summaries available." {
                    continue;
                }
                if let Some(captures) = summary_ref_rx.captures(line) {
                    fallback_refs.push(captures["ref"].to_string());
                }
            }
            "diagnostics" => {
                if !line.trim().is_empty() && diagnostics_line.is_none() {
                    diagnostics_line = Some(line.to_string());
                }
            }
            _ => {}
        }
    }

    let diagnostics = diagnostics_line.ok_or_else(|| anyhow!("missing C# diagnostics line"))?;
    let weak_result = parse_diagnostic_flag(&diagnostics, "weak_result")
        .ok_or_else(|| anyhow!("missing weak_result in C# diagnostics"))?;
    let lesson_count = parse_diagnostic_usize(&diagnostics, "lesson_hits")
        .ok_or_else(|| anyhow!("missing lesson_hits in C# diagnostics"))?;
    let short_term_count = parse_diagnostic_usize(&diagnostics, "short_term_hits")
        .ok_or_else(|| anyhow!("missing short_term_hits in C# diagnostics"))?;
    let scope_lens = parse_diagnostic_string(&diagnostics, "scope_lens")
        .ok_or_else(|| anyhow!("missing scope_lens in C# diagnostics"))?;
    let lane =
        parse_diagnostic_string(&diagnostics, "lane").ok_or_else(|| anyhow!("missing lane in diagnostics"))?;
    let reroute = parse_diagnostic_string(&diagnostics, "reroute")
        .ok_or_else(|| anyhow!("missing reroute in diagnostics"))?;

    Ok(ShadowSurfaceSummary {
        lesson_count,
        top_lesson_title: lesson_titles.first().cloned(),
        lesson_titles,
        short_term_count,
        top_short_term_ref: short_term_refs.first().cloned(),
        short_term_refs,
        fallback_count: fallback_refs.len(),
        top_fallback_ref: fallback_refs.first().cloned(),
        fallback_refs,
        weak_result,
        scope_lens,
        lane,
        reroute,
    })
}

fn compare_shadow_summaries(
    rust: &ShadowSurfaceSummary,
    csharp: &ShadowSurfaceSummary,
    unsupported: &[String],
) -> Vec<String> {
    let mut differences = Vec::new();
    if rust.lesson_count != csharp.lesson_count {
        differences.push(format!(
            "lesson_count rust={} csharp={}",
            rust.lesson_count, csharp.lesson_count
        ));
    }
    if rust.top_lesson_title != csharp.top_lesson_title {
        differences.push(format!(
            "top_lesson rust='{}' csharp='{}'",
            rust.top_lesson_title.as_deref().unwrap_or("<none>"),
            csharp.top_lesson_title.as_deref().unwrap_or("<none>")
        ));
    }
    if rust.short_term_count != csharp.short_term_count {
        differences.push(format!(
            "short_term_count rust={} csharp={}",
            rust.short_term_count, csharp.short_term_count
        ));
    }
    if rust.top_short_term_ref != csharp.top_short_term_ref {
        differences.push(format!(
            "top_short_term rust='{}' csharp='{}'",
            rust.top_short_term_ref.as_deref().unwrap_or("<none>"),
            csharp.top_short_term_ref.as_deref().unwrap_or("<none>")
        ));
    }
    if rust.fallback_count != csharp.fallback_count {
        differences.push(format!(
            "fallback_count rust={} csharp={}",
            rust.fallback_count, csharp.fallback_count
        ));
    }
    if rust.top_fallback_ref != csharp.top_fallback_ref {
        differences.push(format!(
            "top_fallback rust='{}' csharp='{}'",
            rust.top_fallback_ref.as_deref().unwrap_or("<none>"),
            csharp.top_fallback_ref.as_deref().unwrap_or("<none>")
        ));
    }
    if rust.weak_result != csharp.weak_result {
        differences.push(format!(
            "weak_result rust={} csharp={}",
            rust.weak_result, csharp.weak_result
        ));
    }
    if rust.scope_lens != csharp.scope_lens {
        differences.push(format!(
            "scope_lens rust='{}' csharp='{}'",
            rust.scope_lens, csharp.scope_lens
        ));
    }
    if rust.lane != csharp.lane {
        differences.push(format!("lane rust='{}' csharp='{}'", rust.lane, csharp.lane));
    }
    if rust.reroute != csharp.reroute {
        differences.push(format!(
            "reroute rust='{}' csharp='{}'",
            rust.reroute, csharp.reroute
        ));
    }
    if !unsupported.is_empty() {
        differences.extend(
            unsupported
                .iter()
                .map(|value| format!("unsupported:{value}")),
        );
    }
    differences
}

fn parse_diagnostic_string(line: &str, key: &str) -> Option<String> {
    line.split_whitespace().find_map(|segment| {
        segment
            .split_once('=')
            .filter(|(segment_key, _)| *segment_key == key)
            .map(|(_, value)| value.to_string())
    })
}

fn parse_diagnostic_usize(line: &str, key: &str) -> Option<usize> {
    parse_diagnostic_string(line, key)?.parse().ok()
}

fn parse_diagnostic_flag(line: &str, key: &str) -> Option<bool> {
    match parse_diagnostic_string(line, key)?.as_str() {
        "true" => Some(true),
        "false" => Some(false),
        _ => None,
    }
}

fn parse_multi_value(raw: Option<&str>) -> Vec<String> {
    raw.into_iter()
        .flat_map(|value| value.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

fn resolve_relative_path(root: &Path, relative: &str) -> PathBuf {
    let path = Path::new(relative);
    if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    }
}

#[derive(Clone, Debug)]
struct MemoryCtlRunner {
    program: PathBuf,
    fixed_args: Vec<String>,
}

impl MemoryCtlRunner {
    fn run(&self, args: &[String]) -> Result<std::process::Output> {
        let mut command = Command::new(&self.program);
        command.args(&self.fixed_args);
        command.args(args);
        command.output().with_context(|| {
            format!(
                "failed to launch memoryctl runner '{}'",
                self.program.display()
            )
        })
    }
}

fn resolve_memoryctl_runner(override_path: Option<&Path>) -> Result<MemoryCtlRunner> {
    if let Some(path) = override_path {
        return Ok(MemoryCtlRunner {
            program: path.to_path_buf(),
            fixed_args: Vec::new(),
        });
    }

    let repo_root = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
        .parent()
        .and_then(Path::parent)
        .ok_or_else(|| anyhow!("failed to resolve repository root"))?
        .to_path_buf();
    let candidates = [
        repo_root
            .join("tools")
            .join("memoryctl")
            .join("bin")
            .join("Release")
            .join("net9.0")
            .join("MemoryCtl.exe"),
        repo_root
            .join("tools")
            .join("memoryctl")
            .join("bin")
            .join("Debug")
            .join("net9.0")
            .join("MemoryCtl.exe"),
        repo_root
            .join("scripts")
            .join("output")
            .join("all-agents-sessions")
            .join("MemoryCtl.exe"),
    ];
    for candidate in candidates {
        if candidate.exists() {
            return Ok(MemoryCtlRunner {
                program: candidate,
                fixed_args: Vec::new(),
            });
        }
    }

    let project = repo_root.join("tools").join("memoryctl").join("MemoryCtl.csproj");
    if project.exists() {
        return Ok(MemoryCtlRunner {
            program: PathBuf::from("dotnet"),
            fixed_args: vec![
                "run".to_string(),
                "--project".to_string(),
                project.display().to_string(),
                "--".to_string(),
            ],
        });
    }

    bail!("unable to locate MemoryCtl.exe or MemoryCtl.csproj for shadow validation")
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn parse_csharp_agent_query_output_extracts_shadow_summary() {
        let markdown = r#"# AGENT MEMORY

Query: search cache

## Lessons
1. Search cache invalidation fix (score=0.91, confidence=0.82, evidence=1.00, tier=fresh)
- stereotype: family=family version=version

## Short-Term Memory
1. [turn] Session s1 (score=0.88, recency=0.95, matched=2) @ 2026-03-13 19:00
- snippet: text
- ref: chat-msg:s1-0 (session=chat-session:s1)

## Cross-Agent Summaries (fallback)
- smartlist-note:1

# Diagnostics
weak_result=false touched=0 lesson_hits=1 short_term_hits=1 lane=raw-lesson reroute=direct scope_lens=global
"#;

        let summary = parse_csharp_agent_query_output(markdown).unwrap();

        assert_eq!(summary.lesson_count, 1);
        assert_eq!(
            summary.top_lesson_title.as_deref(),
            Some("Search cache invalidation fix")
        );
        assert_eq!(summary.top_short_term_ref.as_deref(), Some("chat-msg:s1-0"));
        assert_eq!(summary.top_fallback_ref.as_deref(), Some("smartlist-note:1"));
        assert_eq!(summary.scope_lens, "global");
        assert_eq!(summary.lane, "raw-lesson");
    }

    #[test]
    fn compare_shadow_summaries_reports_field_differences() {
        let rust = ShadowSurfaceSummary {
            lesson_count: 1,
            top_lesson_title: Some("Rust".to_string()),
            lesson_titles: vec!["Rust".to_string()],
            short_term_count: 0,
            top_short_term_ref: None,
            short_term_refs: Vec::new(),
            fallback_count: 1,
            top_fallback_ref: Some("smartlist-note:1".to_string()),
            fallback_refs: vec!["smartlist-note:1".to_string()],
            weak_result: false,
            scope_lens: "global".to_string(),
            lane: "raw-lesson".to_string(),
            reroute: "direct".to_string(),
        };
        let csharp = ShadowSurfaceSummary {
            lesson_count: 2,
            top_lesson_title: Some("CSharp".to_string()),
            lesson_titles: vec!["CSharp".to_string()],
            short_term_count: 1,
            top_short_term_ref: Some("chat-msg:s1-0".to_string()),
            short_term_refs: vec!["chat-msg:s1-0".to_string()],
            fallback_count: 0,
            top_fallback_ref: None,
            fallback_refs: Vec::new(),
            weak_result: true,
            scope_lens: "local-first-lineage".to_string(),
            lane: "semantic".to_string(),
            reroute: "fallback".to_string(),
        };

        let differences = compare_shadow_summaries(&rust, &csharp, &[]);

        assert!(differences.iter().any(|diff| diff.contains("lesson_count")));
        assert!(differences.iter().any(|diff| diff.contains("top_lesson")));
        assert!(differences.iter().any(|diff| diff.contains("scope_lens")));
    }
}
