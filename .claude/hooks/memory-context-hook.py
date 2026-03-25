#!/usr/bin/env python3
"""
UserPromptSubmit hook: auto-search AMS agent memory and inject relevant context.

Reads the user prompt from stdin (Claude Code hook JSON), extracts the first
~8 words as a search query, resolves the AMS wrapper through `AMS_MEMORY_CMD`
when set, and outputs a context block that Claude sees before responding.
"""
import json
import subprocess
import sys
import os
import re
import time

# Ensure stdout is UTF-8 on Windows (ke entries may contain non-cp1252 characters)
if hasattr(sys.stdout, 'reconfigure'):
    sys.stdout.reconfigure(encoding='utf-8', errors='replace')

HOOK_DIR = os.path.dirname(os.path.abspath(__file__))
REPO_ROOT = os.path.dirname(os.path.dirname(HOOK_DIR))
SCRIPTS_DIR = os.path.join(REPO_ROOT, "scripts")
if SCRIPTS_DIR not in sys.path:
    sys.path.insert(0, SCRIPTS_DIR)

from ams_common import build_ams_wrapper_cmd, corpus_db, proj_dir_db_path

DB_PATH = corpus_db("all")
AMS_JSON_PATH = os.path.join(
    REPO_ROOT,
    "scripts",
    "output",
    "all-agents-sessions",
    "all-agents-sessions.memory.ams.json",
)
SHORT_TERM_SYNC = os.path.join(REPO_ROOT, "scripts", "sync-short-term-agent-memory.bat")
SHORT_TERM_STAMP = os.path.join(
    REPO_ROOT,
    "scripts",
    "output",
    "all-agents-sessions",
    ".short-term-sync.stamp",
)
MAX_RESULTS = 3
MAX_OUTPUT_CHARS = 800
SHORT_TERM_SYNC_MIN_INTERVAL_SECONDS = 45
TASK_GRAPH_OUTPUT_CHARS = 500
HANDOFF_OUTPUT_CHARS = 700

def extract_query(prompt: str) -> str:
    """Take the first 8 meaningful words from the prompt as the search query."""
    words = re.sub(r'[^\w\s]', ' ', prompt).split()
    # Skip common stop words for a better semantic query
    stop = {'the', 'a', 'an', 'is', 'are', 'was', 'were', 'and', 'or', 'but',
            'in', 'on', 'at', 'to', 'for', 'of', 'with', 'it', 'this', 'that',
            'can', 'you', 'i', 'we', 'my', 'your', 'do', 'did', 'have', 'has',
            'be', 'been', 'will', 'would', 'could', 'should', 'please', 'hey'}
    meaningful = [w for w in words if w.lower() not in stop and len(w) > 2]
    return ' '.join(meaningful[:8])


def is_handoff_prompt(prompt: str) -> bool:
    lowered = prompt.lower()
    hints = (
        'handoff',
        'hand-off',
        'take over',
        'pick up',
        'resume',
        'continue from',
        'new task',
        'not in your context',
        'not in context',
        'another agent',
        'claude',
        'codex',
    )
    return any(hint in lowered for hint in hints)


def prefers_latent_recall(prompt: str) -> bool:
    lowered = prompt.lower()
    hints = (
        "latent",
        "background memory",
        "suppressed",
        "parked",
        "incubation",
        "architecture bucket",
        "smartlist bucket",
        "handoff note",
        "remembered plan",
        "recall",
    )
    return any(hint in lowered for hint in hints)

def load_fep_tool_alerts(max_alerts: int = 5) -> str:
    """Load FEP tool health alerts from the AMS snapshot.

    Surfaces tools with high error rates or chronic failures so agents
    can adjust their behavior at session start.
    """
    if not os.path.exists(AMS_JSON_PATH):
        return ''
    try:
        with open(AMS_JSON_PATH, encoding='utf-8-sig') as f:
            snapshot = json.load(f)
    except Exception:
        return ''

    objects = snapshot.get('objects', {})
    if isinstance(objects, list):
        objects = {o.get('objectId', ''): o for o in objects}

    # Find FEP agent-tool prior objects
    alerts = []
    for obj_id, obj in objects.items():
        if not isinstance(obj_id, str) or not obj_id.startswith('fep:agent-tool:'):
            continue
        tool_name = obj_id.replace('fep:agent-tool:', '')
        sp = obj.get('semantic_payload') or obj.get('semanticPayload') or {}
        prov = sp.get('provenance') or {}
        success = prov.get('success_mean', 1.0)
        error = prov.get('error_mean', 0.0)
        null = prov.get('null_mean', 0.0)
        n = prov.get('n', 0)
        if n < 5:
            continue
        # Flag tools with >15% error or <60% success
        if error > 0.15:
            alerts.append((error, f"- {tool_name}: {error:.0%} error rate ({n} calls)"))
        elif success < 0.6:
            alerts.append((1 - success, f"- {tool_name}: {success:.0%} success rate ({n} calls)"))

    if not alerts:
        return ''

    alerts.sort(key=lambda x: -x[0])
    lines = [a[1] for a in alerts[:max_alerts]]
    return "[FEP Tool Alerts]\n" + "\n".join(lines) + "\n[End FEP alerts]"


def load_working_memory_context(max_items: int = 4) -> str:
    if not os.path.exists(AMS_JSON_PATH):
        return ''

    try:
        with open(AMS_JSON_PATH, encoding='utf-8-sig') as handle:
            snapshot = json.load(handle)
    except Exception:
        return ''

    obj_map = {obj.get('objectId', ''): obj for obj in snapshot.get('objects', [])}
    link_map = {link.get('linkNodeId', ''): link for link in snapshot.get('linkNodes', [])}
    container = next(
        (c for c in snapshot.get('containers', []) if c.get('containerId') == 'working-memory'),
        None,
    )
    if not container:
        return ''

    curr = container.get('headLinknodeId')
    seen: set[str] = set()
    items: list[str] = []
    while curr and curr not in seen and len(items) < max_items:
        seen.add(curr)
        link = link_map.get(curr)
        if not link:
            break
        obj = obj_map.get(link.get('objectId', ''), {})
        payload = obj.get('semanticPayload') or {}
        prov = payload.get('provenance') or {}
        label = payload.get('summary') or prov.get('label') or obj.get('objectId', '')
        kind = obj.get('objectKind', 'memory')
        if label:
            items.append(f"- [{kind}] {label}")
        curr = link.get('nextLinknodeId')

    if not items:
        return ''

    return "[AMS Working Memory Fallback]\n" + "\n".join(items) + "\n[End memory context]"


def load_task_graph_context() -> str:
    cmd = build_ams_wrapper_cmd("thread")

    try:
        result = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=8 if cmd[0].lower().endswith('.exe') else 20,
        )
        output = result.stdout.strip()
    except Exception:
        output = ''

    if not output:
        return ''

    if len(output) > TASK_GRAPH_OUTPUT_CHARS:
        output = output[:TASK_GRAPH_OUTPUT_CHARS] + '\n[...truncated]'

    return f"[AMS Task Graph]\n{output}\n[End task graph]"


def load_handoff_context() -> str:
    cmd = build_ams_wrapper_cmd("handoff", "--depth", "3")

    try:
        result = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=8 if cmd[0].lower().endswith('.exe') else 20,
        )
        output = result.stdout.strip()
    except Exception:
        output = ''

    if not output:
        return ''

    if len(output) > HANDOFF_OUTPUT_CHARS:
        output = output[:HANDOFF_OUTPUT_CHARS] + '\n[...truncated]'

    return f"[AMS Handoff Memory]\n{output}\n[End handoff memory]"


def maybe_refresh_short_term_memory() -> None:
    if not os.path.exists(SHORT_TERM_SYNC):
        return

    try:
        age_seconds = time.time() - os.path.getmtime(SHORT_TERM_STAMP)
        if age_seconds < SHORT_TERM_SYNC_MIN_INTERVAL_SECONDS:
            return
    except OSError:
        pass

    env = os.environ.copy()
    env["AMS_SKIP_HTML"] = "1"
    env["AMS_NO_BROWSER"] = "1"

    try:
        result = subprocess.run(
            [SHORT_TERM_SYNC, "--no-browser"],
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            encoding="utf-8",
            errors="replace",
            timeout=45,
            env=env,
        )
        if result.returncode == 0:
            os.makedirs(os.path.dirname(SHORT_TERM_STAMP), exist_ok=True)
            with open(SHORT_TERM_STAMP, "w", encoding="utf-8") as handle:
                handle.write(str(int(time.time())))
    except Exception:
        return

PROJ_DIR_CONTEXT_MAX_CHARS = 3000
KE_CONTEXT_MAX_CHARS = 2000
# Orientation entries always injected regardless of query score.
# These replace cold-start ls/grep/cat file exploration.
KE_PINNED_SCOPES = ['concept:cold-start-guide', 'concept:project-overview']
KE_PINNED_BUDGET = 700   # chars reserved for pinned entries
KE_RANKED_BUDGET = KE_CONTEXT_MAX_CHARS - KE_PINNED_BUDGET


def _fetch_ke_body(scope: str, max_entries: int = 1, max_chars: int = 350) -> str:
    """Fetch ke entries for a scope; return raw body text (header line stripped)."""
    cmd = build_ams_wrapper_cmd(
        "ke", "context", "--scope", scope,
        "--max-entries", str(max_entries), "--max-chars", str(max_chars),
    )
    try:
        result = subprocess.run(
            cmd, cwd=REPO_ROOT, capture_output=True, text=True,
            encoding='utf-8', errors='replace', timeout=5,
        )
        output = result.stdout.strip()
    except Exception:
        return ''
    if not output or output == "(no entries)":
        return ''
    # Strip "## Knowledge Cache [scope: ...]" header line emitted by ke-context
    lines = output.split('\n')
    if lines and lines[0].startswith('## Knowledge Cache'):
        lines = lines[1:]
    return '\n'.join(lines).strip()


def load_ke_context(query: str, scope: str | None = None, max_chars: int = KE_CONTEXT_MAX_CHARS) -> str:
    """Load Agent Knowledge Cache context for injection into agent prompts.

    Always prepends pinned orientation entries (cold-start-guide, project-overview)
    so agents never need to re-derive project bearings from scratch.  Query-ranked
    entries fill the remaining budget.
    """
    if not proj_dir_db_path().exists():
        return ''

    # --- Pinned orientation entries (always surfaced) ---
    pinned_parts = [_fetch_ke_body(s, max_entries=1, max_chars=350) for s in KE_PINNED_SCOPES]
    pinned_body = '\n'.join(p for p in pinned_parts if p)

    # --- Query-ranked entries (fill remaining budget) ---
    ranked_budget = max(max_chars - len(pinned_body) - 60, 400)
    cmd = build_ams_wrapper_cmd("ke", "context", "--max-entries", "6", "--max-chars", str(ranked_budget))
    if scope:
        cmd = cmd + ["--scope", scope]
    try:
        result = subprocess.run(
            cmd, cwd=REPO_ROOT, capture_output=True, text=True,
            encoding='utf-8', errors='replace', timeout=5,
        )
        ranked_output = result.stdout.strip()
    except Exception:
        ranked_output = ''
    if ranked_output == "(no entries)":
        ranked_output = ''

    # --- Combine under one header ---
    header = "## Knowledge Cache [scope: all]\n"
    # Build ranked body, stripping header and deduping entries already in pinned
    ranked_body = ''
    if ranked_output:
        lines = ranked_output.split('\n')
        if lines and lines[0].startswith('## Knowledge Cache'):
            lines = lines[1:]
        # Dedup: skip ranked entry lines whose first 40 chars appear in pinned_body
        seen_sigs = set()
        for chunk in pinned_body.split('\n'):
            sig = chunk.strip()[:40]
            if sig:
                seen_sigs.add(sig)
        filtered = []
        for line in lines:
            if line.startswith('[') and line.strip()[:40] in seen_sigs:
                continue
            filtered.append(line)
        ranked_body = '\n'.join(filtered).strip()

    body_parts = [p for p in [pinned_body, ranked_body] if p]
    body = '\n'.join(body_parts)
    if not body:
        return ''
    full = header + body
    if len(full) > max_chars:
        full = full[:max_chars] + '\n[...truncated]'
    return f"[Agent Knowledge Cache]\n{full}\n[End knowledge cache]"


def load_proj_dir_context(max_chars: int = PROJ_DIR_CONTEXT_MAX_CHARS) -> str:
    """Load compact project directory context from proj_dir.db."""
    if not proj_dir_db_path().exists():
        return ''
    cmd = build_ams_wrapper_cmd("proj-dir", "context")
    try:
        result = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=10,
        )
        output = result.stdout.strip()
    except Exception:
        output = ''
    if not output:
        return ''
    if len(output) > max_chars:
        output = output[:max_chars] + '\n[...truncated]'
    return f"[Project Directory Index]\n{output}\n[End project directory]"


CALLSTACK_CONTEXT_MAX_CHARS = 1500
ATLAS_MAX_RESULTS = 3
ATLAS_MAX_OUTPUT_CHARS = 600


def load_atlas_context(query: str) -> str:
    """Run the AMS wrapper atlas search command and return formatted output."""
    cmd = build_ams_wrapper_cmd("atlas", "search", query, "--top", str(ATLAS_MAX_RESULTS))
    try:
        result = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=10,
        )
        output = result.stdout.strip()
    except Exception:
        output = ''
    if not output:
        return ''
    if len(output) > ATLAS_MAX_OUTPUT_CHARS:
        output = output[:ATLAS_MAX_OUTPUT_CHARS] + '\n[...truncated]'
    return '[AMS Atlas Search: "' + query + '"]\n' + output + '\n[End atlas context]'


def load_swarm_plan_context(max_chars: int = CALLSTACK_CONTEXT_MAX_CHARS) -> str:
    cmd = build_ams_wrapper_cmd("swarm-plan", "context", "--max-chars", str(max_chars))
    try:
        result = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=8 if cmd[0].lower().endswith('.exe') else 20,
        )
        output = result.stdout.strip()
    except Exception:
        output = ''
    if output and "[AMS Callstack Context]" in output:
        return output
    return ''


def main():
    # Skip hook for swarm worker agents to avoid lock contention / stalls
    if os.environ.get("AMS_HOOK_SKIP"):
        sys.exit(0)

    try:
        raw = sys.stdin.read()
        data = json.loads(raw)
        prompt = data.get('prompt', '')
    except Exception:
        sys.exit(0)  # Don't block on parse error

    if not prompt or len(prompt) < 10:
        sys.exit(0)

    query = extract_query(prompt)
    if not query:
        sys.exit(0)

    if not os.path.exists(DB_PATH):
        sys.exit(0)

    maybe_refresh_short_term_memory()

    # Callstack-first: if an active swarm-plan exists, use it as primary context
    swarm_plan_ctx = load_swarm_plan_context()
    if swarm_plan_ctx:
        parts = [swarm_plan_ctx]
        if is_handoff_prompt(prompt):
            handoff_context = load_handoff_context()
            if handoff_context:
                parts.append(handoff_context)
        atlas_ctx_sp = load_atlas_context(query)
        if atlas_ctx_sp:
            parts.append(atlas_ctx_sp)
        ke_block = load_ke_context(query)
        if ke_block:
            parts.append(ke_block)
        fep_alerts = load_fep_tool_alerts()
        if fep_alerts:
            parts.append(fep_alerts)
        print("\n\n".join(parts))
        sys.exit(0)

    # Legacy fallback: task graph + search/recall + handoff + proj-dir onboarding
    proj_dir_context = load_proj_dir_context()
    handoff_context = load_handoff_context() if is_handoff_prompt(prompt) else ''

    atlas_ctx = load_atlas_context(query)

    recall_command = "recall" if prefers_latent_recall(prompt) else "search"
    cmd = build_ams_wrapper_cmd(recall_command, query, "--top", str(MAX_RESULTS))

    try:
        result = subprocess.run(
            cmd,
            cwd=REPO_ROOT,
            capture_output=True,
            text=True,
            encoding='utf-8',
            errors='replace',
            timeout=12 if cmd[0].lower().endswith('.exe') else 30,
        )
        output = result.stdout.strip()
    except Exception:
        output = ''

    if not output:
        output = load_working_memory_context()
    task_graph = load_task_graph_context()
    if not output and not task_graph:
        sys.exit(0)

    if output:
        # Truncate to avoid flooding context window
        if len(output) > MAX_OUTPUT_CHARS:
            output = output[:MAX_OUTPUT_CHARS] + '\n[...truncated]'

    if not output:
        memory_context = ''
    elif output.startswith("[AMS Working Memory Fallback]"):
        memory_context = output
    else:
        memory_context = f"[AMS Memory Search: \"{query}\"]\n{output}\n[End memory context]"

    ke_block = load_ke_context(query)
    fep_alerts = load_fep_tool_alerts()
    context = "\n\n".join(part for part in (proj_dir_context, task_graph, handoff_context, atlas_ctx, ke_block, memory_context, fep_alerts) if part)
    print(context)
    sys.exit(0)

if __name__ == '__main__':
    main()
