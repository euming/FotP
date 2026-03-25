"""Rich-based live terminal dashboard for the swarm orchestrator.

Replaces the ANSI-escape poll+erase loop in Dashboard with a proper
rich.live.Live render context.  Drop-in replacement: same public interface
as Dashboard (register / unregister / render / clear).

Install dependency if not already present:
    pip install rich
"""
from __future__ import annotations

import json
import sys
import threading
import time
from collections import deque
from dataclasses import dataclass, field
from enum import Enum
from typing import IO, TextIO

from rich.console import Console
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.tree import Tree


# ── Shared agent-status types (moved here from dashboard.py) ──────────────

class Phase(Enum):
    PENDING = "pending"
    THINKING = "thinking"
    TOOL_USE = "tool"
    TEXT = "writing"
    DONE = "done"
    FAILED = "failed"


@dataclass
class AgentStatus:
    """Live status for a single agent, updated by its StreamParser."""
    title: str
    team: str
    model: str = ""
    effort: str = ""
    phase: Phase = Phase.PENDING
    current_tool: str = ""
    elapsed_s: float = 0.0
    cost_usd: float = 0.0
    turns: int = 0
    error: str = ""
    result_preview: str = ""
    _start: float = field(default_factory=time.time)
    tool_log: list = field(default_factory=list)  # [(timestamp, tool_name, duration_s)]
    _tool_start: float = 0.0
    _tool_name: str = ""

    def tick(self) -> None:
        if self.phase not in (Phase.DONE, Phase.FAILED):
            self.elapsed_s = time.time() - self._start

    def format_tool_audit(self) -> str:
        """Format tool call log as a human-readable audit trail."""
        if self._tool_name and self._tool_start:
            self.tool_log.append((self._tool_start, self._tool_name, time.time() - self._tool_start))
            self._tool_name = ""
            self._tool_start = 0.0
        if not self.tool_log:
            return "(no tool calls recorded)"
        lines = [f"  Tool audit trail for '{self.title}' ({len(self.tool_log)} calls, {self.elapsed_s:.0f}s total):"]
        from collections import Counter
        tool_times: dict[str, float] = {}
        tool_counts: Counter = Counter()
        for _ts, name, dur in self.tool_log:
            tool_times[name] = tool_times.get(name, 0.0) + dur
            tool_counts[name] += 1
        for name, count in tool_counts.most_common():
            lines.append(f"    {name}: {count} calls, {tool_times[name]:.1f}s total")
        slowest = sorted(self.tool_log, key=lambda x: x[2], reverse=True)[:5]
        if slowest:
            lines.append("  Slowest calls:")
            for ts, name, dur in slowest:
                rel = ts - self._start
                lines.append(f"    {name} at +{rel:.0f}s: {dur:.1f}s")
        return "\n".join(lines)


class StreamParser(threading.Thread):
    """Background thread that reads stream-json lines from a claude process
    and updates an AgentStatus in real time."""

    def __init__(self, stream: IO[str], status: AgentStatus):
        super().__init__(daemon=True)
        self.stream = stream
        self.status = status
        self._collected: list[str] = []

    def run(self) -> None:
        for raw_line in self.stream:
            raw_line = raw_line.rstrip("\n\r")
            if not raw_line:
                continue
            self._collected.append(raw_line)
            try:
                evt = json.loads(raw_line)
            except json.JSONDecodeError:
                continue
            self._handle(evt)

    def _handle(self, evt: dict) -> None:
        s = self.status
        typ = evt.get("type", "")

        if typ == "assistant":
            msg = evt.get("message", {})
            content = msg.get("content", [])
            for block in content:
                btype = block.get("type", "")
                if btype == "thinking":
                    s.phase = Phase.THINKING
                elif btype == "tool_use":
                    now = time.time()
                    if s._tool_name and s._tool_start:
                        s.tool_log.append((s._tool_start, s._tool_name, now - s._tool_start))
                    s.phase = Phase.TOOL_USE
                    tool_name = block.get("name", "")
                    if tool_name == "Bash":
                        inp = block.get("input", {})
                        cmd = inp.get("command", "") if isinstance(inp, dict) else ""
                        cmd_first = cmd.strip().split("\n")[0].split("|")[0].strip()
                        parts = cmd_first.split()
                        while parts and "=" in parts[0]:
                            parts.pop(0)
                        summary = " ".join(parts)[:60]
                        if summary:
                            tool_name = f"Bash: {summary}"
                    s.current_tool = tool_name
                    s._tool_name = tool_name
                    s._tool_start = now
                elif btype == "text":
                    s.phase = Phase.TEXT
                    text = block.get("text", "")
                    if text:
                        s.result_preview = text[:120]

        elif typ == "result":
            now = time.time()
            if s._tool_name and s._tool_start:
                s.tool_log.append((s._tool_start, s._tool_name, now - s._tool_start))
                s._tool_name = ""
                s._tool_start = 0.0
            s.turns = evt.get("num_turns", s.turns)
            s.cost_usd = evt.get("total_cost_usd", s.cost_usd)
            result = evt.get("result", "")
            if result:
                s.result_preview = str(result)[:120]
            if evt.get("is_error"):
                s.phase = Phase.FAILED
                s.error = str(result)[:200]
            else:
                s.phase = Phase.DONE
            s.elapsed_s = time.time() - s._start

    @property
    def raw_output(self) -> str:
        return "\n".join(self._collected)

    @property
    def result_text(self) -> str:
        for line in reversed(self._collected):
            try:
                evt = json.loads(line)
                if evt.get("type") == "result":
                    return str(evt.get("result", ""))
            except json.JSONDecodeError:
                continue
        return ""


# ── Plan-tree panel ───────────────────────────────────────────────────────

class PlanTreePanel:
    """Renders a rich Tree of swarm-plan nodes in the `plan_tree` region.

    The caller feeds node data via `update(nodes)`.  Each node is a dict with:
        title   : str   — display label
        status  : str   — "pending" | "active" | "done" | "failed"
        children: list  — nested node dicts (same schema, recursive)

    When no data has been supplied yet, a placeholder is shown.
    """

    STATUS_STYLE = {
        "pending": "dim",
        "active":  "bold cyan",
        "done":    "green",
        "failed":  "bold red",
    }
    STATUS_PREFIX = {
        "pending": "○ ",
        "active":  "◑ ",
        "done":    "● ",
        "failed":  "✗ ",
    }

    def __init__(self) -> None:
        self._nodes: list[dict] = []
        self._lock = threading.Lock()

    def update(self, nodes: list[dict]) -> None:
        with self._lock:
            self._nodes = nodes

    def _build_tree(self, parent: Tree, nodes: list[dict]) -> None:
        for node in nodes:
            status = node.get("status", "pending")
            style  = self.STATUS_STYLE.get(status, "")
            prefix = self.STATUS_PREFIX.get(status, "  ")
            label  = Text(f"{prefix}{node.get('title', '?')}", style=style)
            branch = parent.add(label)
            self._build_tree(branch, node.get("children", []))

    def renderable(self) -> Panel:
        with self._lock:
            nodes = list(self._nodes)

        if not nodes:
            placeholder = Text("(no plan data)", style="dim italic")
            return Panel(placeholder, title="Plan Tree", border_style="blue")

        root_tree = Tree(Text("swarm-plan", style="bold blue"))
        self._build_tree(root_tree, nodes)
        return Panel(root_tree, title="Plan Tree", border_style="blue")


# ── Agent-status panel ────────────────────────────────────────────────────

def _fmt_elapsed(seconds: float) -> str:
    if seconds < 60:
        return f"{seconds:.0f}s"
    m, s = divmod(int(seconds), 60)
    return f"{m}m{s:02d}s"


def _agents_table(agents: dict[str, AgentStatus], step: int, max_steps: int) -> Panel:
    table = Table(show_header=True, header_style="bold", box=None, expand=True)
    table.add_column("", width=2)                          # icon
    table.add_column("Team",    style="dim", no_wrap=True)
    table.add_column("Model",   no_wrap=True)
    table.add_column("Title",   no_wrap=True)
    table.add_column("Elapsed", justify="right", no_wrap=True)
    table.add_column("Phase",   no_wrap=True)
    table.add_column("Tool")

    PHASE_ICON: dict[Phase, str] = {
        Phase.PENDING:  "○",
        Phase.THINKING: "◐",
        Phase.TOOL_USE: "◑",
        Phase.TEXT:     "◑",
        Phase.DONE:     "●",
        Phase.FAILED:   "✗",
    }
    PHASE_STYLE: dict[Phase, str] = {
        Phase.PENDING:  "dim",
        Phase.THINKING: "dim white",
        Phase.TOOL_USE: "cyan",
        Phase.TEXT:     "white",
        Phase.DONE:     "green",
        Phase.FAILED:   "bold red",
    }

    for status in agents.values():
        status.tick()
        icon  = PHASE_ICON[status.phase]
        style = PHASE_STYLE[status.phase]
        model_short = status.model.split("-")[1] if "-" in status.model else status.model
        if status.effort:
            model_short = f"{model_short}/{status.effort}"
        title_trunc = status.title[:30]
        tool = ""
        if status.phase == Phase.TOOL_USE and status.current_tool:
            tool = status.current_tool
        elif status.phase == Phase.FAILED and status.error:
            tool = status.error[:60]
        table.add_row(
            Text(icon, style=style),
            status.team,
            model_short,
            title_trunc,
            _fmt_elapsed(status.elapsed_s),
            Text(status.phase.value, style=style),
            Text(tool, style="dim"),
        )

    if not agents:
        table.add_row("", "", "", Text("(no agents)", style="dim italic"), "", "", "")

    title = f"Agents  step {step}/{max_steps}"
    return Panel(table, title=title, border_style="magenta")


# ── Log panel ─────────────────────────────────────────────────────────────

class LogPanel:
    """Collects log lines and renders the most-recent 8 in the log region."""

    def __init__(self) -> None:
        self._lines: deque[str] = deque(maxlen=8)
        self._lock = threading.Lock()

    def append(self, line: str) -> None:
        with self._lock:
            self._lines.append(line)

    def renderable(self) -> Panel:
        with self._lock:
            lines = list(self._lines)
        text = Text("\n".join(lines) if lines else "", style="dim")
        return Panel(text, title="[dim]log[/dim]", border_style="green")


# ── RichDashboard ─────────────────────────────────────────────────────────

class RichDashboard:
    """Drop-in replacement for Dashboard using rich.live.Live.

    Public interface mirrors Dashboard:
        register(status)          — add an AgentStatus
        unregister(title)         — remove by title
        render(step, max_steps)   — refresh the live display
        clear()                   — stop the live context
        log(line)                 — append a line to the log panel
        plan_tree                 — PlanTreePanel instance for caller to update
    """

    def __init__(self, out: TextIO | None = None) -> None:
        console = Console(file=out or sys.stderr, highlight=False)
        self._agents: dict[str, AgentStatus] = {}
        self._lock = threading.Lock()

        self.plan_tree = PlanTreePanel()
        self._log = LogPanel()

        layout = Layout(name="root")
        layout.split_column(
            Layout(name="top", ratio=3),
            Layout(name="log", ratio=1),
        )
        layout["top"].split_row(
            Layout(name="plan_tree", ratio=1),
            Layout(name="agents",    ratio=2),
        )
        self._layout = layout
        self._live = Live(layout, console=console, refresh_per_second=4, screen=False)
        self._live.start(refresh=True)

    # ── Public API ────────────────────────────────────────────────────────

    def register(self, status: AgentStatus) -> None:
        with self._lock:
            self._agents[status.title] = status

    def unregister(self, title: str) -> None:
        with self._lock:
            self._agents.pop(title, None)

    def render(self, step: int = 0, max_steps: int = 0) -> None:
        with self._lock:
            agents_snapshot = dict(self._agents)

        self._layout["plan_tree"].update(self.plan_tree.renderable())
        self._layout["agents"].update(_agents_table(agents_snapshot, step, max_steps))
        self._layout["log"].update(self._log.renderable())

    def update_tree(self, nodes: list[dict]) -> None:
        """Update the plan tree panel.

        Each dict must have at least ``title`` and ``status``.  Status values:
        ``pending``, ``ready``, ``running``, ``done``, ``failed``, ``blocked``
        are mapped to the PlanTreePanel's ``pending`` / ``active`` / ``done`` /
        ``failed`` vocabulary.
        """
        _STATUS_MAP = {
            "running": "active",
            "ready":   "active",
            "done":    "done",
            "failed":  "failed",
        }
        panel_nodes = [
            {
                "title":    n.get("title", "?"),
                "status":   _STATUS_MAP.get(n.get("status", ""), "pending"),
                "children": [],
            }
            for n in nodes
        ]
        self.plan_tree.update(panel_nodes)

    def add_log(self, line: str) -> None:
        self._log.append(line)

    def log(self, line: str) -> None:
        self._log.append(line)

    def clear(self) -> None:
        try:
            self._live.stop()
        except Exception:
            pass


# ── Manual smoke-test ─────────────────────────────────────────────────────

if __name__ == "__main__":
    import random

    dash = RichDashboard()

    # Populate plan tree with sample nodes
    dash.plan_tree.update([
        {"title": "root-task",  "status": "active", "children": [
            {"title": "subtask-A", "status": "done",    "children": []},
            {"title": "subtask-B", "status": "active",  "children": [
                {"title": "leaf-1", "status": "pending", "children": []},
            ]},
        ]},
    ])

    # Register a few fake agents
    statuses = [
        AgentStatus(title="worker-1", team="alpha", model="claude-sonnet-4-6"),
        AgentStatus(title="worker-2", team="alpha", model="claude-sonnet-4-6"),
        AgentStatus(title="worker-3", team="beta",  model="claude-haiku-4-5"),
    ]
    for s in statuses:
        dash.register(s)

    statuses[0].phase = Phase.TOOL_USE
    statuses[0].current_tool = "Bash: scripts/ams.bat"
    statuses[1].phase = Phase.THINKING
    statuses[2].phase = Phase.DONE

    dash.log("[00:01] worker-1 started")
    dash.log("[00:02] worker-2 thinking...")
    dash.log("[00:03] worker-3 completed subtask-A")

    for step in range(1, 6):
        dash.render(step=step, max_steps=5)
        time.sleep(1)

    dash.clear()
    print("Smoke-test complete.")
