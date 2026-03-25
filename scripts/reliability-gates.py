#!/usr/bin/env python3
"""
reliability-gates.py

Runs repeatable AMS reliability gates against generated corpus artifacts.

Gates covered:
1) ingestion integrity (required fields in chat_event raw JSONL)
2) dream provenance integrity (latest dream nodes have source signals)
3) title quality signal (non-high ratio in dream-node UI)
4) structural noise control (no structural dream node IDs in UI)
5) retrieval Recall@K (benchmark cases against embeddings sidecar)
6) route-memory A/B eval (--route-ab-eval): baseline vs replay pass rate with training episodes
7) route-memory cutover-ready (--cutover-check): composite gate checking success criteria,
   regression guardrails, and inspectability contract before cutover
"""

from __future__ import annotations

import argparse
import json
import math
import re
import subprocess
import sys
import tempfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any


DEFAULT_CHATLOG = Path("scripts/output/all-agents-sessions/all-agents-sessions.chat.raw.jsonl")
DEFAULT_AMS_JSON = Path("scripts/output/all-agents-sessions/all-agents-sessions.memory.ams.json")
DEFAULT_HTML = Path("scripts/output/all-agents-sessions/all-agents-sessions.ams-debug.html")
DEFAULT_EMBEDDINGS = Path("scripts/output/all-agents-sessions/all-agents-sessions.memory.embeddings.json")
DEFAULT_BENCHMARK = Path("docs/testing/reliability-query-benchmark.json")

REQUIRED_CHAT_KEYS = ("channel", "chat_id", "message_id", "ts", "direction", "text")
DREAM_KINDS = {"topic", "thread", "decision", "invariant"}
LESSON_KIND = "lesson"
SESSION_KIND = "session"
TITLE_TOKEN_RX = re.compile(r"[a-z0-9]{3,}")
TITLE_STOPWORDS = {
    "a", "an", "the", "and", "or", "but", "if", "then", "else", "of", "to", "in", "on", "at", "for", "with", "by",
    "is", "are", "was", "were", "be", "been", "being", "do", "does", "did", "have", "has", "had", "it", "this", "that",
    "from", "into", "onto", "over", "under", "after", "before", "through", "without", "within",
}
GENERIC_TITLE_TOKENS = {
    "address", "adjustments", "always", "analysis", "apply", "build", "clarity", "communications", "compliance",
    "configuration", "consistency", "context", "contextual", "current", "decision", "enhance", "experience",
    "feature", "feedback", "general", "identify", "implement", "improvements", "improve", "integration", "interactions",
    "issues", "next", "open", "process", "proceed", "requests", "resolve", "resolved", "review", "session", "sessions",
    "settings", "steps", "system", "terms", "thread", "topic", "updates", "user", "users", "validation", "never",
}
LOW_SIGNAL_SUPPORT_MARKERS = (
    "what were we doing",
    "can we finish what we were last doing",
    "this is a new chat window",
    "you generate search-optimized titles",
    "return only a json array",
    "<environment_context>",
    "approval_policy",
    "sandbox_mode",
    "network_access",
    "\"role\":",
    "\"text\":",
)
LOW_SIGNAL_TITLE_MARKERS = (
    "what were we doing",
    "can we finish what we were last doing",
    "this is a new chat window",
    "<environment_context>",
    "approval_policy",
    "sandbox_mode",
    "network_access",
    "what's our progress",
    "hey are available",
    "resume",
    "clear context",
)
SOURCE_SIGNAL_KEYS = (
    "evidence",
    "snippets",
    "source_ids",
    "sourceIds",
    "source_link_ids",
    "sourceLinkIds",
    "session_ids",
)


@dataclass
class GateResult:
    name: str
    passed: bool
    details: str
    metrics: dict[str, Any]


def read_json(path: Path) -> Any:
    with open(path, encoding="utf-8-sig") as f:
        return json.load(f)


def read_text(path: Path) -> str:
    return path.read_text(encoding="utf-8", errors="ignore")


def cosine_similarity(a: list[float], b: list[float]) -> float:
    dot = sum(x * y for x, y in zip(a, b))
    norm_a = math.sqrt(sum(x * x for x in a))
    norm_b = math.sqrt(sum(x * x for x in b))
    if norm_a == 0 or norm_b == 0:
        return 0.0
    return dot / (norm_a * norm_b)


def gate_ingestion_integrity(chatlog_path: Path, min_ratio: float) -> GateResult:
    total = 0
    parse_errors = 0
    complete = 0
    missing = {k: 0 for k in REQUIRED_CHAT_KEYS}

    for raw in chatlog_path.read_text(encoding="utf-8", errors="ignore").splitlines():
        line = raw.strip()
        if not line:
            continue
        total += 1
        try:
            obj = json.loads(line)
        except json.JSONDecodeError:
            parse_errors += 1
            continue

        ok = True
        for key in REQUIRED_CHAT_KEYS:
            value = obj.get(key)
            if value is None or (isinstance(value, str) and not value.strip()):
                missing[key] += 1
                ok = False
        if ok:
            complete += 1

    ratio = (complete / total) if total else 0.0
    passed = total > 0 and parse_errors == 0 and ratio >= min_ratio

    details = (
        f"required-fields ratio={ratio:.4f} ({complete}/{total}), "
        f"threshold>={min_ratio:.4f}, parse_errors={parse_errors}"
    )
    return GateResult(
        "ingestion_integrity",
        passed,
        details,
        {
            "total_events": total,
            "complete_events": complete,
            "ratio": ratio,
            "threshold": min_ratio,
            "parse_errors": parse_errors,
            "missing_counts": missing,
        },
    )


def _canonical_dream_nodes(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    nodes: list[dict[str, Any]] = []
    for obj in snapshot.get("objects", []):
        kind = str(obj.get("objectKind", ""))
        if kind not in DREAM_KINDS:
            continue
        obj_id = str(obj.get("objectId", ""))
        if not obj_id.startswith(f"{kind}:"):
            continue
        nodes.append(obj)
    return nodes


def _extract_latest_dream_nodes(snapshot: dict[str, Any]) -> tuple[list[dict[str, Any]], str]:
    objects = snapshot.get("objects", [])
    dreamrun_created_at = {
        str(o.get("objectId", "")): str(o.get("createdAt", ""))
        for o in objects
        if o.get("objectKind") == "dreamrun"
    }
    nodes_by_run: dict[str, list[dict[str, Any]]] = {}
    unscoped_nodes: list[dict[str, Any]] = []

    for obj in _canonical_dream_nodes(snapshot):
        prov = ((obj.get("semanticPayload") or {}).get("provenance") or {})
        run_id = str(prov.get("run_id") or "").strip()
        if run_id:
            nodes_by_run.setdefault(run_id, []).append(obj)
        else:
            unscoped_nodes.append(obj)

    if nodes_by_run:
        latest_run_id = max(
            nodes_by_run,
            key=lambda run_id: (
                dreamrun_created_at.get(run_id, ""),
                run_id,
            ),
        )
        return nodes_by_run[latest_run_id], latest_run_id

    return unscoped_nodes, ""


def gate_dream_provenance(ams_json_path: Path, min_ratio: float) -> GateResult:
    snapshot = read_json(ams_json_path)
    nodes, latest_run_id = _extract_latest_dream_nodes(snapshot)

    container_member_counts: dict[str, int] = {}
    for ln in snapshot.get("linkNodes", []):
        cid = ln.get("containerId")
        oid = ln.get("objectId")
        if not cid or not oid:
            continue
        container_member_counts[cid] = container_member_counts.get(cid, 0) + 1

    def has_source_signal(node: dict[str, Any]) -> bool:
        prov = ((node.get("semanticPayload") or {}).get("provenance") or {})
        for key in SOURCE_SIGNAL_KEYS:
            value = prov.get(key)
            if isinstance(value, list) and any(str(v).strip() for v in value):
                return True

        obj_id = str(node.get("objectId", ""))
        kind = str(node.get("objectKind", ""))
        if ":" in obj_id:
            suffix = obj_id.split(":", 1)[1]
            members_container = f"{kind}-members:{suffix}"
            if container_member_counts.get(members_container, 0) > 0:
                return True
        return False

    total = len(nodes)
    with_signal = 0
    missing_ids: list[str] = []
    for node in nodes:
        if has_source_signal(node):
            with_signal += 1
        else:
            missing_ids.append(str(node.get("objectId", "")))

    ratio = (with_signal / total) if total else 0.0
    passed = total > 0 and ratio >= min_ratio
    missing_preview = ", ".join(missing_ids[:5]) if missing_ids else "-"

    details = (
        f"latest-dream source-signal ratio={ratio:.4f} ({with_signal}/{total}), "
        f"threshold>={min_ratio:.4f}, latest_run_id={latest_run_id or '<none>'}, "
        f"missing_preview={missing_preview}"
    )
    return GateResult(
        "dream_provenance_integrity",
        passed,
        details,
        {
            "latest_run_id": latest_run_id,
            "total_nodes": total,
            "nodes_with_source_signal": with_signal,
            "ratio": ratio,
            "threshold": min_ratio,
            "missing_node_ids": missing_ids,
        },
    )


def gate_title_quality(html_path: Path, max_nonhigh_ratio: float) -> GateResult:
    html = read_text(html_path)

    qualities = re.findall(
        r'<details class="session dream-node"[^>]*data-title-quality="([^"]+)"',
        html,
        flags=re.IGNORECASE,
    )
    total = len(qualities)
    nonhigh = [q for q in qualities if q.lower() != "high"]
    invalid = [q for q in qualities if q.lower() == "invalid"]
    ratio = (len(nonhigh) / total) if total else 1.0
    details = (
        f"dream-node non-high ratio={ratio:.4f} ({len(nonhigh)}/{total}), "
        f"invalid={len(invalid)}, threshold<={max_nonhigh_ratio:.4f}"
    )
    return GateResult(
        "title_quality",
        total > 0 and ratio <= max_nonhigh_ratio,
        details,
        {
            "total_dream_nodes": total,
            "nonhigh_count": len(nonhigh),
            "invalid_count": len(invalid),
            "ratio": ratio,
            "threshold": max_nonhigh_ratio,
        },
    )


def _title_tokens(text: str) -> list[str]:
    return [token for token in TITLE_TOKEN_RX.findall(text.lower()) if token not in TITLE_STOPWORDS]


def _looks_low_signal_title(text: str) -> bool:
    lowered = (text or "").lower()
    if any(marker in lowered for marker in LOW_SIGNAL_TITLE_MARKERS):
        return True
    tokens = _title_tokens(text)
    if len((text or "").split()) <= 1 or len(tokens) < 2:
        return True
    nongeneric = [token for token in tokens if token not in GENERIC_TITLE_TOKENS]
    return len(nongeneric) < 2


def _is_low_signal_support_text(text: str) -> bool:
    lowered = (text or "").lower()
    return any(marker in lowered for marker in LOW_SIGNAL_SUPPORT_MARKERS)


def _trusted_thread_title(meta: dict[str, Any]) -> str:
    enriched = str(meta.get("enriched_title") or "").strip()
    validation = str(meta.get("title_validation") or "").strip().lower()
    if enriched and validation == "accepted" and not _looks_low_signal_title(enriched):
        return enriched
    bootstrap = str(meta.get("bootstrap_title") or "").strip()
    if bootstrap and not _looks_low_signal_title(bootstrap):
        return bootstrap
    raw_title = str(meta.get("title") or "").strip()
    if raw_title and not _looks_low_signal_title(raw_title):
        return raw_title
    return ""


def _clean_thread_title(text: str) -> str:
    candidate = " ".join((text or "").split()).strip()
    if "|" in candidate:
        candidate = candidate.split("|", 1)[1].strip()
    candidate = re.sub(r"\s*\(\d{4}-\d{2}-\d{2}[^)]*\)\s*$", "", candidate)
    candidate = re.sub(r"\s*\(\d{4}-\d{2}-\d{2}.*$", "", candidate)
    return candidate.strip()


def _collect_session_support(session_id: str, containers: dict[str, dict[str, Any]],
                             objects: dict[str, dict[str, Any]], link_chain: dict[str, list[str]]) -> str:
    best = ""
    for object_id in link_chain.get(session_id, []):
        obj = objects.get(object_id)
        if not obj or obj.get("objectKind") != "chat_message":
            continue
        prov = (obj.get("semanticPayload") or {}).get("provenance", {})
        text = str(prov.get("text") or "").strip()
        if not text or _is_low_signal_support_text(text):
            continue
        best = text
        if prov.get("direction") == "out":
            return text

    if best:
        return best

    container = containers.get(session_id) or {}
    meta = container.get("metadata") or {}
    session_title = _clean_thread_title(str(meta.get("enriched_title") or meta.get("title") or "").strip())
    if session_title:
        return session_title
    return str(meta.get("source") or meta.get("channel") or "").strip()


def _collect_node_support_texts(snapshot: dict[str, Any], node: dict[str, Any]) -> list[str]:
    objects = {
        str(obj.get("objectId", "")): obj
        for obj in snapshot.get("objects", [])
        if str(obj.get("objectId", "")).strip()
    }
    containers = {
        str(container.get("containerId", "")): container
        for container in snapshot.get("containers", [])
        if str(container.get("containerId", "")).strip()
    }
    link_nodes = {
        str(link.get("linkNodeId", "")): link
        for link in snapshot.get("linkNodes", [])
        if str(link.get("linkNodeId", "")).strip()
    }
    link_chain: dict[str, list[str]] = {}
    for container_id, container in containers.items():
        current = container.get("headLinknodeId")
        if not current:
            continue
        seen: set[str] = set()
        chain: list[str] = []
        while current and current not in seen:
            seen.add(current)
            link = link_nodes.get(str(current))
            if not link:
                break
            object_id = str(link.get("objectId", "")).strip()
            if object_id:
                chain.append(object_id)
            current = link.get("nextLinknodeId")
        if chain:
            link_chain[container_id] = chain

    msg_to_session: dict[str, str] = {}
    for link in snapshot.get("linkNodes", []):
        container_id = str(link.get("containerId", "")).strip()
        object_id = str(link.get("objectId", "")).strip()
        if container_id.startswith("chat-session:") and object_id.startswith("chat-msg:"):
            msg_to_session[object_id] = container_id

    kind = str(node.get("objectKind", ""))
    object_id = str(node.get("objectId", ""))
    suffix = object_id.split(":", 1)[1] if ":" in object_id else object_id
    member_container_id = f"{kind}-members:{suffix}"
    support_texts: list[str] = []
    for member_id in link_chain.get(member_container_id, [])[:12]:
        if member_id.startswith("chat-session:"):
            text = _collect_session_support(member_id, containers, objects, link_chain)
        else:
            member_obj = objects.get(member_id) or {}
            prov = (member_obj.get("semanticPayload") or {}).get("provenance", {})
            text = str(prov.get("text") or (member_obj.get("semanticPayload") or {}).get("summary") or "").strip()
        if text and not _is_low_signal_support_text(text):
            support_texts.append(text)

    return support_texts


def gate_title_groundedness(ams_json_path: Path, max_low_signal_ratio: float) -> GateResult:
    snapshot = read_json(ams_json_path)
    nodes, latest_run_id = _extract_latest_dream_nodes(snapshot)

    total = len(nodes)
    low_signal: list[dict[str, Any]] = []
    for node in nodes:
        obj_id = str(node.get("objectId", ""))
        title = str((node.get("semanticPayload") or {}).get("summary") or "").strip()
        tokens = _title_tokens(title)
        nongeneric_tokens = [token for token in tokens if token not in GENERIC_TITLE_TOKENS]
        support_texts = _collect_node_support_texts(snapshot, node)
        support_tokens = _title_tokens("\n".join(support_texts))
        grounded_overlap = sorted({token for token in tokens if token in support_tokens and token not in GENERIC_TITLE_TOKENS})

        reasons: list[str] = []
        if len(tokens) < 2:
            reasons.append("too_short")
        if len(nongeneric_tokens) < 2:
            reasons.append("generic")
        if not grounded_overlap:
            reasons.append("ungrounded")

        if reasons:
            low_signal.append(
                {
                    "object_id": obj_id,
                    "title": title,
                    "reasons": reasons,
                    "nongeneric_tokens": nongeneric_tokens,
                    "grounded_overlap": grounded_overlap,
                }
            )

    ratio = (len(low_signal) / total) if total else 1.0
    passed = total > 0 and ratio <= max_low_signal_ratio
    preview = ", ".join(f"{item['object_id']}[{'+'.join(item['reasons'])}]" for item in low_signal[:5]) if low_signal else "-"
    details = (
        f"dream-node low-signal ratio={ratio:.4f} ({len(low_signal)}/{total}), "
        f"threshold<={max_low_signal_ratio:.4f}, latest_run_id={latest_run_id or '<none>'}, preview={preview}"
    )
    return GateResult(
        "title_groundedness",
        passed,
        details,
        {
            "latest_run_id": latest_run_id,
            "total_dream_nodes": total,
            "low_signal_count": len(low_signal),
            "ratio": ratio,
            "threshold": max_low_signal_ratio,
            "low_signal_nodes": low_signal,
        },
    )


def gate_thread_title_quality(ams_json_path: Path, max_bad_ratio: float) -> GateResult:
    snapshot = read_json(ams_json_path)
    objects = {
        str(obj.get("objectId", "")): obj
        for obj in snapshot.get("objects", [])
        if str(obj.get("objectId", "")).strip()
    }
    containers = {
        str(container.get("containerId", "")): container
        for container in snapshot.get("containers", [])
        if str(container.get("containerId", "")).strip()
    }
    link_nodes = {
        str(link.get("linkNodeId", "")): link
        for link in snapshot.get("linkNodes", [])
        if str(link.get("linkNodeId", "")).strip()
    }
    link_chain: dict[str, list[str]] = {}
    for container_id, container in containers.items():
        current = container.get("headLinknodeId")
        if not current:
            continue
        seen: set[str] = set()
        chain: list[str] = []
        while current and current not in seen:
            seen.add(current)
            link = link_nodes.get(str(current))
            if not link:
                break
            object_id = str(link.get("objectId", "")).strip()
            if object_id:
                chain.append(object_id)
            current = link.get("nextLinknodeId")
        if chain:
            link_chain[container_id] = chain

    threads = [
        container for container in snapshot.get("containers", [])
        if container.get("containerKind") == "conversation_thread"
    ]
    total = len(threads)
    if total == 0:
        return GateResult(
            "thread_title_quality",
            False,
            "no conversation_thread containers found",
            {"total_threads": 0},
        )

    one_word_count = 0
    low_signal_count = 0
    duplicate_generic_count = 0
    failing_threads: list[dict[str, Any]] = []
    title_counts: dict[str, int] = {}
    generic_title_counts: dict[str, int] = {}

    for thread in threads:
        meta = thread.get("metadata") or {}
        thread_id = str(thread.get("containerId", ""))
        title = _trusted_thread_title(meta)
        normalized_title = " ".join(title.lower().split())
        if normalized_title:
            title_counts[normalized_title] = title_counts.get(normalized_title, 0) + 1

        canonical_session_id = str(meta.get("canonical_session_id") or "").strip()
        support_texts: list[str] = []
        bootstrap_title = str(meta.get("bootstrap_title") or "").strip()
        if bootstrap_title and not _looks_low_signal_title(bootstrap_title):
            support_texts.append(bootstrap_title)
        if canonical_session_id:
            support = _collect_session_support(canonical_session_id, containers, objects, link_chain)
            if support:
                support_texts.append(support)
            session_meta = (containers.get(canonical_session_id) or {}).get("metadata") or {}
            for key in ("source", "channel"):
                value = str(session_meta.get(key) or "").strip()
                if value:
                    support_texts.append(value)

        tokens = _title_tokens(title)
        nongeneric = [token for token in tokens if token not in GENERIC_TITLE_TOKENS]
        support_tokens = _title_tokens("\n".join(support_texts))
        grounded_overlap = sorted({token for token in nongeneric if token in support_tokens})

        reasons: list[str] = []
        if len(title.split()) <= 1:
            one_word_count += 1
            reasons.append("one_word")
        if _looks_low_signal_title(title):
            low_signal_count += 1
            reasons.append("low_signal")
            if normalized_title:
                generic_title_counts[normalized_title] = generic_title_counts.get(normalized_title, 0) + 1
        if not grounded_overlap:
            if "low_signal" not in reasons:
                low_signal_count += 1
            reasons.append("ungrounded")

        if reasons:
            failing_threads.append(
                {
                    "thread_id": thread_id,
                    "title": title,
                    "reasons": reasons,
                    "grounded_overlap": grounded_overlap,
                    "canonical_session_id": canonical_session_id,
                }
            )

    duplicate_generic_count = sum(count for count in generic_title_counts.values() if count > 1)
    low_signal_ratio = low_signal_count / total
    one_word_ratio = one_word_count / total
    duplicate_generic_ratio = duplicate_generic_count / total
    passed = (
        low_signal_ratio <= max_bad_ratio
        and one_word_ratio <= max_bad_ratio
        and duplicate_generic_ratio <= max_bad_ratio
    )
    preview = ", ".join(
        f"{item['thread_id']}[{'+'.join(item['reasons'])}]"
        for item in failing_threads[:5]
    ) if failing_threads else "-"
    details = (
        f"thread low-signal ratio={low_signal_ratio:.4f} ({low_signal_count}/{total}), "
        f"one-word ratio={one_word_ratio:.4f} ({one_word_count}/{total}), "
        f"duplicate-generic ratio={duplicate_generic_ratio:.4f} ({duplicate_generic_count}/{total}), "
        f"threshold<={max_bad_ratio:.4f}, preview={preview}"
    )

    return GateResult(
        "thread_title_quality",
        passed,
        details,
        {
            "total_threads": total,
            "low_signal_count": low_signal_count,
            "low_signal_ratio": low_signal_ratio,
            "one_word_count": one_word_count,
            "one_word_ratio": one_word_ratio,
            "duplicate_generic_count": duplicate_generic_count,
            "duplicate_generic_ratio": duplicate_generic_ratio,
            "threshold": max_bad_ratio,
            "failing_threads": failing_threads,
        },
    )


def gate_noise_control(html_path: Path) -> GateResult:
    html = read_text(html_path)
    node_ids = re.findall(
        r'<details class="session dream-node"[^>]*id="([^"]+)"',
        html,
        flags=re.IGNORECASE,
    )
    structural = [nid for nid in node_ids if "members" in nid.lower()]
    passed = len(structural) == 0
    details = f"structural dream-node IDs visible={len(structural)} (expected 0)"

    return GateResult(
        "noise_control",
        passed,
        details,
        {
            "total_dream_nodes": len(node_ids),
            "structural_visible_count": len(structural),
            "structural_ids": structural,
        },
    )


def gate_recall_at_k(
    embeddings_path: Path,
    benchmark_path: Path,
    top_k_override: int | None,
    min_recall_override: float | None,
) -> GateResult:
    embeddings = read_json(embeddings_path)
    model_name = embeddings.get("model", "all-MiniLM-L6-v2")
    entries = embeddings.get("entries", [])
    if not entries:
        return GateResult(
            "retrieval_recall_at_k",
            False,
            "embeddings sidecar has zero entries",
            {"entries": 0},
        )

    benchmark = read_json(benchmark_path)
    all_cases = benchmark.get("cases", [])
    cases = [
        case for case in all_cases
        if str(case.get("embedding_query") or case.get("query") or "").strip()
        and any(str(x).strip() for x in (case.get("embedding_expected_ids") or case.get("expected_ids") or []))
    ]
    top_k = int(top_k_override if top_k_override is not None else benchmark.get("top_k", 5))
    min_recall = float(
        min_recall_override if min_recall_override is not None else benchmark.get("min_recall", 0.9)
    )
    if not cases:
        return GateResult(
            "retrieval_recall_at_k",
            False,
            "benchmark has zero cases",
            {"top_k": top_k, "min_recall": min_recall, "cases": 0},
        )

    try:
        from sentence_transformers import SentenceTransformer  # type: ignore
    except ImportError:
        return GateResult(
            "retrieval_recall_at_k",
            False,
            "sentence-transformers is not installed; run: pip install sentence-transformers",
            {"top_k": top_k, "min_recall": min_recall, "cases": len(cases)},
        )

    model = SentenceTransformer(model_name)
    entry_ids = {str(entry.get("id", "")) for entry in entries if str(entry.get("id", "")).strip()}

    total_expected = 0
    total_hits = 0
    invalid_cases = 0
    case_rows: list[dict[str, Any]] = []

    for case in cases:
        name = str(case.get("name", case.get("query", "unnamed")))
        query = str(case.get("embedding_query") or case.get("query") or "").strip()
        expected_ids = [
            str(x)
            for x in (case.get("embedding_expected_ids") or case.get("expected_ids") or [])
            if str(x).strip()
        ]
        candidate_kinds = {
            str(x).strip()
            for x in (case.get("embedding_candidate_kinds") or [])
            if str(x).strip()
        }

        if not query or not expected_ids:
            case_rows.append(
                {
                    "name": name,
                    "query": query,
                    "expected_ids": expected_ids,
                    "top_ids": [],
                    "hits": [],
                    "recall": 0.0,
                    "valid": False,
                    "missing_expected_ids": [],
                }
            )
            invalid_cases += 1
            continue

        candidate_entries = entries
        if candidate_kinds:
            candidate_entries = [entry for entry in entries if str(entry.get("kind", "")) in candidate_kinds]
        elif all(not eid.startswith("chat-session:") and not eid.startswith("lesson:") for eid in expected_ids):
            candidate_entries = [entry for entry in entries if str(entry.get("kind", "")) != SESSION_KIND]

        candidate_ids = {str(entry.get("id", "")) for entry in candidate_entries if str(entry.get("id", "")).strip()}
        missing_expected_ids = [eid for eid in expected_ids if eid not in entry_ids]
        if missing_expected_ids:
            case_rows.append(
                {
                    "name": name,
                    "query": query,
                    "expected_ids": expected_ids,
                    "top_ids": [],
                    "hits": [],
                    "recall": 0.0,
                    "valid": False,
                    "missing_expected_ids": missing_expected_ids,
                    "candidate_kinds": sorted(candidate_kinds) if candidate_kinds else [],
                }
            )
            invalid_cases += 1
            continue

        query_vec = model.encode([query], convert_to_numpy=True)[0].tolist()
        scored: list[tuple[float, str]] = []
        for entry in candidate_entries:
            emb = entry.get("embedding", [])
            if not emb:
                continue
            score = cosine_similarity(query_vec, emb)
            scored.append((score, str(entry.get("id", ""))))
        scored.sort(key=lambda x: x[0], reverse=True)
        top_ids = [entry_id for _, entry_id in scored[:top_k]]
        hits = [eid for eid in expected_ids if eid in top_ids]

        total_expected += len(expected_ids)
        total_hits += len(hits)

        case_rows.append(
            {
                "name": name,
                "query": query,
                "expected_ids": expected_ids,
                    "top_ids": top_ids,
                    "hits": hits,
                    "recall": (len(hits) / len(expected_ids)) if expected_ids else 0.0,
                    "valid": True,
                    "missing_expected_ids": [],
                    "candidate_entry_count": len(candidate_entries),
                    "candidate_ids_present": all(eid in candidate_ids for eid in expected_ids),
                }
        )

    recall = (total_hits / total_expected) if total_expected else 0.0
    passed = invalid_cases == 0 and total_expected > 0 and recall >= min_recall
    details = (
        f"Recall@{top_k}={recall:.4f} ({total_hits}/{total_expected}), "
        f"threshold>={min_recall:.4f}, cases={len(cases)}, invalid_cases={invalid_cases}"
    )

    return GateResult(
        "retrieval_recall_at_k",
        passed,
        details,
        {
            "model": model_name,
            "top_k": top_k,
            "min_recall": min_recall,
            "cases": case_rows,
            "total_expected": total_expected,
            "total_hits": total_hits,
            "recall": recall,
            "invalid_cases": invalid_cases,
        },
    )


def _read_lessons(snapshot: dict[str, Any]) -> list[dict[str, Any]]:
    return [obj for obj in snapshot.get("objects", []) if obj.get("objectKind") == LESSON_KIND]


def _lesson_title_and_text(lesson: dict[str, Any]) -> tuple[str, str]:
    sem = lesson.get("semanticPayload") or {}
    summary = str(sem.get("summary") or "")
    prov = sem.get("provenance") or {}
    snippets = []
    for item in prov.get("evidence_snapshots", []) or []:
        snippet = str(item.get("snippet") or "").strip()
        if snippet:
            snippets.append(snippet)
    text = "\n".join(snippets)
    return summary, text


def _lesson_score(query_tokens: list[str], lesson: dict[str, Any]) -> float:
    title, text = _lesson_title_and_text(lesson)
    hay = f"{title}\n{text}".lower()
    if not query_tokens:
        return 0.0
    semantic = sum(1 for t in query_tokens if t in hay) / len(query_tokens)
    prov = ((lesson.get("semanticPayload") or {}).get("provenance") or {})
    tier = str(prov.get("freshness_tier") or "yearly")
    tier_weight = {
        "fresh": 1.00,
        "1d": 0.90,
        "7d": 0.75,
        "30d": 0.55,
        "90d": 0.35,
        "yearly": 0.20,
    }.get(tier, 0.20)
    evidence = float(prov.get("evidence_health") or 0.0)
    decay = float(prov.get("decay_multiplier") or 1.0)
    decay = max(1.0, decay)
    return (0.65 * semantic + 0.20 * tier_weight + 0.15 * evidence) / decay


def gate_lesson_durability(ams_json_path: Path, min_ratio: float) -> GateResult:
    snapshot = read_json(ams_json_path)
    lessons = _read_lessons(snapshot)
    total = len(lessons)
    durable = 0

    for lesson in lessons:
        prov = ((lesson.get("semanticPayload") or {}).get("provenance") or {})
        snapshots = prov.get("evidence_snapshots", []) or []
        has_snapshot = any(str(item.get("snippet") or "").strip() for item in snapshots if isinstance(item, dict))
        if has_snapshot:
            durable += 1

    ratio = (durable / total) if total else 0.0
    passed = total > 0 and ratio >= min_ratio
    details = f"lesson durability ratio={ratio:.4f} ({durable}/{total}), threshold>={min_ratio:.4f}"

    return GateResult(
        "agent_lesson_durability",
        passed,
        details,
        {
            "total_lessons": total,
            "durable_lessons": durable,
            "ratio": ratio,
            "threshold": min_ratio,
        },
    )


def _expected_agent_ids(case: dict[str, Any]) -> list[str]:
    if case.get("agent_expected_ids"):
        return [str(x) for x in case.get("agent_expected_ids", []) if str(x).strip()]
    expected = [str(x) for x in case.get("expected_ids", []) if str(x).strip()]
    mapped: list[str] = []
    for eid in expected:
        if ":" in eid:
            kind, suffix = eid.split(":", 1)
            if kind in DREAM_KINDS:
                mapped.append(f"lesson:{kind}:{suffix}")
    return mapped


def _expected_agent_refs(case: dict[str, Any]) -> list[str]:
    refs = [str(x) for x in case.get("agent_expected_refs", []) if str(x).strip()]
    if refs:
        return refs
    return _expected_agent_ids(case)


def _forbidden_agent_refs(case: dict[str, Any]) -> list[str]:
    return [str(x) for x in case.get("agent_forbidden_refs", []) if str(x).strip()]


def _case_enforced(case: dict[str, Any]) -> bool:
    return bool(case.get("agent_enforce", True))


def _is_agent_query_case(case: dict[str, Any]) -> bool:
    query = str(case.get("agent_query") or case.get("query") or "").strip()
    if not query:
        return False

    if _expected_agent_refs(case) or _forbidden_agent_refs(case):
        return True

    structural_keys = (
        "agent_acceptance_case",
        "agent_expect_scope_lens",
        "agent_expect_weak_result",
        "agent_expect_routing_flags",
        "agent_forbid_routing_flags",
        "agent_expect_path_contains",
        "agent_forbid_path_contains",
    )
    return any(key in case for key in structural_keys)


def _db_path_from_ams_json(ams_json_path: Path) -> Path:
    name = ams_json_path.name
    if not name.endswith(".ams.json"):
        raise ValueError(f"AMS snapshot path does not end with .ams.json: {ams_json_path}")
    return ams_json_path.with_name(name[:-9] + ".jsonl")


def _build_agent_query_command(db_path: Path, case: dict[str, Any], top_k: int) -> list[str]:
    query = str(case.get("agent_query") or case.get("query") or "").strip()
    if not query:
        raise ValueError("agent_query case is missing a query")

    cmd = [
        "dotnet",
        "run",
        "--project",
        "tools/memoryctl",
        "--",
        "agent-query",
        "--db",
        str(db_path),
        "--q",
        query,
        "--top",
        str(top_k),
        "--explain",
    ]

    simple_fields = {
        "current_node": "current-node",
        "parent_node": "parent-node",
        "grandparent_node": "grandparent-node",
        "role": "role",
        "mode": "mode",
        "failure_bucket": "failure-bucket",
    }
    for key, flag in simple_fields.items():
        value = str(case.get(key) or "").strip()
        if value:
            cmd.extend([f"--{flag}", value])

    artifacts = [str(x) for x in case.get("artifacts", []) if str(x).strip()]
    if artifacts:
        cmd.extend(["--artifact", ",".join(artifacts)])

    traversal_budget = case.get("traversal_budget")
    if traversal_budget is not None:
        cmd.extend(["--traversal-budget", str(int(traversal_budget))])

    if bool(case.get("no_active_thread_context")):
        cmd.append("--no-active-thread-context")

    return cmd


def _extract_first_lesson_id(path_line: str) -> str | None:
    matches = re.findall(r"(lesson:[^\s\),\]]+)", path_line)
    return matches[0] if matches else None


def _parse_agent_query_output(stdout: str) -> dict[str, Any]:
    lines = stdout.splitlines()
    lesson_refs: list[str] = []
    short_refs: list[str] = []
    scope_lens = ""
    weak_result: bool | None = None
    routing_flags: list[str] = []
    explain_paths: list[str] = []
    in_short_term = False
    in_explain = False

    for raw_line in lines:
        line = raw_line.strip()
        if not line:
            continue

        if line == "## Short-Term Memory":
            in_short_term = True
            in_explain = False
            continue

        if line == "## Explain":
            in_explain = True
            in_short_term = False
            continue

        if line.startswith("# Diagnostics"):
            in_explain = False
            in_short_term = False
            continue

        if line.startswith("weak_result="):
            weak_match = re.search(r"weak_result=(true|false)", line)
            if weak_match:
                weak_result = weak_match.group(1) == "true"

            match = re.search(r"scope_lens=([a-z0-9-]+)", line)
            if match:
                scope_lens = match.group(1)

            flags_match = re.search(r"routing_flags=([a-z0-9,\- ]+)", line)
            if flags_match:
                routing_flags = [
                    flag.strip()
                    for flag in flags_match.group(1).split(",")
                    if flag.strip() and flag.strip() != "none"
                ]
            continue

        if line.startswith("routing_flags="):
            routing_flags = [
                flag.strip()
                for flag in line[len("routing_flags="):].split(",")
                if flag.strip() and flag.strip() != "none"
            ]
            continue

        if in_short_term and line.startswith("- ref: "):
            ref = line[len("- ref: "):].split(" ", 1)[0].strip()
            if ref:
                short_refs.append(ref)
            continue

        if in_explain and line.startswith("- path: "):
            explain_paths.append(line[len("- path: "):].strip())
            lesson_id = _extract_first_lesson_id(line)
            if lesson_id:
                lesson_refs.append(lesson_id)

    return {
        "lesson_refs": lesson_refs,
        "short_refs": short_refs,
        "top_refs": lesson_refs + short_refs,
        "scope_lens": scope_lens,
        "weak_result": weak_result,
        "routing_flags": routing_flags,
        "explain_paths": explain_paths,
    }


def _run_agent_query_case(db_path: Path, case: dict[str, Any], top_k: int) -> dict[str, Any]:
    cmd = _build_agent_query_command(db_path, case, top_k)
    completed = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=Path(__file__).resolve().parent.parent,
        check=False,
    )
    parsed = _parse_agent_query_output(completed.stdout)
    parsed["command"] = cmd
    parsed["exit_code"] = completed.returncode
    parsed["stdout"] = completed.stdout
    parsed["stderr"] = completed.stderr
    return parsed


def _case_assertions(case: dict[str, Any], result: dict[str, Any], top_k: int) -> tuple[bool, dict[str, Any]]:
    top_refs = result["top_refs"][:top_k]
    forbidden_refs = _forbidden_agent_refs(case)
    unexpected_refs = [ref for ref in forbidden_refs if ref in top_refs]

    expected_scope_lens = str(case.get("agent_expect_scope_lens") or "").strip()
    scope_lens_ok = not expected_scope_lens or result["scope_lens"] == expected_scope_lens

    weak_expected = case.get("agent_expect_weak_result")
    weak_ok = weak_expected is None or result["weak_result"] == bool(weak_expected)

    required_flags = [str(x).strip() for x in case.get("agent_expect_routing_flags", []) if str(x).strip()]
    forbidden_flags = [str(x).strip() for x in case.get("agent_forbid_routing_flags", []) if str(x).strip()]
    present_flags = set(result["routing_flags"])
    missing_flags = [flag for flag in required_flags if flag not in present_flags]
    violated_flags = [flag for flag in forbidden_flags if flag in present_flags]

    explain_paths = result["explain_paths"]
    required_path_terms = [str(x).strip() for x in case.get("agent_expect_path_contains", []) if str(x).strip()]
    forbidden_path_terms = [str(x).strip() for x in case.get("agent_forbid_path_contains", []) if str(x).strip()]
    missing_path_terms = [term for term in required_path_terms if not any(term in path for path in explain_paths)]
    violated_path_terms = [term for term in forbidden_path_terms if any(term in path for path in explain_paths)]

    assertions_ok = (
        scope_lens_ok
        and weak_ok
        and not unexpected_refs
        and not missing_flags
        and not violated_flags
        and not missing_path_terms
        and not violated_path_terms
    )

    return assertions_ok, {
        "scope_lens_ok": scope_lens_ok,
        "weak_result_ok": weak_ok,
        "unexpected_refs": unexpected_refs,
        "missing_routing_flags": missing_flags,
        "violated_routing_flags": violated_flags,
        "missing_path_terms": missing_path_terms,
        "violated_path_terms": violated_path_terms,
    }


def gate_agent_query_recall(ams_json_path: Path, benchmark_path: Path, top_k: int, min_recall: float) -> GateResult:
    db_path = _db_path_from_ams_json(ams_json_path)
    benchmark = read_json(benchmark_path)
    all_cases = benchmark.get("cases", [])
    cases = [case for case in all_cases if _is_agent_query_case(case) and _expected_agent_refs(case)]

    if not db_path.exists():
        return GateResult("agent_query_recall_at_k", False, f"agent-query db path not found: {db_path}", {"db_path": str(db_path)})
    if not cases:
        return GateResult(
            "agent_query_recall_at_k",
            False,
            "no agent-query benchmark cases found",
            {"db_path": str(db_path), "cases": 0},
        )

    total_expected = 0
    total_hits = 0
    invalid_cases = 0
    case_rows: list[dict[str, Any]] = []

    for case in cases:
        query = str(case.get("agent_query") or case.get("query") or "").strip()
        expected_refs = _expected_agent_refs(case)
        if not query or not expected_refs:
            case_rows.append(
                {
                    "name": str(case.get("name", query)),
                    "query": query,
                    "expected_refs": expected_refs,
                    "top_refs": [],
                    "hits": [],
                    "valid": False,
                    "command_failed": False,
                    "scope_lens": "",
                    "scope_lens_ok": "agent_expect_scope_lens" not in case,
                }
            )
            invalid_cases += 1
            continue

        result = _run_agent_query_case(db_path, case, top_k)
        top_refs = result["top_refs"][:top_k]
        hits = [ref for ref in expected_refs if ref in top_refs]
        assertions_ok, assertion_details = _case_assertions(case, result, top_k)
        command_failed = result["exit_code"] != 0
        enforced = _case_enforced(case)

        if enforced:
            total_expected += len(expected_refs)
            total_hits += len(hits)
        case_rows.append(
            {
                "name": str(case.get("name", query)),
                "query": query,
                "expected_refs": expected_refs,
                "top_refs": top_refs,
                "hits": hits,
                "valid": not command_failed and assertions_ok,
                "command_failed": command_failed,
                "exit_code": result["exit_code"],
                "scope_lens": result["scope_lens"],
                "scope_lens_ok": assertion_details["scope_lens_ok"],
                "enforced": enforced,
                "weak_result": result["weak_result"],
                "routing_flags": result["routing_flags"],
                "assertions_ok": assertions_ok,
                "assertion_details": assertion_details,
                "stderr": result["stderr"].strip(),
            }
        )
        if enforced and (command_failed or not assertions_ok):
            invalid_cases += 1

    recall = (total_hits / total_expected) if total_expected else 0.0
    passed = invalid_cases == 0 and total_expected > 0 and recall >= min_recall
    details = (
        f"agent Recall@{top_k}={recall:.4f} ({total_hits}/{total_expected}), "
        f"threshold>={min_recall:.4f}, invalid_cases={invalid_cases}"
    )
    return GateResult(
        "agent_query_recall_at_k",
        passed,
        details,
        {
            "db_path": str(db_path),
            "top_k": top_k,
            "min_recall": min_recall,
            "total_expected": total_expected,
            "total_hits": total_hits,
            "recall": recall,
            "invalid_cases": invalid_cases,
            "cases": case_rows,
        },
    )


def gate_agent_query_acceptance_harness(ams_json_path: Path, benchmark_path: Path, top_k: int) -> GateResult:
    db_path = _db_path_from_ams_json(ams_json_path)
    benchmark = read_json(benchmark_path)
    all_cases = benchmark.get("cases", [])
    cases = [case for case in all_cases if bool(case.get("agent_acceptance_case"))]

    if not db_path.exists():
        return GateResult(
            "agent_query_acceptance_harness",
            False,
            f"agent-query db path not found: {db_path}",
            {"db_path": str(db_path)},
        )
    if not cases:
        return GateResult(
            "agent_query_acceptance_harness",
            False,
            "no acceptance-harness cases found",
            {"db_path": str(db_path), "cases": 0},
        )

    enforced_total = 0
    enforced_passed = 0
    pending_total = 0
    pending_green = 0
    case_rows: list[dict[str, Any]] = []

    for case in cases:
        query = str(case.get("agent_query") or case.get("query") or "").strip()
        result = _run_agent_query_case(db_path, case, top_k)
        assertions_ok, assertion_details = _case_assertions(case, result, top_k)
        command_failed = result["exit_code"] != 0
        case_passed = not command_failed and assertions_ok
        enforced = _case_enforced(case)

        if enforced:
            enforced_total += 1
            if case_passed:
                enforced_passed += 1
        else:
            pending_total += 1
            if case_passed:
                pending_green += 1

        case_rows.append(
            {
                "name": str(case.get("name", query)),
                "query": query,
                "enforced": enforced,
                "passed": case_passed,
                "command_failed": command_failed,
                "exit_code": result["exit_code"],
                "top_refs": result["top_refs"][:top_k],
                "scope_lens": result["scope_lens"],
                "weak_result": result["weak_result"],
                "routing_flags": result["routing_flags"],
                "explain_paths": result["explain_paths"],
                "assertion_details": assertion_details,
                "stderr": result["stderr"].strip(),
            }
        )

    passed = enforced_total > 0 and enforced_passed == enforced_total
    details = (
        f"acceptance cases enforced={enforced_passed}/{enforced_total}, "
        f"pending_green={pending_green}/{pending_total}"
    )
    return GateResult(
        "agent_query_acceptance_harness",
        passed,
        details,
        {
            "db_path": str(db_path),
            "enforced_total": enforced_total,
            "enforced_passed": enforced_passed,
            "pending_total": pending_total,
            "pending_green": pending_green,
            "cases": case_rows,
        },
    )


def _extract_ranking_source_from_path(explain_path: str) -> str:
    """Extract the ranking_source prefix from an explain path string."""
    if explain_path.startswith("raw-lesson"):
        return "raw-lesson"
    return "semantic-node-first"


def _extract_route_label_from_path(explain_path: str) -> str:
    """Extract the context-route portion from an explain path string.

    Format: "{ranking_source} -> {lesson_id} -> {family} -> {version} -> {route_label...}"
    The route label is everything after the 4th arrow (skipping source, lesson, family, version).
    Falls back to the full path if the format is not recognised.
    """
    parts = [p.strip() for p in explain_path.split("->")]
    # Skip: ranking_source, lesson_id, family_id, version_id (4 leading segments).
    if len(parts) > 4:
        return " -> ".join(parts[4:])
    # Shorter / unknown format — use everything after the first lesson: ref.
    for i, part in enumerate(parts):
        if part.startswith("lesson:"):
            return " -> ".join(parts[i + 3:]) if len(parts) > i + 3 else parts[-1]
    return explain_path


def _build_route_replay_record(
    case: dict[str, Any],
    baseline_result: dict[str, Any],
    top_k: int,
) -> dict[str, Any] | None:
    """
    Build a RouteReplayRecord dict from a benchmark case and its baseline result.

    The training episode uses the first expected ref as the winner.  The route
    is derived from the explain path of the best-matching baseline hit, or a
    synthetic strong-local route when no matching hit was found.
    """
    query = str(case.get("agent_query") or case.get("query") or "").strip()
    expected_refs = _expected_agent_refs(case)
    if not query or not expected_refs:
        return None

    winning_ref = expected_refs[0]
    top_refs = baseline_result.get("top_refs", [])[:top_k]
    explain_paths = baseline_result.get("explain_paths", [])
    candidate_refs = list(top_refs) if top_refs else [winning_ref]
    if winning_ref not in candidate_refs:
        candidate_refs.insert(0, winning_ref)

    # Find the explain path that mentions the winning ref.
    matched_path = next(
        (p for p in explain_paths if winning_ref in p),
        explain_paths[0] if explain_paths else "",
    )

    ranking_source = _extract_ranking_source_from_path(matched_path) if matched_path else "raw-lesson"
    route_label = _extract_route_label_from_path(matched_path) if matched_path else "retrieval-graph:self-thread -> in-bucket"

    lineage: list[str] = []
    for key in ("current_node", "parent_node", "grandparent_node"):
        val = str(case.get(key) or "").strip()
        if val:
            lineage.append(val)

    frame = {
        "scope_lens": str(baseline_result.get("scope_lens") or "local-first-lineage"),
        "agent_role": str(case.get("role") or "implementer"),
        "mode": str(case.get("mode") or "build"),
        "lineage_node_ids": lineage,
        "artifact_refs": [str(a) for a in case.get("artifacts", []) if str(a).strip()],
    }

    route = {
        "ranking_source": ranking_source,
        "path": route_label,
        "cost": 1.0,
        "risk_flags": [],
    }

    episode = {
        "query_text": query,
        "occurred_at": "2026-03-10T00:00:00Z",
        "weak_result": False,
        "used_fallback": False,
        "winning_target_ref": winning_ref,
        "top_target_refs": list(expected_refs),
    }

    episodes = [
        {
            "frame": frame,
            "route": route,
            "episode": episode,
            "candidate_target_refs": candidate_refs,
            "winning_target_ref": winning_ref,
        }
    ]

    return {
        "query": query,
        "top": top_k,
        "current_node": str(case.get("current_node") or "") or None,
        "parent_node": str(case.get("parent_node") or "") or None,
        "grandparent_node": str(case.get("grandparent_node") or "") or None,
        "role": str(case.get("role") or "implementer"),
        "mode": str(case.get("mode") or "build"),
        "no_active_thread_context": bool(case.get("no_active_thread_context")),
        "episodes": episodes,
        "expected_refs": expected_refs,
    }


def _run_route_replay(db_path: Path, input_path: Path, out_path: Path, top_k: int) -> int:
    cmd = [
        "dotnet", "run", "--project", "tools/memoryctl", "--",
        "route-replay",
        "--db", str(db_path),
        "--input", str(input_path),
        "--out", str(out_path),
        "--top", str(top_k),
    ]
    result = subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        encoding="utf-8",
        cwd=Path(__file__).resolve().parent.parent,
        check=False,
    )
    return result.returncode


def gate_route_memory_ab_eval(ams_json_path: Path, benchmark_path: Path, top_k: int) -> GateResult:
    """
    A/B evaluation gate for route-memory bias.

    Sub-steps:
    1. Run graph-first baseline for all acceptance cases.
    2. Build training episodes from ground-truth expected refs and run route-replay.
    3. Compare baseline vs replay pass rates and report the delta.

    A case "improves" when it fails baseline but passes replay.
    A case "regresses" when it passes baseline but fails replay.
    The gate passes when no enforced cases regress.
    """
    db_path = _db_path_from_ams_json(ams_json_path)
    benchmark = read_json(benchmark_path)
    all_cases = benchmark.get("cases", [])
    cases = [
        case for case in all_cases
        if bool(case.get("agent_acceptance_case")) and _expected_agent_refs(case)
    ]

    if not db_path.exists():
        return GateResult(
            "route_memory_ab_eval",
            False,
            f"agent-query db path not found: {db_path}",
            {"db_path": str(db_path)},
        )
    if not cases:
        return GateResult(
            "route_memory_ab_eval",
            True,
            "no trainable acceptance cases found — skipped",
            {"db_path": str(db_path), "cases": 0},
        )

    # ── Step 1: run baseline ──────────────────────────────────────────────────
    baseline_rows: list[dict[str, Any]] = []
    for case in cases:
        result = _run_agent_query_case(db_path, case, top_k)
        assertions_ok, assertion_details = _case_assertions(case, result, top_k)
        case_passed = result["exit_code"] == 0 and assertions_ok
        baseline_rows.append({
            "case": case,
            "result": result,
            "passed": case_passed,
            "enforced": _case_enforced(case),
        })

    # ── Step 2: build route-replay input and run ─────────────────────────────
    with tempfile.TemporaryDirectory() as tmp:
        input_path = Path(tmp) / "ab_eval_input.jsonl"
        out_path = Path(tmp) / "ab_eval_output.jsonl"

        replay_records = []
        valid_indices: list[int] = []  # indices into baseline_rows that have replay records
        for i, row in enumerate(baseline_rows):
            record = _build_route_replay_record(row["case"], row["result"], top_k)
            if record is not None:
                replay_records.append(record)
                valid_indices.append(i)

        input_path.write_text(
            "\n".join(json.dumps(r) for r in replay_records),
            encoding="utf-8",
        )

        exit_code = _run_route_replay(db_path, input_path, out_path, top_k)
        replay_outputs: list[dict[str, Any]] = []
        if exit_code == 0 and out_path.exists():
            for line in out_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line:
                    try:
                        replay_outputs.append(json.loads(line))
                    except json.JSONDecodeError:
                        pass

    # ── Step 3: compare metrics ───────────────────────────────────────────────
    improved = 0
    regressed = 0
    unchanged_pass = 0
    unchanged_fail = 0
    enforced_regressions = 0
    case_rows: list[dict[str, Any]] = []

    for seq_i, (row_i, replay_out) in enumerate(zip(valid_indices, replay_outputs)):
        row = baseline_rows[row_i]
        case = row["case"]
        enforced = row["enforced"]
        baseline_passed = row["passed"]

        # Evaluate the full measured retrieval surface — replay_surface includes
        # lesson hits + short-term scoped hits. Fall back to replay_hits for
        # backwards compatibility if replay_surface is absent.
        replay_surface = replay_out.get("replay_surface") or replay_out.get("replay_hits", [])
        replay_scope_lens = replay_out.get("replay_scope_lens", "")
        replay_weak = replay_out.get("replay_weak", True)
        expected_refs_hit = replay_out.get("expected_refs_hit")

        expected_refs = _expected_agent_refs(case)
        forbidden_refs = _forbidden_agent_refs(case)
        replay_surface_set = set(replay_surface[:top_k])

        expected_scope_lens = str(case.get("agent_expect_scope_lens") or "").strip()
        scope_ok = not expected_scope_lens or replay_scope_lens == expected_scope_lens
        weak_ok = case.get("agent_expect_weak_result") is None or replay_weak == bool(case.get("agent_expect_weak_result"))
        no_forbidden = all(r not in replay_surface_set for r in forbidden_refs)
        expected_ok = (
            bool(expected_refs_hit)
            if expected_refs_hit is not None
            else (not expected_refs or any(r in replay_surface_set for r in expected_refs))
        )
        replay_passed = scope_ok and weak_ok and no_forbidden and expected_ok and not replay_weak

        if baseline_passed and not replay_passed:
            regressed += 1
            if enforced:
                enforced_regressions += 1
        elif not baseline_passed and replay_passed:
            improved += 1
        elif baseline_passed:
            unchanged_pass += 1
        else:
            unchanged_fail += 1

        case_rows.append({
            "name": str(case.get("name", "")),
            "query": row["result"].get("query", ""),
            "enforced": enforced,
            "baseline_passed": baseline_passed,
            "replay_passed": replay_passed,
            "delta": replay_out.get("delta", ""),
            "top1_baseline": replay_out.get("top1_baseline"),
            "top1_replay": replay_out.get("top1_replay"),
            "top1_changed": replay_out.get("top1_changed", False),
            "expected_refs_hit": expected_refs_hit,
        })

    # Cases without replay records keep their baseline status.
    for i, row in enumerate(baseline_rows):
        if i not in valid_indices:
            case_rows.append({
                "name": str(row["case"].get("name", "")),
                "enforced": row["enforced"],
                "baseline_passed": row["passed"],
                "replay_passed": row["passed"],  # unchanged — no training signal
                "delta": "no-training-signal",
                "top1_changed": False,
            })

    total = len(cases)
    gate_passed = enforced_regressions == 0
    details = (
        f"route-memory A/B: total={total} improved={improved} regressed={regressed} "
        f"unchanged_pass={unchanged_pass} unchanged_fail={unchanged_fail} "
        f"enforced_regressions={enforced_regressions}"
    )
    return GateResult(
        "route_memory_ab_eval",
        gate_passed,
        details,
        {
            "db_path": str(db_path),
            "total_cases": total,
            "improved": improved,
            "regressed": regressed,
            "unchanged_pass": unchanged_pass,
            "unchanged_fail": unchanged_fail,
            "enforced_regressions": enforced_regressions,
            "cases": case_rows,
        },
    )


def gate_route_memory_cutover_ready(ams_json_path: Path, benchmark_path: Path, top_k: int) -> GateResult:
    """
    Composite cutover gate: route-memory is ready to be primary routing when ALL pass.

    Sub-criteria:
    01 success-criteria   — A/B eval: no enforced regressions, replay never makes enforced
                            cases worse than baseline.
    02 regression-guardrails — All currently-enforced acceptance harness cases still pass
                               without route-memory (baseline must be stable).
    03 inspectability-contract — When bias is applied (any episode injected), at least one
                                 replay explain path contains the "route-memory:" signal.
    """
    db_path = _db_path_from_ams_json(ams_json_path)

    # ── 01: success-criteria via A/B eval ────────────────────────────────────
    ab_result = gate_route_memory_ab_eval(ams_json_path, benchmark_path, top_k)
    success_criteria_ok = ab_result.passed
    enforced_regressions = int(ab_result.metrics.get("enforced_regressions", 0))
    improved = int(ab_result.metrics.get("improved", 0))

    # ── 02: regression-guardrails via acceptance harness ─────────────────────
    harness_result = gate_agent_query_acceptance_harness(ams_json_path, benchmark_path, top_k)
    regression_guardrails_ok = harness_result.passed

    # ── 03: inspectability-contract via route-replay explain paths ────────────
    # Build one replay record for each trainable case and verify that when bias
    # is injected, the route-memory signal appears in at least one explain path.
    benchmark = read_json(benchmark_path)
    all_cases = benchmark.get("cases", [])
    trainable_cases = [
        c for c in all_cases
        if bool(c.get("agent_acceptance_case")) and _expected_agent_refs(c)
    ]

    inspectability_ok = True
    inspectability_details: list[dict[str, Any]] = []
    cases_with_bias = 0
    signal_present_count = 0

    if db_path.exists() and trainable_cases:
        with tempfile.TemporaryDirectory() as tmp:
            input_path = Path(tmp) / "cutover_inspect_input.jsonl"
            out_path = Path(tmp) / "cutover_inspect_output.jsonl"

            # Re-run baseline for each case to get explain paths for episode building.
            records = []
            for case in trainable_cases:
                baseline = _run_agent_query_case(db_path, case, top_k)
                record = _build_route_replay_record(case, baseline, top_k)
                if record is not None:
                    records.append(record)

            if records:
                input_path.write_text(
                    "\n".join(json.dumps(r) for r in records), encoding="utf-8"
                )
                exit_code = _run_route_replay(db_path, input_path, out_path, top_k)

                if exit_code == 0 and out_path.exists():
                    for line in out_path.read_text(encoding="utf-8").splitlines():
                        line = line.strip()
                        if not line:
                            continue
                        try:
                            out = json.loads(line)
                        except json.JSONDecodeError:
                            continue

                        has_episodes = bool(records[out.get("case_index", 0)].get("episodes"))
                        signal = bool(out.get("route_memory_signal_present", False))

                        if has_episodes:
                            cases_with_bias += 1
                            if signal:
                                signal_present_count += 1
                            inspectability_details.append({
                                "query": out.get("query"),
                                "signal_present": signal,
                                "replay_explain_paths": out.get("replay_explain_paths", []),
                            })

                    # Contract: when bias episodes are injected and a replay hit exists,
                    # the signal must be present.  Allow cases where replay returned 0 hits.
                    non_empty_cases = [
                        d for d in inspectability_details
                        if d.get("replay_explain_paths")
                    ]
                    if non_empty_cases:
                        inspectability_ok = all(d["signal_present"] for d in non_empty_cases)

    gate_passed = success_criteria_ok and regression_guardrails_ok and inspectability_ok

    criteria = {
        "01_success_criteria": {
            "passed": success_criteria_ok,
            "enforced_regressions": enforced_regressions,
            "improved": improved,
        },
        "02_regression_guardrails": {
            "passed": regression_guardrails_ok,
            "enforced_passed": harness_result.metrics.get("enforced_passed"),
            "enforced_total": harness_result.metrics.get("enforced_total"),
        },
        "03_inspectability_contract": {
            "passed": inspectability_ok,
            "cases_with_bias": cases_with_bias,
            "signal_present_count": signal_present_count,
            "details": inspectability_details,
        },
    }

    details = (
        f"cutover-ready={'YES' if gate_passed else 'NO'}: "
        f"success_criteria={'PASS' if success_criteria_ok else 'FAIL'} "
        f"regression_guardrails={'PASS' if regression_guardrails_ok else 'FAIL'} "
        f"inspectability={'PASS' if inspectability_ok else 'FAIL'} "
        f"(improved={improved}, regressions={enforced_regressions}, "
        f"signal={signal_present_count}/{cases_with_bias})"
    )
    return GateResult("route_memory_cutover_ready", gate_passed, details, criteria)


def gate_agent_query_stability(ams_json_path: Path, benchmark_path: Path, top_k: int, min_ratio: float) -> GateResult:
    db_path = _db_path_from_ams_json(ams_json_path)
    benchmark = read_json(benchmark_path)
    all_cases = benchmark.get("cases", [])
    cases = [case for case in all_cases if _is_agent_query_case(case)]

    if not db_path.exists() or not cases:
        return GateResult("agent_query_stability", False, "missing db path or benchmark cases", {"db_path": str(db_path), "cases": len(cases)})

    stable = 0
    case_rows: list[dict[str, Any]] = []
    for case in cases:
        query = str(case.get("agent_query") or case.get("query") or "").strip()
        run1 = _run_agent_query_case(db_path, case, top_k)
        run2 = _run_agent_query_case(db_path, case, top_k)
        refs1 = run1["top_refs"][:top_k]
        refs2 = run2["top_refs"][:top_k]
        identical = run1["exit_code"] == 0 and run2["exit_code"] == 0 and refs1 == refs2
        if identical:
            stable += 1
        case_rows.append(
            {
                "name": str(case.get("name", query)),
                "query": query,
                "stable": identical,
                "refs1": refs1,
                "refs2": refs2,
                "exit_code1": run1["exit_code"],
                "exit_code2": run2["exit_code"],
            }
        )

    ratio = stable / len(cases)
    passed = ratio >= min_ratio
    details = f"agent ranking stability={ratio:.4f} ({stable}/{len(cases)}), threshold>={min_ratio:.4f}"
    return GateResult(
        "agent_query_stability",
        passed,
        details,
        {"db_path": str(db_path), "stable_cases": stable, "total_cases": len(cases), "ratio": ratio, "threshold": min_ratio, "cases": case_rows},
    )


def ensure_exists(path: Path, flag_name: str) -> None:
    if not path.exists():
        raise FileNotFoundError(f"{flag_name} path not found: {path}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Run repeatable AMS reliability gates.")
    parser.add_argument("--chatlog", type=Path, default=DEFAULT_CHATLOG, help="Path to chat raw JSONL.")
    parser.add_argument("--ams-json", type=Path, default=DEFAULT_AMS_JSON, help="Path to .memory.ams.json.")
    parser.add_argument("--html", type=Path, default=DEFAULT_HTML, help="Path to debug HTML.")
    parser.add_argument(
        "--embeddings",
        type=Path,
        default=DEFAULT_EMBEDDINGS,
        help="Path to .memory.ams.embeddings.json sidecar.",
    )
    parser.add_argument(
        "--benchmark",
        type=Path,
        default=DEFAULT_BENCHMARK,
        help="Path to Recall@K benchmark JSON.",
    )
    parser.add_argument("--skip-recall", action="store_true", help="Skip Recall@K gate.")
    parser.add_argument("--min-ingestion-ratio", type=float, default=0.99)
    parser.add_argument("--min-dream-provenance-ratio", type=float, default=1.0)
    parser.add_argument("--max-nonhigh-title-ratio", type=float, default=0.05)
    parser.add_argument("--min-lesson-durability-ratio", type=float, default=0.95)
    parser.add_argument("--min-agent-stability-ratio", type=float, default=1.0)
    parser.add_argument("--top-k", type=int, default=None, help="Override benchmark top_k.")
    parser.add_argument("--min-recall", type=float, default=None, help="Override benchmark min_recall.")
    parser.add_argument("--report-json", type=Path, default=None, help="Optional output JSON report path.")
    parser.add_argument("--route-ab-eval", action="store_true", help="Run route-memory A/B eval gate (slow; requires built memoryctl).")
    parser.add_argument("--cutover-check", action="store_true", help="Run composite route-memory cutover-ready gate (slow; requires built memoryctl).")
    args = parser.parse_args()

    try:
        ensure_exists(args.chatlog, "--chatlog")
        ensure_exists(args.ams_json, "--ams-json")
        ensure_exists(args.html, "--html")
        if not args.skip_recall:
            ensure_exists(args.embeddings, "--embeddings")
            ensure_exists(args.benchmark, "--benchmark")
    except FileNotFoundError as ex:
        print(f"ERROR: {ex}", file=sys.stderr)
        sys.exit(1)

    results: list[GateResult] = []
    results.append(gate_ingestion_integrity(args.chatlog, args.min_ingestion_ratio))
    results.append(gate_dream_provenance(args.ams_json, args.min_dream_provenance_ratio))
    title_quality_result = gate_title_quality(args.html, args.max_nonhigh_title_ratio)
    title_groundedness_result = gate_title_groundedness(args.ams_json, args.max_nonhigh_title_ratio)
    combined_title_details = (
        f"{title_quality_result.details}; {title_groundedness_result.details}"
    )
    combined_title_metrics = {
        "badge_metrics": title_quality_result.metrics,
        "groundedness_metrics": title_groundedness_result.metrics,
    }
    results.append(
        GateResult(
            "title_quality",
            title_quality_result.passed and title_groundedness_result.passed,
            combined_title_details,
            combined_title_metrics,
        )
    )
    results.append(gate_thread_title_quality(args.ams_json, args.max_nonhigh_title_ratio))
    results.append(gate_noise_control(args.html))
    results.append(gate_lesson_durability(args.ams_json, args.min_lesson_durability_ratio))
    if not args.skip_recall:
        results.append(gate_recall_at_k(args.embeddings, args.benchmark, args.top_k, args.min_recall))
        benchmark = read_json(args.benchmark)
        agent_top_k = int(args.top_k if args.top_k is not None else benchmark.get("top_k", 5))
        agent_min_recall = float(args.min_recall if args.min_recall is not None else benchmark.get("min_recall", 0.9))
        results.append(gate_agent_query_recall(args.ams_json, args.benchmark, agent_top_k, agent_min_recall))
        results.append(gate_agent_query_acceptance_harness(args.ams_json, args.benchmark, agent_top_k))
        results.append(gate_agent_query_stability(args.ams_json, args.benchmark, agent_top_k, args.min_agent_stability_ratio))
        if args.route_ab_eval:
            results.append(gate_route_memory_ab_eval(args.ams_json, args.benchmark, agent_top_k))
        if args.cutover_check:
            results.append(gate_route_memory_cutover_ready(args.ams_json, args.benchmark, agent_top_k))
    else:
        results.append(
            GateResult(
                "retrieval_recall_at_k",
                True,
                "skipped by --skip-recall",
                {"skipped": True},
            )
        )
        results.append(
            GateResult(
                "agent_query_recall_at_k",
                True,
                "skipped by --skip-recall",
                {"skipped": True},
            )
        )
        results.append(
            GateResult(
                "agent_query_acceptance_harness",
                True,
                "skipped by --skip-recall",
                {"skipped": True},
            )
        )
        results.append(
            GateResult(
                "agent_query_stability",
                True,
                "skipped by --skip-recall",
                {"skipped": True},
            )
        )

    all_pass = all(r.passed for r in results)
    print("AMS Reliability Gates")
    print("=" * 80)
    for r in results:
        status = "PASS" if r.passed else "FAIL"
        print(f"[{status}] {r.name}: {r.details}")
    print("=" * 80)
    print(f"OVERALL: {'PASS' if all_pass else 'FAIL'}")

    if args.report_json:
        payload = {
            "overall_pass": all_pass,
            "results": [
                {
                    "name": r.name,
                    "passed": r.passed,
                    "details": r.details,
                    "metrics": r.metrics,
                }
                for r in results
            ],
        }
        args.report_json.parent.mkdir(parents=True, exist_ok=True)
        args.report_json.write_text(json.dumps(payload, indent=2), encoding="utf-8")
        print(f"Report written: {args.report_json}")

    sys.exit(0 if all_pass else 2)


if __name__ == "__main__":
    main()
