"""
enrich-titles.py — LLM-generated titles for AMS conversation threads and dream nodes.

Reads a *.memory.ams.json file and enriches two things:
1. Conversation threads: replaces keyphrase-extracted enriched_title with a
   commit-message-style title (e.g. "Fix timeline duplicate sessions from ingest grouping")
2. Dream overview nodes (topic/thread/decision/invariant): replaces single-word
   extracted keywords with a concise phrase synthesized from the keyword + related session titles

Supports Anthropic (Claude) and OpenAI. Provider is auto-detected from the
available environment variables / flags, or can be set explicitly.

Usage:
    python enrich-titles.py --ams <path.memory.ams.json> [options]

Options:
    --provider deterministic|openai|anthropic   Force provider (default: auto-detect)
    --api-key KEY                 API key (default: $OPENAI_API_KEY or $ANTHROPIC_API_KEY)
    --model ID                    Model ID (default: gpt-4o-mini for OpenAI, claude-haiku-4-5-20251001 for Anthropic)
    --batch-size N                Threads per API call (default: 10)
    --max-chars N                 Max chars per message in context (default: 300)
    --dry-run                     Print titles without writing
    --force                       Re-enrich even if title_source == "lm"
    --repair-threads              Sanitize and repair thread titles in-place even without LLM output
    --skip-threads                Skip conversation thread enrichment
    --skip-dream-nodes            Skip dream overview node enrichment
"""

import argparse
import json
import os
import re
import subprocess
import sys
import tempfile


# ---------------------------------------------------------------------------
# Prompt
# ---------------------------------------------------------------------------

SYSTEM_PROMPT = (
    "You generate search-optimized titles for developer chat sessions stored in a "
    "long-term memory system. Titles are used by an AI agent to find relevant history efficiently."
)

USER_PROMPT_TEMPLATE = """\
For each session below, write a 5-10 word title that captures the OUTCOME or DECISION, not just the task.

Rules:
- Lead with what was discovered, fixed, decided, or built — not what was attempted
- Include the specific file, command, system, or component (e.g. ChatIngestor.cs, enrich-titles.py, dream-all-sessions.bat)
- Use precise technical terms that will match future searches
- Prefer outcome verbs: "Fix", "Discover", "Resolve", "Add", "Refactor", "Wire", "Enable" — not "Discuss", "Work on", "Continue", "Implement plan for"
- If the session ended without resolution, say what was tried and what blocked it (e.g. "Lean import fails due to missing DigitCell export")

Examples of GOOD titles:
  "Fix CLAUDE.local.md not updating after ingest-all-sessions"
  "Discover enrich-titles skips source=all due to missing if-branch"
  "Wire ConversationThreadBuilder into dream command output"
  "Resolve lake build failure from bad Int-label import path"

Examples of BAD titles (too vague):
  "Discuss architecture plan for AMS"
  "Implement code-only memory pipeline update commit plan"
  "Work on Lean syntax issues"

If the first message is "hi" / "what were we doing?" / "let's continue" etc., use the
assistant's reply to infer context — it usually recaps the prior work.

Return ONLY a JSON array: [{{"id": "<id>", "title": "<title>"}}, ...]

Sessions:
{sessions_json}"""

DEFAULT_MODELS = {
    "deterministic": "",
    "openai": "gpt-4o-mini",
    "anthropic": "claude-haiku-4-5-20251001",
    "claude-cli": "claude-haiku-4-5-20251001",
}

DREAM_SYSTEM_PROMPT = (
    "You generate concise labels for dream overview nodes in a developer memory system. "
    "Each node clusters sessions by a common theme. Labels are used by an AI agent to "
    "identify relevant topic clusters at a glance."
)

DREAM_USER_PROMPT_TEMPLATE = """\
Each item below is a dream overview node of kind "{kind}". It has a raw extracted keyword
plus grounded evidence:
- related session titles
- direct evidence snippets from member sessions/messages

Write a specific, searchable label for each node. The label must let a developer decide
whether a session cluster is worth reading WITHOUT opening it.

Grounding rules for every kind:
- Reuse concrete terms that actually appear in RELATED SESSIONS or EVIDENCE SNIPPETS.
- Prefer filenames, commands, subsystem names, repo names, and distinctive technical nouns.
- Do NOT invent a broader theme like "user feedback", "build process", "system improvements", or "current issues" unless those exact concrete terms dominate the evidence.
- If the evidence is mixed, pick the repeated technical thread, not the generic words around it.
- Labels must stay close to the evidence. Generic paraphrases are worse than awkward but concrete labels.

Rules by kind:

  topic:     5-8 words naming the concrete technical area AND its outcome or status.
             Derive from the RELATED SESSIONS list — the keyword is weak noise.
             Include specific filenames, commands, or subsystems.
             GOOD: "AMS ingest deduplication and slug naming fixes"
             GOOD: "dream-all-sessions.bat enrichment pipeline for source=all"
             BAD:  "development work", "various improvements"

  thread:    "Open: <specific blocker or next step>" or "Resolved: <what was completed and how>".
             Name the actual task and the specific file/command/component.
             GOOD: "Resolved: wire ConversationThreadBuilder into dream command output"
             GOOD: "Open: fix DigitCell import missing from FUNDAMENTAL_DEFINITIONS.lean"
             BAD:  "Open: resolve merge conflicts", "Resolved: fix issues"

  decision:  One sentence naming exactly what was chosen and why, with the specific component.
             Format: "<Verb> <specific thing> [because/via/instead of <alternative>]"
             GOOD: "Use os.replace() for atomic *.ams.json writes to prevent corruption"
             GOOD: "Enable enrich-titles.py for source=all, was incorrectly skipped"
             BAD:  "plan implementation", "confirm actions", "decide approach"

  invariant: A standing rule with the specific file, command, or component it governs.
             Format: "Always/Never <specific action> [in/for <specific file or context>]"
             GOOD: "Always run embed-dream-cards.py after enrich-titles to refresh embeddings"
             GOOD: "Never call claude subprocess without unsetting CLAUDECODE env var"
             BAD:  "always ensure stability", "never break things", "must comply"

Return ONLY a JSON array: [{{"id": "<id>", "label": "<label>"}}, ...]

Nodes:
{nodes_json}"""

DREAM_NODE_KINDS = {"topic", "thread", "decision", "invariant"}
GROUNDING_TOKEN_RX = re.compile(r"[a-z0-9]{3,}")
KEYWORD_TOKEN_RX = re.compile(r"[A-Za-z][A-Za-z0-9_.:/-]{2,}")
STOPWORDS = {
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
FALLBACK_NOISE_TOKENS = GENERIC_TITLE_TOKENS | {
    "able", "about", "again", "all", "already", "answers", "any", "branch", "call", "can", "chat", "concrete", "continue",
    "core", "current", "directory", "done", "doing", "dont", "env", "environment", "example", "finish", "first",
    "from", "git", "good", "got", "help", "here", "how", "ill", "im", "ive", "just", "keep", "last", "lake", "left",
    "lets", "look", "make", "more", "msgs", "new", "now", "okay", "one", "our", "out", "quick", "recap", "reply",
    "resume", "right", "said", "see", "show", "small", "some", "still", "sure", "talk", "task", "tell", "thanks",
    "them", "there", "these", "they", "think", "three", "through", "time", "two", "using", "want", "way", "were",
    "what", "when", "where", "which", "why", "window", "work", "worked", "working", "would", "wrote", "yes", "you",
    "your", "text", "role", "assistant", "approval_policy", "sandbox_mode", "network_access", "cwd", "wkspaces",
    "eumin", "provider", "model", "json", "array", "return", "only", "below", "stored", "memory", "long", "term",
    "back", "progress", "available", "token", "tokens",
}
LOW_SIGNAL_SNIPPET_MARKERS = (
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
THREAD_RESUME_PREFIXES = (
    "this is a new chat window",
    "we ran out of tokens",
    "what were we doing",
    "hey do you remember",
    "can we finish what we were last doing",
    "hi, what were we doing",
    "resume",
    "clear context",
)
LOW_SIGNAL_TITLE_MARKERS = THREAD_RESUME_PREFIXES + (
    "<environment_context>",
    "approval_policy",
    "sandbox_mode",
    "network_access",
    "what's our progress",
    "hey are available",
)


# ---------------------------------------------------------------------------
# Provider detection
# ---------------------------------------------------------------------------

def detect_provider(args) -> tuple[str, str]:
    """Return (provider, api_key). api_key may be empty string for claude-cli provider."""
    explicit = getattr(args, "provider", None)

    if explicit == "deterministic":
        return "deterministic", ""

    if explicit == "claude-cli":
        return "claude-cli", ""

    if explicit == "anthropic" or (not explicit and not os.environ.get("OPENAI_API_KEY")):
        key = args.api_key or os.environ.get("ANTHROPIC_API_KEY")
        if key:
            return "anthropic", key

    if explicit == "openai" or not explicit:
        key = args.api_key or os.environ.get("OPENAI_API_KEY")
        if key:
            return "openai", key

    # Fall back to claude-cli (uses Claude Code OAuth session)
    print("No API key found — trying claude-cli provider (OAuth via Claude Code).", file=sys.stderr)
    return "claude-cli", ""


def _call_claude_cli(model: str, system: str, user: str) -> str:
    """Call `claude -p` subprocess and return the response text."""
    prompt = f"{system}\n\n{user}"
    cmd = ["claude", "-p", prompt]
    if model:
        cmd += ["--model", model]
    # Unset CLAUDECODE so the subprocess isn't blocked by the nested-session guard
    env = {k: v for k, v in os.environ.items() if k != "CLAUDECODE"}
    result = subprocess.run(cmd, capture_output=True, text=True, timeout=120, env=env)
    if result.returncode != 0:
        raise RuntimeError(f"claude-cli failed: {result.stderr.strip()}")
    return result.stdout.strip()


def enrich_batch_claude_cli(model: str, batch: list[dict]) -> dict[str, str]:
    sessions_json = json.dumps(batch, indent=2, ensure_ascii=False)
    user_msg = USER_PROMPT_TEMPLATE.format(sessions_json=sessions_json)
    raw = _call_claude_cli(model, SYSTEM_PROMPT, user_msg)
    return _parse_response_text(raw)


def enrich_dream_batch_claude_cli(model: str, kind: str, batch: list[dict]) -> dict[str, str]:
    nodes_json = json.dumps(
        [
            {
                "id": n["id"],
                "keyword": n["keyword"],
                "related_sessions": n["session_titles"],
                "evidence_snippets": n["evidence_snippets"],
            }
            for n in batch
        ],
        indent=2, ensure_ascii=False
    )
    user_msg = DREAM_USER_PROMPT_TEMPLATE.format(kind=kind, nodes_json=nodes_json)
    raw = _call_claude_cli(model, DREAM_SYSTEM_PROMPT, user_msg)
    return _parse_dream_response(raw)


# ---------------------------------------------------------------------------
# AMS traversal helpers
# ---------------------------------------------------------------------------

def walk_chain(container: dict, link_node_map: dict) -> list[str]:
    """Return ordered list of objectIds from a container's head→tail chain."""
    head = container.get("headLinknodeId")
    if not head:
        return []
    chain = []
    curr = head
    visited = set()
    while curr and curr not in visited:
        visited.add(curr)
        ln = link_node_map.get(curr)
        if not ln:
            break
        chain.append(ln["objectId"])
        curr = ln.get("nextLinknodeId")
    return chain


def build_indexes(data: dict):
    obj_map = {o["objectId"]: o for o in data.get("objects", [])}
    container_map = {c["containerId"]: c for c in data.get("containers", [])}
    link_node_map = {ln["linkNodeId"]: ln for ln in data.get("linkNodes", [])}

    # containerId → ordered list of objectIds (walk head → tail)
    link_chain: dict[str, list[str]] = {}
    for c in data.get("containers", []):
        chain = walk_chain(c, link_node_map)
        if chain:
            link_chain[c["containerId"]] = chain

    # objectId (chat-msg) → containerId (chat-session) reverse map
    msg_to_session: dict[str, str] = {}
    for ln in data.get("linkNodes", []):
        cid = ln.get("containerId", "")
        oid = ln.get("objectId", "")
        if cid.startswith("chat-session:") and oid.startswith("chat-msg:"):
            msg_to_session[oid] = cid

    # Also build: session containerId → thread container (for enriched_title lookup)
    # thread containers have canonical_session_id in metadata
    session_to_thread: dict[str, dict] = {}
    for c in data.get("containers", []):
        if c.get("containerKind") == "conversation_thread":
            can_id = (c.get("metadata") or {}).get("canonical_session_id")
            if can_id:
                session_to_thread[can_id] = c

    return obj_map, container_map, link_chain, msg_to_session, session_to_thread


def extract_messages(thread: dict, container_map: dict, obj_map: dict,
                     link_chain: dict, max_chars: int) -> list[dict]:
    """Return up to 4 messages: first user, first assistant, second user, last assistant."""
    can_id = thread.get("metadata", {}).get("canonical_session_id")
    if not can_id:
        return []

    all_oids = link_chain.get(can_id, [])
    msgs = []
    for oid in all_oids:
        obj = obj_map.get(oid)
        if not obj or obj.get("objectKind") != "chat_message":
            continue
        prov = obj.get("semanticPayload", {}).get("provenance", {})
        direction = prov.get("direction", "")
        text = (prov.get("text") or "").strip()
        if not text:
            continue
        role = "user" if direction == "in" else "assistant"
        msgs.append({"role": role, "text": text[:max_chars]})

    if not msgs:
        return []

    selected = []
    seen = set()

    def add(m):
        key = (m["role"], m["text"][:50])
        if key not in seen:
            seen.add(key)
            selected.append(m)

    # First user
    for m in msgs:
        if m["role"] == "user":
            add(m)
            break
    # First assistant
    for m in msgs:
        if m["role"] == "assistant":
            add(m)
            break
    # Second user
    user_count = 0
    for m in msgs:
        if m["role"] == "user":
            user_count += 1
            if user_count == 2:
                add(m)
                break
    # Last assistant
    for m in reversed(msgs):
        if m["role"] == "assistant":
            add(m)
            break

    return selected


# ---------------------------------------------------------------------------
# API calls
# ---------------------------------------------------------------------------

def _parse_response_text(raw: str) -> dict[str, str]:
    """Parse JSON array from model response, stripping markdown fences."""
    raw = raw.strip()
    if raw.startswith("```"):
        lines = raw.splitlines()
        raw = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    titles_list = json.loads(raw)
    return {item["id"]: item["title"] for item in titles_list}


def enrich_batch_anthropic(client, model: str, batch: list[dict]) -> dict[str, str]:
    sessions_json = json.dumps(batch, indent=2, ensure_ascii=False)
    user_msg = USER_PROMPT_TEMPLATE.format(sessions_json=sessions_json)

    response = client.messages.create(
        model=model,
        max_tokens=512,
        system=SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    return _parse_response_text(response.content[0].text)


def enrich_batch_openai(client, model: str, batch: list[dict]) -> dict[str, str]:
    sessions_json = json.dumps(batch, indent=2, ensure_ascii=False)
    user_msg = USER_PROMPT_TEMPLATE.format(sessions_json=sessions_json)

    response = client.chat.completions.create(
        model=model,
        max_tokens=512,
        messages=[
            {"role": "system", "content": SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
    )
    return _parse_response_text(response.choices[0].message.content)


# ---------------------------------------------------------------------------
# Dream node enrichment
# ---------------------------------------------------------------------------

def get_session_title(sess_container: dict) -> str:
    """Extract the best available title from a chat_session container."""
    meta = sess_container.get("metadata") or {}
    enriched = str(meta.get("enriched_title") or "").strip()
    validation = str(meta.get("title_validation") or "").strip().lower()
    if enriched and validation == "accepted":
        return enriched
    t = meta.get("bootstrap_title") or meta.get("title") or sess_container["containerId"]
    return t


def normalize_grounding_tokens(text: str) -> list[str]:
    return [token for token in GROUNDING_TOKEN_RX.findall((text or "").lower()) if token not in STOPWORDS]


def looks_low_signal_title(text: str) -> bool:
    lowered = (text or "").lower()
    if any(marker in lowered for marker in LOW_SIGNAL_TITLE_MARKERS):
        return True
    tokens = normalize_grounding_tokens(text)
    if len(tokens) < 2:
        return True
    nongeneric = [token for token in tokens if token not in GENERIC_TITLE_TOKENS]
    return len(nongeneric) < 2


def normalize_title_candidate(text: str) -> str:
    candidate = re.sub(r"\s+", " ", (text or "").strip())
    candidate = candidate.strip("\"'`")
    return candidate


def strip_bootstrap_suffix(text: str) -> str:
    candidate = normalize_title_candidate(text)
    candidate = re.sub(r"\s*\(\d{4}-\d{2}-\d{2}[^)]*\)\s*$", "", candidate)
    candidate = re.sub(r"\s*\(\d{4}-\d{2}-\d{2}.*$", "", candidate)
    candidate = candidate.replace("...", " ").replace("…", " ")
    candidate = re.sub(r"\s+", " ", candidate).strip(" -|")
    return candidate


def clean_bootstrap_title(text: str) -> str:
    candidate = strip_bootstrap_suffix(text)
    lowered = candidate.lower()
    for prefix in THREAD_RESUME_PREFIXES:
        if lowered.startswith(prefix):
            candidate = candidate[len(prefix):].lstrip(" :,-.")
            break
    if "\n" in candidate:
        lines = [line.strip() for line in candidate.splitlines() if line.strip()]
        if lines:
            candidate = lines[0]
    if candidate.lower().startswith("<environment_context>"):
        return ""
    return normalize_title_candidate(candidate)


def clean_session_title(text: str) -> str:
    candidate = normalize_title_candidate(text)
    if "|" in candidate:
        candidate = candidate.split("|", 1)[1].strip()
    return clean_bootstrap_title(candidate)


def collect_thread_support_texts(thread: dict, messages: list[dict], session_container: dict | None = None) -> tuple[list[str], list[str]]:
    meta = thread.get("metadata", {}) or {}
    real_texts: list[str] = []
    soft_texts: list[str] = []

    for msg in messages:
        text = str(msg.get("text") or "").strip()
        if not text:
            continue
        if not is_low_signal_snippet(text):
            real_texts.append(text)
        soft_texts.append(text)

    if session_container:
        session_meta = session_container.get("metadata", {}) or {}
        session_title = clean_session_title(str(session_meta.get("enriched_title") or session_meta.get("title") or ""))
        if session_title:
            soft_texts.append(session_title)
        for key in ("source", "channel"):
            value = normalize_title_candidate(str(session_meta.get(key) or ""))
            if value:
                soft_texts.append(value)

    bootstrap = clean_bootstrap_title(str(meta.get("bootstrap_title") or ""))
    if bootstrap:
        soft_texts.append(bootstrap)

    raw_title = clean_bootstrap_title(str(meta.get("title") or ""))
    if raw_title:
        soft_texts.append(raw_title)

    return real_texts, soft_texts


def supported_title_overlap(title: str, support_texts: list[str]) -> list[str]:
    title_tokens = normalize_grounding_tokens(title)
    support_tokens = set(normalize_grounding_tokens(" ".join(support_texts)))
    return [
        token for token in title_tokens
        if token in support_tokens and token not in GENERIC_TITLE_TOKENS and token not in FALLBACK_NOISE_TOKENS
    ]


def validate_thread_title(
    title: str,
    thread: dict,
    messages: list[dict],
    session_container: dict | None = None,
) -> tuple[bool, str, str]:
    candidate = normalize_title_candidate(title)
    if not candidate:
        return False, "missing", ""
    if looks_low_signal_title(candidate):
        return False, "low_signal", candidate

    real_support_texts, soft_support_texts = collect_thread_support_texts(thread, messages, session_container)
    tokens = normalize_grounding_tokens(candidate)
    nongeneric = [
        token for token in tokens
        if token not in GENERIC_TITLE_TOKENS and token not in FALLBACK_NOISE_TOKENS
    ]
    overlap = supported_title_overlap(candidate, real_support_texts or soft_support_texts)

    if len(candidate.split()) <= 1:
        return False, "one_word", candidate
    if len(tokens) < 2:
        return False, "too_short", candidate
    if len(nongeneric) < 2:
        return False, "generic", candidate
    if not real_support_texts and not soft_support_texts:
        return False, "missing_support", candidate
    if not overlap:
        return False, "ungrounded", candidate

    return True, "accepted", candidate


def select_weighted_keywords(texts: list[str], limit: int = 8) -> list[str]:
    counts: dict[str, int] = {}
    display: dict[str, str] = {}
    order: list[str] = []

    for text in texts:
        for raw in KEYWORD_TOKEN_RX.findall(text or ""):
            cleaned = raw.strip(".,:;()[]{}<>\"'")
            token = cleaned.lower()
            if len(token) < 3 or token in STOPWORDS or token in FALLBACK_NOISE_TOKENS:
                continue
            if is_likely_identifier_token(token):
                continue
            if token not in counts:
                counts[token] = 0
                display[token] = cleaned
                order.append(token)
            counts[token] += 3 if any(ch in cleaned for ch in "._/-") else 1

    ranked = sorted(order, key=lambda token: (-counts[token], order.index(token)))
    return [display[token] for token in ranked[:limit]]


def build_session_metadata_title(thread: dict, session_container: dict | None, support_texts: list[str]) -> tuple[str, str]:
    if not session_container:
        return "", ""

    session_meta = session_container.get("metadata", {}) or {}
    channel = str(session_meta.get("channel") or "").strip()
    session_title = clean_session_title(str(session_meta.get("title") or ""))
    title_tokens = select_weighted_keywords([session_title], limit=3) if session_title else []
    support_tokens = select_weighted_keywords(support_texts, limit=6)
    agent = ""
    if "/" in channel:
        agent = channel.split("/", 1)[0].replace("-", " ").title()
    elif channel:
        agent = channel.replace("-", " ").title()

    merged: list[str] = []
    for token in title_tokens + support_tokens:
        lower = token.lower()
        if lower in FALLBACK_NOISE_TOKENS or lower in GENERIC_TITLE_TOKENS:
            continue
        if token not in merged:
            merged.append(token)

    if agent and agent not in merged:
        merged.append(agent)

    candidate = normalize_title_candidate(" ".join(merged[:4]))
    if candidate:
        return candidate, "session_metadata"
    if session_title and not looks_low_signal_title(session_title):
        return session_title, "session_metadata"
    return "", ""


def build_thread_fallback_title(
    thread: dict,
    messages: list[dict],
    session_container: dict | None = None,
) -> tuple[str, str]:
    meta = thread.get("metadata", {}) or {}
    bootstrap = clean_bootstrap_title(str(meta.get("bootstrap_title") or ""))
    real_support_texts, soft_support_texts = collect_thread_support_texts(thread, messages, session_container)
    support_texts = real_support_texts or soft_support_texts

    if bootstrap:
        valid, _, normalized = validate_thread_title(bootstrap, thread, messages, session_container)
        if valid:
            return normalized, "bootstrap"

    keywords = select_weighted_keywords(support_texts, limit=6)
    if keywords:
        keyword_title = normalize_title_candidate(" ".join(keywords))
        valid, _, normalized = validate_thread_title(keyword_title, thread, messages, session_container)
        if valid:
            return normalized, "fallback"

    metadata_title, metadata_source = build_session_metadata_title(thread, session_container, support_texts)
    if metadata_title:
        valid, _, normalized = validate_thread_title(metadata_title, thread, messages, session_container)
        if valid:
            return normalized, metadata_source

    raw_title = clean_bootstrap_title(str(meta.get("title") or ""))
    if raw_title and not looks_low_signal_title(raw_title):
        return raw_title, "bootstrap"
    if bootstrap and not looks_low_signal_title(bootstrap):
        return bootstrap, "bootstrap"

    container_id = str(thread.get("containerId") or "conversation-thread")
    suffix = container_id.split(":", 1)[1] if ":" in container_id else container_id
    return f"thread {suffix[:8]}", "fallback"


def apply_thread_title_metadata(
    thread: dict,
    final_title: str,
    title_source: str,
    validation: str,
    candidate_title: str,
    provider: str,
    model: str,
    rejection_reason: str,
) -> bool:
    meta = thread.setdefault("metadata", {})
    before = {
        "enriched_title": str(meta.get("enriched_title") or ""),
        "title_source": str(meta.get("title_source") or ""),
        "title_validation": str(meta.get("title_validation") or ""),
        "title_candidate": str(meta.get("title_candidate") or ""),
        "title_provider": str(meta.get("title_provider") or ""),
        "title_model": str(meta.get("title_model") or ""),
        "title_rejection_reason": str(meta.get("title_rejection_reason") or ""),
    }

    meta["enriched_title"] = final_title
    meta["title_source"] = title_source
    meta["title_validation"] = validation
    meta["title_candidate"] = candidate_title
    meta["title_provider"] = provider
    meta["title_model"] = model
    meta["title_rejection_reason"] = rejection_reason

    after = {
        "enriched_title": str(meta.get("enriched_title") or ""),
        "title_source": str(meta.get("title_source") or ""),
        "title_validation": str(meta.get("title_validation") or ""),
        "title_candidate": str(meta.get("title_candidate") or ""),
        "title_provider": str(meta.get("title_provider") or ""),
        "title_model": str(meta.get("title_model") or ""),
        "title_rejection_reason": str(meta.get("title_rejection_reason") or ""),
    }
    return before != after


def repair_thread_title(
    thread: dict,
    messages: list[dict],
    session_container: dict | None,
    provider: str,
    model: str,
) -> tuple[bool, str]:
    meta = thread.get("metadata", {}) or {}
    current_title = normalize_title_candidate(str(meta.get("enriched_title") or ""))
    candidate_title = current_title or normalize_title_candidate(str(meta.get("title_candidate") or ""))

    valid = False
    rejection_reason = "missing"
    trusted_title = ""
    title_source = str(meta.get("title_source") or "").strip() or "lm"

    if current_title:
        valid, rejection_reason, trusted_title = validate_thread_title(current_title, thread, messages, session_container)
        if not valid:
            trusted_title = ""

    if not trusted_title:
        trusted_title, title_source = build_thread_fallback_title(thread, messages, session_container)
        valid = True
        if rejection_reason == "accepted":
            rejection_reason = ""

    changed = apply_thread_title_metadata(
        thread,
        trusted_title,
        title_source if title_source else "fallback",
        "accepted" if valid else "rejected",
        candidate_title,
        provider,
        model,
        "" if trusted_title == candidate_title else rejection_reason,
    )
    return changed, title_source if title_source else "fallback"


def is_likely_identifier_token(token: str) -> bool:
    if not token or token.isdigit():
        return True
    lower = token.lower()
    if all(ch in "0123456789abcdef" for ch in lower):
        return True
    digit_count = sum(ch.isdigit() for ch in token)
    return len(token) >= 8 and digit_count / len(token) >= 0.45


def is_low_signal_snippet(text: str) -> bool:
    lowered = (text or "").lower()
    if any(marker in lowered for marker in LOW_SIGNAL_SNIPPET_MARKERS):
        return True
    tokens = normalize_grounding_tokens(lowered)
    nongeneric = [token for token in tokens if token not in FALLBACK_NOISE_TOKENS]
    return len(nongeneric) < 2


def score_snippet(text: str) -> int:
    score = 0
    for raw in KEYWORD_TOKEN_RX.findall(text or ""):
        token = raw.strip(".,:;()[]{}<>\"'").lower()
        if len(token) < 3 or token in STOPWORDS or token in FALLBACK_NOISE_TOKENS:
            continue
        if is_likely_identifier_token(token):
            continue
        score += 3 if any(ch in raw for ch in "._/-") else 1
    return score


def collect_session_snippet(session_id: str, container_map: dict, obj_map: dict,
                            link_chain: dict, max_chars: int = 220) -> str:
    messages = link_chain.get(session_id, [])
    first_nonempty = ""
    best_text = ""
    best_score = -1
    for oid in messages:
        obj = obj_map.get(oid)
        if not obj or obj.get("objectKind") != "chat_message":
            continue
        prov = (obj.get("semanticPayload") or {}).get("provenance", {})
        text = str(prov.get("text") or "").strip()
        if not text:
            continue
        if not first_nonempty:
            first_nonempty = text
        if is_low_signal_snippet(text):
            continue
        score = score_snippet(text)
        if prov.get("direction") == "out":
            score += 1
        if score > best_score:
            best_score = score
            best_text = text

    if best_text:
        return best_text[:max_chars]

    if first_nonempty:
        return first_nonempty[:max_chars]

    sess = container_map.get(session_id) or {}
    title = get_session_title(sess) if sess else session_id
    return str(title)[:max_chars]


def build_grounded_fallback_label(kind: str, keyword: str, session_titles: list[str],
                                  evidence_snippets: list[str], object_id: str) -> str:
    candidate_texts = [keyword, *session_titles, *evidence_snippets]
    counts: dict[str, int] = {}
    order: list[str] = []
    for text in candidate_texts:
        for raw in KEYWORD_TOKEN_RX.findall(text or ""):
            token = raw.strip(".,:;()[]{}<>\"'").lower()
            if len(token) < 3 or token in STOPWORDS or token in FALLBACK_NOISE_TOKENS:
                continue
            if is_likely_identifier_token(token):
                continue
            if token not in counts:
                counts[token] = 0
                order.append(token)
            counts[token] += 3 if any(ch in raw for ch in "._/-") else 1

    keywords = sorted(order, key=lambda token: (-counts[token], order.index(token)))[:6]
    suffix = object_id.split(":", 1)[1] if ":" in object_id else object_id
    short_suffix = suffix[:8]

    if kind == "thread":
        if keywords:
            return f"Open: {' '.join(keywords)}"
        return f"Open: {short_suffix}"
    if kind == "decision":
        if keywords:
            return f"Use {' '.join(keywords)}"
        return f"Use {short_suffix}"
    if kind == "invariant":
        lead = "Never" if "never" in normalize_grounding_tokens(keyword + " " + " ".join(evidence_snippets)) else "Always"
        if keywords:
            return f"{lead} {' '.join(keywords)}"
        return f"{lead} {short_suffix}"
    if keywords:
        return " ".join(keywords)
    return f"{kind} {short_suffix}"


def is_grounded_dream_label(label: str, evidence_snippets: list[str], session_titles: list[str]) -> bool:
    label_tokens = normalize_grounding_tokens(label)
    if len(label_tokens) < 2:
        return False
    nongeneric = [token for token in label_tokens if token not in GENERIC_TITLE_TOKENS and token not in FALLBACK_NOISE_TOKENS]
    if len(nongeneric) < 2:
        return False
    evidence_tokens = set(normalize_grounding_tokens(" ".join([*session_titles, *evidence_snippets])))
    grounded_overlap = {
        token
        for token in label_tokens
        if token in evidence_tokens and token not in GENERIC_TITLE_TOKENS and token not in FALLBACK_NOISE_TOKENS
    }
    return bool(grounded_overlap)


def collect_dream_nodes(data: dict, obj_map: dict, container_map: dict, link_chain: dict,
                        msg_to_session: dict, session_to_thread: dict, force: bool) -> list[dict]:
    """
    Return a list of dicts ready for LLM enrichment:
      { id, kind, keyword, session_titles, evidence_snippets, _obj_ref }
    """
    nodes = []
    for obj in data.get("objects", []):
        kind = obj.get("objectKind", "")
        if kind not in DREAM_NODE_KINDS:
            continue
        # Skip the "-members" container objects (e.g. "thread-members:xxx"); only process "thread:xxx"
        if not obj["objectId"].startswith(f"{kind}:"):
            continue
        sp = obj.get("semanticPayload") or {}
        prov = sp.get("provenance") or {}
        # Skip already-enriched unless --force
        if prov.get("enriched_by") == "lm" and not force:
            continue
        keyword = prov.get("label") or sp.get("summary") or ""
        # sig16 is the hex suffix of the objectId after the colon
        oid = obj["objectId"]
        sig16 = oid.split(":", 1)[1] if ":" in oid else oid
        members_cid = f"{kind}-members:{sig16}"
        member_oids = link_chain.get(members_cid, [])

        # Resolve member objectIds -> session titles.
        # Topics link directly to chat-session containers; decisions/invariants/threads
        # link to chat-msg objects — resolve those via msg_to_session.
        seen_sessions: set[str] = set()
        session_titles: list[str] = []
        evidence_snippets: list[str] = []
        for mid in member_oids:
            if mid.startswith("chat-session:"):
                sess_cid = mid
                snippet = collect_session_snippet(sess_cid, container_map, obj_map, link_chain)
            else:
                sess_cid = msg_to_session.get(mid)
                member_obj = obj_map.get(mid) or {}
                prov = (member_obj.get("semanticPayload") or {}).get("provenance", {})
                snippet = str(prov.get("text") or (member_obj.get("semanticPayload") or {}).get("summary") or "").strip()[:220]

            if snippet and not is_low_signal_snippet(snippet):
                evidence_snippets.append(snippet)
                if len(evidence_snippets) > 8:
                    evidence_snippets = evidence_snippets[:8]

            if not sess_cid or sess_cid in seen_sessions:
                continue
            seen_sessions.add(sess_cid)
            # Prefer thread's enriched title; fall back to session container title
            thread = session_to_thread.get(sess_cid)
            if thread:
                title = get_session_title(thread)
            else:
                sess = container_map.get(sess_cid)
                title = get_session_title(sess) if sess else sess_cid
            if title and not looks_low_signal_title(title):
                session_titles.append(title)
            if len(session_titles) >= 8:
                break

        deduped_snippets: list[str] = []
        seen_snippets: set[str] = set()
        for snippet in evidence_snippets:
            normalized = " ".join(snippet.lower().split())
            if not normalized or normalized in seen_snippets:
                continue
            seen_snippets.add(normalized)
            deduped_snippets.append(snippet)
            if len(deduped_snippets) >= 6:
                break

        nodes.append({
            "id": oid,
            "kind": kind,
            "keyword": keyword,
            "session_titles": session_titles,
            "evidence_snippets": deduped_snippets,
            "_obj_ref": obj,
        })
    return nodes


def enrich_dream_batch_openai(client, model: str, kind: str, batch: list[dict]) -> dict[str, str]:
    nodes_json = json.dumps(
        [
            {
                "id": n["id"],
                "keyword": n["keyword"],
                "related_sessions": n["session_titles"],
                "evidence_snippets": n["evidence_snippets"],
            }
            for n in batch
        ],
        indent=2, ensure_ascii=False
    )
    user_msg = DREAM_USER_PROMPT_TEMPLATE.format(kind=kind, nodes_json=nodes_json)
    response = client.chat.completions.create(
        model=model,
        max_tokens=1024,
        messages=[
            {"role": "system", "content": DREAM_SYSTEM_PROMPT},
            {"role": "user", "content": user_msg},
        ],
    )
    return _parse_dream_response(response.choices[0].message.content)


def enrich_dream_batch_anthropic(client, model: str, kind: str, batch: list[dict]) -> dict[str, str]:
    nodes_json = json.dumps(
        [
            {
                "id": n["id"],
                "keyword": n["keyword"],
                "related_sessions": n["session_titles"],
                "evidence_snippets": n["evidence_snippets"],
            }
            for n in batch
        ],
        indent=2, ensure_ascii=False
    )
    user_msg = DREAM_USER_PROMPT_TEMPLATE.format(kind=kind, nodes_json=nodes_json)
    response = client.messages.create(
        model=model,
        max_tokens=1024,
        system=DREAM_SYSTEM_PROMPT,
        messages=[{"role": "user", "content": user_msg}],
    )
    return _parse_dream_response(response.content[0].text)


def _parse_dream_response(raw: str) -> dict[str, str]:
    raw = raw.strip()
    if raw.startswith("```"):
        lines = raw.splitlines()
        raw = "\n".join(lines[1:-1] if lines[-1].strip() == "```" else lines[1:])
    items = json.loads(raw)
    return {item["id"]: item["label"] for item in items}


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def main():
    parser = argparse.ArgumentParser(description="Enrich AMS thread titles via LLM")
    parser.add_argument("--ams", required=True, help="Path to .memory.ams.json")
    parser.add_argument("--provider", choices=["deterministic", "openai", "anthropic", "claude-cli"], default=None,
                        help="LLM provider (default: auto-detect from env; claude-cli uses OAuth via Claude Code)")
    parser.add_argument("--api-key", default=None)
    parser.add_argument("--model", default=None,
                        help="Model ID (default: gpt-4o-mini / claude-haiku-4-5-20251001)")
    parser.add_argument("--batch-size", type=int, default=10)
    parser.add_argument("--max-chars", type=int, default=300)
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--force", action="store_true")
    parser.add_argument("--repair-threads", action="store_true")
    parser.add_argument("--skip-threads", action="store_true")
    parser.add_argument("--skip-dream-nodes", action="store_true")
    args = parser.parse_args()

    provider, api_key = detect_provider(args)
    model = args.model or DEFAULT_MODELS[provider]
    print(f"Provider: {provider}  Model: {model}")

    ams_path = args.ams
    if not os.path.exists(ams_path):
        print(f"ERROR: File not found: {ams_path}", file=sys.stderr)
        sys.exit(1)

    with open(ams_path, encoding="utf-8-sig") as f:
        data = json.load(f)

    obj_map, container_map, link_chain, msg_to_session, session_to_thread = build_indexes(data)

    # Build provider client
    if provider == "deterministic":
        _enrich_thread_batch = lambda b: {}
        _enrich_dream_batch = lambda kind, b: {}
    elif provider == "claude-cli":
        _enrich_thread_batch  = lambda b: enrich_batch_claude_cli(model, b)
        _enrich_dream_batch   = lambda kind, b: enrich_dream_batch_claude_cli(model, kind, b)
    elif provider == "anthropic":
        try:
            import anthropic as _anthropic
        except ImportError:
            print("ERROR: anthropic package not installed. Run: pip install anthropic", file=sys.stderr)
            sys.exit(1)
        client = _anthropic.Anthropic(api_key=api_key)
        _enrich_thread_batch  = lambda b: enrich_batch_anthropic(client, model, b)
        _enrich_dream_batch   = lambda kind, b: enrich_dream_batch_anthropic(client, model, kind, b)
    else:
        try:
            import openai as _openai
        except ImportError:
            print("ERROR: openai package not installed. Run: pip install openai", file=sys.stderr)
            sys.exit(1)
        client = _openai.OpenAI(api_key=api_key)
        _enrich_thread_batch  = lambda b: enrich_batch_openai(client, model, b)
        _enrich_dream_batch   = lambda kind, b: enrich_dream_batch_openai(client, model, kind, b)

    dirty = False

    # ── Pass 1: conversation thread titles ──────────────────────────────────
    if not args.skip_threads:
        threads = [c for c in data["containers"] if c.get("containerKind") == "conversation_thread"]
        deterministic_only = provider == "deterministic"
        to_enrich = []
        if not deterministic_only:
            to_enrich = [
                t for t in threads
                if args.force
                or t.get("metadata", {}).get("title_source") != "lm"
                or t.get("metadata", {}).get("title_validation") != "accepted"
            ]

        print(f"\n[Pass 1] Thread titles: {len(to_enrich)} to enrich / {len(threads)} total")
        t_enriched = t_skipped = t_repaired = 0

        if deterministic_only:
            print("  deterministic provider selected - skipping LLM thread enrichment and running repair only")
        else:
            for batch_start in range(0, len(to_enrich), args.batch_size):
                batch_threads = to_enrich[batch_start: batch_start + args.batch_size]
                batch_payload = []
                for t in batch_threads:
                    msgs = extract_messages(t, container_map, obj_map, link_chain, args.max_chars)
                    slug = t.get("metadata", {}).get("slug", t["containerId"])
                    batch_payload.append({"id": t["containerId"], "slug": slug, "messages": msgs})
                try:
                    results = _enrich_thread_batch(batch_payload)
                except Exception as e:
                    print(f"  WARNING: batch failed: {e}", file=sys.stderr)
                    t_skipped += len(batch_threads)
                    continue
                for t in batch_threads:
                    tid = t["containerId"]
                    title = results.get(tid)
                    if not title:
                        print(f"  WARNING: no title for {tid}", file=sys.stderr)
                        t_skipped += 1
                        continue
                    meta = t.setdefault("metadata", {})
                    meta["enriched_title"] = normalize_title_candidate(title)
                    meta["title_candidate"] = normalize_title_candidate(title)
                    meta["title_source"] = "lm"
                    meta["title_provider"] = provider
                    meta["title_model"] = model
                    meta["title_validation"] = "pending"
                    meta["title_rejection_reason"] = ""
                    t_enriched += 1

        for t in threads:
            msgs = extract_messages(t, container_map, obj_map, link_chain, args.max_chars)
            canonical_session_id = str((t.get("metadata") or {}).get("canonical_session_id") or "").strip()
            session_container = container_map.get(canonical_session_id) if canonical_session_id else None
            changed, source = repair_thread_title(t, msgs, session_container, provider, model)
            slug = t.get("metadata", {}).get("slug", t["containerId"])
            title = t.get("metadata", {}).get("enriched_title", "")
            if changed:
                print(f"  [repair:{source}] [{slug}] {title}")
                if not args.dry_run:
                    dirty = True
                t_repaired += 1

        print(f"  -> Enriched: {t_enriched}, Repaired: {t_repaired}, Skipped: {t_skipped}")
    else:
        print("\n[Pass 1] Thread titles: skipped (--skip-threads)")

    # ── Pass 2: dream overview node labels ──────────────────────────────────
    if provider == "deterministic" and not args.skip_dream_nodes:
        print("\n[Pass 2] Dream nodes: skipped (deterministic provider)")
    elif not args.skip_dream_nodes:
        dream_nodes = collect_dream_nodes(data, obj_map, container_map, link_chain, msg_to_session, session_to_thread, args.force)

        print(f"\n[Pass 2] Dream nodes: {len(dream_nodes)} to enrich")
        d_enriched = d_skipped = 0

        # Group by kind so the prompt examples stay relevant
        from itertools import groupby
        dream_nodes.sort(key=lambda n: n["kind"])
        for kind, group in groupby(dream_nodes, key=lambda n: n["kind"]):
            group_list = list(group)
            for batch_start in range(0, len(group_list), args.batch_size):
                batch = group_list[batch_start: batch_start + args.batch_size]
                try:
                    results = _enrich_dream_batch(kind, batch)
                except Exception as e:
                    print(f"  WARNING: {kind} batch failed: {e}", file=sys.stderr)
                    results = {}
                for n in batch:
                    label = results.get(n["id"])
                    if not label or not is_grounded_dream_label(label, n["evidence_snippets"], n["session_titles"]):
                        label = build_grounded_fallback_label(kind, n["keyword"], n["session_titles"], n["evidence_snippets"], n["id"])
                        print(f"  [{kind}] {n['keyword']!r} -> {label} (grounded fallback)")
                    else:
                        print(f"  [{kind}] {n['keyword']!r} -> {label}")
                    if not args.dry_run:
                        obj = n["_obj_ref"]
                        if obj.get("semanticPayload") is None:
                            obj["semanticPayload"] = {}
                        obj["semanticPayload"]["summary"] = label
                        prov = obj["semanticPayload"].setdefault("provenance", {})
                        prov["enriched_by"] = "lm"
                        dirty = True
                    d_enriched += 1

        print(f"  -> Enriched: {d_enriched}, Skipped: {d_skipped}")
    else:
        print("\n[Pass 2] Dream nodes: skipped (--skip-dream-nodes)")

    if args.dry_run:
        print("\n(dry-run — no changes written)")
        return

    if not dirty:
        print("\nNothing changed.")
        return

    # Atomic write: temp file → rename
    dir_name = os.path.dirname(os.path.abspath(ams_path))
    with tempfile.NamedTemporaryFile("w", encoding="utf-8", dir=dir_name,
                                     suffix=".tmp", delete=False) as tf:
        tmp_path = tf.name
        json.dump(data, tf, ensure_ascii=False, indent=2)

    os.replace(tmp_path, ams_path)
    print(f"\nWritten: {ams_path}")


if __name__ == "__main__":
    main()
