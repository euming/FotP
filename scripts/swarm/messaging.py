"""Inter-agent message queue backed by SmartList buckets.

Messages live at smartlist/message-queue/{channel}.  Each message is a
SmartList note whose title encodes ``msg:{sender}->{recipient}:{timestamp}``
and whose text carries the full JSON envelope (sender, recipient, channel,
timestamp, payload).
"""
from __future__ import annotations

import json
import sys
from datetime import datetime, timezone
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parent.parent))
from ams_common import build_rust_ams_cmd, rust_backend_env

import subprocess

MESSAGE_QUEUE_ROOT = "smartlist/message-queue"


def _run_kernel(backend_root: str | None, *args: str) -> subprocess.CompletedProcess[str]:
    cmd = build_rust_ams_cmd(*args)
    if cmd is None:
        raise RuntimeError("unable to locate the Rust AMS kernel binary or Cargo project")
    return subprocess.run(
        cmd,
        env=rust_backend_env(backend_root),
        text=True,
        capture_output=True,
        check=False,
    )


def _parse_kv(stdout: str) -> dict[str, str]:
    values: dict[str, str] = {}
    for line in stdout.splitlines():
        if "=" not in line:
            continue
        key, value = line.split("=", 1)
        values[key.strip()] = value.strip()
    return values


def bootstrap_message_queue(
    input_path: str,
    channel: str,
    backend_root: str | None = None,
) -> str:
    """Create the bucket for a message channel.

    Creates:
      smartlist/message-queue/{channel}

    Returns the bucket path.
    """
    bucket_path = f"{MESSAGE_QUEUE_ROOT}/{channel}"
    result = _run_kernel(
        backend_root,
        "smartlist-create",
        "--input", input_path,
        "--path", bucket_path,
    )
    if result.returncode != 0:
        raise RuntimeError(f"smartlist-create {bucket_path} failed: {result.stderr}")
    return bucket_path


PRIORITY_LEVELS = ("low", "normal", "high", "critical")


def send_message(
    input_path: str,
    channel: str,
    sender: str,
    recipient: str,
    payload: str,
    backend_root: str | None = None,
    timestamp: str | None = None,
    priority: str = "normal",
) -> str:
    """Send a message on the given channel.

    *priority* must be one of: low, normal, high, critical.
    Returns the note ID assigned by the kernel.
    """
    if priority not in PRIORITY_LEVELS:
        raise ValueError(f"priority must be one of {PRIORITY_LEVELS}, got {priority!r}")
    bucket_path = f"{MESSAGE_QUEUE_ROOT}/{channel}"
    ts = timestamp or datetime.now(timezone.utc).isoformat()

    title = f"msg:{sender}->{recipient}:{ts}"
    text = json.dumps({
        "sender": sender,
        "recipient": recipient,
        "channel": channel,
        "timestamp": ts,
        "payload": payload,
        "priority": priority,
    })

    result = _run_kernel(
        backend_root,
        "smartlist-note",
        "--input", input_path,
        "--title", title,
        "--text", text,
        "--buckets", bucket_path,
    )
    if result.returncode != 0:
        raise RuntimeError(f"smartlist-note in {bucket_path} failed: {result.stderr}")

    data = _parse_kv(result.stdout)
    return data.get("note_id", "")


def receive_messages(
    input_path: str,
    channel: str,
    recipient: str,
    backend_root: str | None = None,
) -> list[dict]:
    """Receive all messages addressed to *recipient* on the given channel.

    Browses the channel bucket and returns messages whose title encodes the
    recipient.  Each returned dict has keys: sender, recipient, channel,
    timestamp, payload, note_id.
    """
    bucket_path = f"{MESSAGE_QUEUE_ROOT}/{channel}"
    result = _run_kernel(
        backend_root,
        "smartlist-browse",
        "--input", input_path,
        "--path", bucket_path,
    )
    if result.returncode != 0:
        raise RuntimeError(f"smartlist-browse {bucket_path} failed: {result.stderr}")

    messages: list[dict] = []
    target_suffix = f"->{recipient}:"
    for line in result.stdout.splitlines():
        kv = _parse_kv_line(line)
        if not kv:
            continue
        name = kv.get("name", "")
        # Title format: msg:{sender}->{recipient}:{timestamp}
        if target_suffix not in name:
            continue
        obj_id = kv.get("object_id", "")
        # Parse sender and timestamp from the title
        try:
            after_msg = name.split("msg:", 1)[1]
            sender_part, rest = after_msg.split(f"->{recipient}:", 1)
            ts_part = rest
        except (IndexError, ValueError):
            continue
        messages.append({
            "sender": sender_part,
            "recipient": recipient,
            "channel": channel,
            "timestamp": ts_part,
            "payload": None,  # full payload requires note-read (not yet available)
            "note_id": obj_id,
        })
    return messages


def _parse_kv_line(line: str) -> dict[str, str]:
    """Parse a single browse output line of the form ``key=val key2=val2``."""
    parts: dict[str, str] = {}
    for token in line.split():
        if "=" in token:
            k, v = token.split("=", 1)
            parts[k] = v
    return parts


def list_channels(
    input_path: str,
    backend_root: str | None = None,
) -> list[str]:
    """Return the names of all message-queue channels that have been created."""
    result = _run_kernel(
        backend_root,
        "smartlist-browse",
        "--input", input_path,
        "--path", MESSAGE_QUEUE_ROOT,
    )
    if result.returncode != 0:
        return []
    channels: list[str] = []
    for line in result.stdout.splitlines():
        kv = _parse_kv_line(line)
        name = kv.get("name", "")
        if name and name != "inbox":
            channels.append(name)
    return channels


def peek_messages(
    input_path: str,
    channel: str,
    backend_root: str | None = None,
) -> list[dict]:
    """Return all messages on a channel regardless of recipient (non-destructive).

    Each returned dict has keys from the browse output (object_id, name, etc.).
    """
    bucket_path = f"{MESSAGE_QUEUE_ROOT}/{channel}"
    result = _run_kernel(
        backend_root,
        "smartlist-browse",
        "--input", input_path,
        "--path", bucket_path,
    )
    if result.returncode != 0:
        raise RuntimeError(f"smartlist-browse {bucket_path} failed: {result.stderr}")
    messages: list[dict] = []
    for line in result.stdout.splitlines():
        kv = _parse_kv_line(line)
        if not kv:
            continue
        name = kv.get("name", "")
        if not name.startswith("msg:"):
            continue
        messages.append({"note_id": kv.get("object_id", ""), "title": name, **kv})
    return messages


def acknowledge_message(
    input_path: str,
    channel: str,
    message_id: str,
    backend_root: str | None = None,
    actor_id: str = "messaging-api",
) -> None:
    """Acknowledge (delete) a message from a channel by its note ID.

    This detaches the message from the channel bucket, marking it as consumed.
    """
    bucket_path = f"{MESSAGE_QUEUE_ROOT}/{channel}"
    result = _run_kernel(
        backend_root,
        "smartlist-detach",
        "--input", input_path,
        "--path", bucket_path,
        "--member-ref", message_id,
        "--actor-id", actor_id,
    )
    if result.returncode != 0:
        raise RuntimeError(
            f"acknowledge_message {message_id} on {channel} failed: {result.stderr}"
        )


# ---------------------------------------------------------------------------
# Inbox-per-agent routing (3d2)
# ---------------------------------------------------------------------------

INBOX_ROOT = f"{MESSAGE_QUEUE_ROOT}/inbox"


def ensure_inbox(
    input_path: str,
    agent_ref: str,
    backend_root: str | None = None,
) -> str:
    """Ensure an inbox bucket exists for the given agent. Returns bucket path."""
    inbox_path = f"{INBOX_ROOT}/{agent_ref}"
    result = _run_kernel(
        backend_root,
        "smartlist-create",
        "--input", input_path,
        "--path", inbox_path,
    )
    if result.returncode != 0:
        raise RuntimeError(f"smartlist-create {inbox_path} failed: {result.stderr}")
    return inbox_path


def send_to_inbox(
    input_path: str,
    sender: str,
    recipient: str,
    subject: str,
    body: str,
    backend_root: str | None = None,
    timestamp: str | None = None,
) -> str:
    """Send a message directly to an agent's inbox bucket.

    Also cross-posts to the channel if one is active on the sender.
    Returns the note_id.
    """
    ts = timestamp or datetime.now(timezone.utc).isoformat()
    inbox_path = ensure_inbox(input_path, recipient, backend_root=backend_root)

    payload = json.dumps({
        "sender": sender,
        "recipient": recipient,
        "subject": subject,
        "body": body,
        "timestamp": ts,
    })

    result = _run_kernel(
        backend_root,
        "smartlist-note",
        "--input", input_path,
        "--title", f"msg:{sender}->{recipient}:{subject}",
        "--text", payload,
        "--buckets", inbox_path,
    )
    if result.returncode != 0:
        raise RuntimeError(f"send_to_inbox {recipient} failed: {result.stderr}")
    data = _parse_kv(result.stdout)
    return data.get("note_id", "")


def read_inbox(
    input_path: str,
    agent_ref: str,
    backend_root: str | None = None,
) -> list[dict]:
    """Read all messages in an agent's inbox. Returns list of message dicts."""
    inbox_path = f"{INBOX_ROOT}/{agent_ref}"
    result = _run_kernel(
        backend_root,
        "smartlist-browse",
        "--input", input_path,
        "--path", inbox_path,
    )
    if result.returncode != 0:
        return []

    messages: list[dict] = []
    for line in result.stdout.strip().splitlines():
        kv = _parse_kv_line(line)
        if not kv:
            continue
        obj_id = kv.get("object_id", line.strip())
        messages.append({"note_id": obj_id, **kv})
    return messages


def broadcast(
    input_path: str,
    sender: str,
    recipients: list[str],
    subject: str,
    body: str,
    backend_root: str | None = None,
    timestamp: str | None = None,
) -> list[str]:
    """Send the same message to multiple agents' inboxes. Returns list of note_ids."""
    return [
        send_to_inbox(input_path, sender, r, subject, body,
                       backend_root=backend_root, timestamp=timestamp)
        for r in recipients
    ]


# ---------------------------------------------------------------------------
# System broadcast channel (3d3)
# ---------------------------------------------------------------------------

SYSTEM_CHANNEL = "system"


def bootstrap_system_channel(
    input_path: str,
    backend_root: str | None = None,
) -> str:
    """Create the system broadcast channel. Returns the bucket path."""
    return bootstrap_message_queue(input_path, SYSTEM_CHANNEL, backend_root=backend_root)


def system_broadcast(
    input_path: str,
    sender: str,
    subject: str,
    body: str,
    backend_root: str | None = None,
    timestamp: str | None = None,
) -> str:
    """Send a message on the system broadcast channel (recipient='*').

    Any agent can poll the system channel for announcements like
    orchestration-start, orchestration-complete, or shutdown signals.
    """
    return send_message(
        input_path,
        channel=SYSTEM_CHANNEL,
        sender=sender,
        recipient="*",
        payload=json.dumps({"subject": subject, "body": body}),
        backend_root=backend_root,
        timestamp=timestamp,
    )


def read_system_broadcasts(
    input_path: str,
    backend_root: str | None = None,
) -> list[dict]:
    """Read all messages on the system broadcast channel."""
    return peek_messages(input_path, SYSTEM_CHANNEL, backend_root=backend_root)


# ---------------------------------------------------------------------------
# Cross-SmartList triggers for inter-project dependencies (3d3)
# ---------------------------------------------------------------------------

TRIGGER_ROOT = f"{MESSAGE_QUEUE_ROOT}/triggers"


def register_trigger(
    input_path: str,
    source_project: str,
    target_project: str,
    event: str,
    action_subject: str,
    action_body: str,
    backend_root: str | None = None,
) -> str:
    """Register a cross-project trigger.

    When *source_project* emits *event*, a message with *action_subject* and
    *action_body* is delivered to the orchestrator inbox of *target_project*.

    Triggers are stored as SmartList notes under ``triggers/{source_project}``.
    Returns the note_id.
    """
    bucket_path = f"{TRIGGER_ROOT}/{source_project}"
    # Ensure the trigger bucket exists
    _run_kernel(
        backend_root,
        "smartlist-create",
        "--input", input_path,
        "--path", bucket_path,
    )

    ts = datetime.now(timezone.utc).isoformat()
    payload = json.dumps({
        "source_project": source_project,
        "target_project": target_project,
        "event": event,
        "action_subject": action_subject,
        "action_body": action_body,
        "registered_at": ts,
    })
    result = _run_kernel(
        backend_root,
        "smartlist-note",
        "--input", input_path,
        "--title", f"trigger:{source_project}->{target_project}:{event}",
        "--text", payload,
        "--buckets", bucket_path,
    )
    if result.returncode != 0:
        raise RuntimeError(f"register_trigger failed: {result.stderr}")
    return _parse_kv(result.stdout).get("note_id", "")


def fire_triggers(
    input_path: str,
    source_project: str,
    event: str,
    backend_root: str | None = None,
) -> list[str]:
    """Fire all triggers registered for *event* on *source_project*.

    For each matching trigger, sends a message to the target project's
    orchestrator inbox. Returns list of note_ids for delivered messages.
    """
    bucket_path = f"{TRIGGER_ROOT}/{source_project}"
    result = _run_kernel(
        backend_root,
        "smartlist-browse",
        "--input", input_path,
        "--path", bucket_path,
    )
    if result.returncode != 0:
        return []

    delivered: list[str] = []
    for line in result.stdout.splitlines():
        kv = _parse_kv_line(line)
        name = kv.get("name", "")
        if f":{event}" not in name:
            continue
        # Extract target_project from title: trigger:{src}->{tgt}:{event}
        try:
            after_arrow = name.split("->", 1)[1]
            target_project = after_arrow.split(":", 1)[0]
        except (IndexError, ValueError):
            continue
        # Read the trigger payload to get action details
        obj_id = kv.get("object_id", "")
        if not obj_id:
            continue
        # Deliver notification to target orchestrator inbox
        note_id = send_to_inbox(
            input_path,
            sender=f"trigger:{source_project}",
            recipient=f"orchestrator:{target_project}",
            subject=f"trigger-fired:{event}",
            body=f"Cross-project trigger from {source_project} on event '{event}'",
            backend_root=backend_root,
        )
        delivered.append(note_id)
    return delivered


def list_triggers(
    input_path: str,
    source_project: str,
    backend_root: str | None = None,
) -> list[dict]:
    """List all triggers registered for a source project."""
    bucket_path = f"{TRIGGER_ROOT}/{source_project}"
    result = _run_kernel(
        backend_root,
        "smartlist-browse",
        "--input", input_path,
        "--path", bucket_path,
    )
    if result.returncode != 0:
        return []
    triggers: list[dict] = []
    for line in result.stdout.splitlines():
        kv = _parse_kv_line(line)
        if not kv:
            continue
        name = kv.get("name", "")
        if name.startswith("trigger:"):
            triggers.append({"note_id": kv.get("object_id", ""), "title": name, **kv})
    return triggers
