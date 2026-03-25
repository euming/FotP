#!/usr/bin/env python3
"""
Convert a Codex session JSONL log to chat_event rawUser format
for ingestion via memoryctl ingest-chatlog.

Usage: python convert-codex.py <input.jsonl> <output.raw.jsonl>

Extracts:
  - event_msg / user_message  -> direction=in  (User)
  - event_msg / agent_message -> direction=out (Codex)

The IDE context header ("## My request for Codex:") is stripped so only
the user's actual question is stored.
"""
import json
import re
import sys


def extract_user_request(msg: str) -> str:
    """Return just the user's actual request, stripping the IDE context block."""
    m = re.search(r"## My request for Codex:\n(.*?)(?:\Z)", msg, re.DOTALL)
    return m.group(1).strip() if m else msg.strip()


def convert(in_path: str, out_path: str) -> None:
    session_id = None
    events: list[dict] = []

    with open(in_path, encoding="utf-8") as f:
        for line in f:
            line = line.strip()
            if not line:
                continue
            try:
                obj = json.loads(line)
            except json.JSONDecodeError:
                continue

            ts = obj.get("timestamp")
            t  = obj.get("type", "")
            p  = obj.get("payload", {})

            if t == "session_meta":
                session_id = p.get("id", "codex-session")

            if t == "event_msg":
                pt = p.get("type", "")
                if pt == "user_message":
                    text = extract_user_request(p.get("message", ""))
                    if text:
                        events.append({"ts": ts, "direction": "in", "author": "User", "text": text})

                elif pt == "agent_message":
                    text = p.get("message", "").strip()
                    if text:
                        events.append({"ts": ts, "direction": "out", "author": "Codex", "text": text})

    channel = "codex"
    chat_id = session_id or "codex-session"

    with open(out_path, "w", encoding="utf-8") as out:
        for i, e in enumerate(events):
            rec = {
                "type":       "chat_event",
                "channel":    channel,
                "chat_id":    chat_id,
                "message_id": f"msg-{i:04d}",
                "ts":         e["ts"],
                "author":     e["author"],
                "direction":  e["direction"],
                "text":       e["text"],
            }
            out.write(json.dumps(rec, ensure_ascii=False) + "\n")

    user_c = sum(1 for e in events if e["direction"] == "in")
    asst_c = sum(1 for e in events if e["direction"] == "out")
    print(f"Session : {chat_id}")
    print(f"Converted: {len(events)} events  (User: {user_c}, Codex: {asst_c})")
    print(f"Output  : {out_path}")


if __name__ == "__main__":
    if len(sys.argv) < 3:
        print(f"Usage: {sys.argv[0]} <input.jsonl> <output.raw.jsonl>", file=sys.stderr)
        sys.exit(1)
    convert(sys.argv[1], sys.argv[2])
