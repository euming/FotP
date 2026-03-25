"""Populate FotP-Implementation children from YAML."""
import yaml, subprocess, sys

PROJECT = "FotP-Implementation"
BASE = "smartlist/execution-plan/fotp-implementation/10-children"

def slugify(title):
    return title.lower().replace(" ", "-").replace("(", "").replace(")", "").replace(",", "").replace("'", "").replace("/", "-").replace(".", "").replace("+", "").replace("*", "").replace(":", "").replace("[", "").replace("]", "")

def insert(name, parent_slug, desc):
    cmd = [
        "scripts\\ams.bat", "swarm-plan", "--project", PROJECT,
        "insert", name, "--parent", f"{BASE}/{parent_slug}",
        "--actor-id", "claude-opus",
    ]
    if desc:
        cmd.extend(["--description", desc.strip()])
    r = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    tag = "OK" if r.returncode == 0 else "ERR"
    print(f"  {tag}: {name}")
    if r.returncode != 0:
        print(f"       {r.stderr.strip()[:120]}", file=sys.stderr)

data = yaml.safe_load(open("docs/fotp-implementation-plan.yaml"))
for node in data["nodes"]:
    children = node.get("children", [])
    if not children:
        continue
    parent_slug = slugify(node["title"])
    print(f"\n=== {node['title']} ({len(children)} children) ===")
    for child in children:
        insert(child["title"], parent_slug, child.get("description", ""))

print("\nDone.")
