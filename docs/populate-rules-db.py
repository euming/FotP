"""
Populate the FotP-Rules-v20 swarm-plan with all nested children from the YAML.
Reads fotp-rules-v20-plan.yaml and inserts every child node under its parent.
"""
import yaml
import subprocess
import sys
import time

YAML_PATH = "docs/fotp-rules-v20-plan.yaml"
PROJECT = "FotP-Rules-v20"
BASE_PATH = "smartlist/execution-plan/fotp-rules-v20/10-children"

def run_insert(name, parent_path, description, actor="claude-opus"):
    """Insert a child node under a parent in the swarm-plan."""
    cmd = [
        "scripts\\ams.bat", "swarm-plan",
        "--project", PROJECT,
        "insert",
        name,
        "--parent", parent_path,
        "--actor-id", actor,
    ]
    if description:
        cmd.extend(["--description", description.strip()])

    result = subprocess.run(cmd, capture_output=True, text=True, timeout=60)
    if result.returncode != 0:
        print(f"  ERROR inserting '{name}': {result.stderr.strip()}", file=sys.stderr)
        return False
    else:
        stdout = result.stdout.strip()
        if stdout:
            print(f"  OK: {name} -> {stdout[:120]}")
        else:
            print(f"  OK: {name}")
        return True


def slugify(title):
    """Convert a title to a slug matching what AMS generates."""
    return title.lower().replace(" ", "-").replace("(", "").replace(")", "").replace(",", "").replace("'", "").replace("/", "-").replace(".", "").replace("+", "").replace("*", "").replace(":", "")


def main():
    with open(YAML_PATH) as f:
        data = yaml.safe_load(f)

    total = 0
    errors = 0

    for node in data["nodes"]:
        children = node.get("children", [])
        if not children:
            continue

        # The parent path in the DB for this top-level node
        node_slug = slugify(node["title"])
        parent_path = f"{BASE_PATH}/{node_slug}"

        print(f"\n=== {node['title']} ({len(children)} children) ===")
        print(f"    parent: {parent_path}")

        for child in children:
            title = child["title"]
            desc = child.get("description", "")

            ok = run_insert(title, parent_path, desc)
            total += 1
            if not ok:
                errors += 1

    print(f"\n--- Done: {total} nodes inserted, {errors} errors ---")


if __name__ == "__main__":
    main()
