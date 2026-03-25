#!/usr/bin/env python3
"""
build_roadmap.py — reads roadmap-seed.yaml and generates roadmap.memory.jsonl
in AMS SmartList object format.

Usage:
    python scripts/build_roadmap.py [--seed PATH] [--output PATH]
"""

import argparse
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path

try:
    import yaml
except ImportError:
    import sys
    sys.exit("PyYAML is required: pip install pyyaml")


NOW = datetime.now(timezone.utc).isoformat().replace("+00:00", "Z")
CREATED_BY = "build_roadmap.py"

# Ordered dependency layer definitions
LAYER_NAMES = {
    0: "substrate",
    1: "memory",
    2: "retrieval",
    3: "orchestration",
    4: "learning",
    5: "platform",
    6: "application",
}


def make_id():
    return str(uuid.uuid4()).replace("-", "")


def bucket_object(path: str, parent_path: str, title: str) -> dict:
    members_id = f"smartlist-members:{path}"
    return {
        "objectId": f"smartlist-bucket:{path}",
        "objectKind": "smartlist_bucket",
        "semanticPayload": {
            "tags": ["smartlist_bucket", "long_term"],
            "summary": title,
            "provenance": {
                "created_at": NOW,
                "created_by": CREATED_BY,
                "display_name": title,
                "durability": "long_term",
                "members_container_id": members_id,
                "parent_path": parent_path,
                "path": path,
                "retrieval_visibility": "default",
                "source": "manual",
                "title": title,
                "updated_at": NOW,
            },
        },
        "createdAt": NOW,
        "updatedAt": NOW,
    }


def container_object(path: str) -> dict:
    container_id = f"smartlist-members:{path}"
    return {
        "objectId": container_id,
        "objectKind": "container",
        "createdAt": NOW,
        "updatedAt": NOW,
    }


def container_dict(path: str, head_ln=None, tail_ln=None) -> dict:
    d = {
        "containerId": f"smartlist-members:{path}",
        "containerKind": "smartlist_members",
        "expectationMetadata": {"interpretation": "ordered_frame"},
        "policies": {
            "uniqueMembers": True,
            "orderedByRecency": False,
            "allowMultiParent": False,
        },
        "absoluteSemantics": {"absoluteKind": "other"},
        "hypothesisState": {},
    }
    if head_ln:
        d["headLinknodeId"] = head_ln
    if tail_ln:
        d["tailLinknodeId"] = tail_ln
    return d


def concept_object(concept: dict) -> dict:
    slug = concept["slug"]
    return {
        "objectId": f"roadmap-concept:{slug}",
        "objectKind": "roadmap_concept",
        "semanticPayload": {
            "intent": concept.get("intent", ""),
            "summary": concept.get("summary", ""),
            "status": concept.get("status", "planned"),
            "tdd_layer": concept.get("tdd_layer", ""),
            "layer": concept.get("layer", concept.get("tdd_layer", "")),
            "doc_refs": concept.get("doc_refs", []),
            "name": concept.get("name", slug),
            "slug": slug,
            "depends_on": concept.get("depends_on", []),
            "enables": concept.get("enables", []),
            "themes": concept.get("themes", []),
        },
        "createdAt": NOW,
        "updatedAt": NOW,
    }


def edge_object(from_slug: str, to_slug: str, kind: str) -> dict:
    """A directed dependency edge between two concepts."""
    edge_id = f"roadmap-edge:{kind}:{from_slug}:{to_slug}"
    return {
        "objectId": edge_id,
        "objectKind": "roadmap_edge",
        "semanticPayload": {
            "from_concept": from_slug,
            "to_concept": to_slug,
            "kind": kind,  # "depends_on" or "enables"
        },
        "createdAt": NOW,
        "updatedAt": NOW,
    }


def sprint_object(sprint: dict) -> dict:
    ref = sprint["swarm_plan_ref"]
    return {
        "objectId": f"roadmap-sprint:{ref}",
        "objectKind": "roadmap_sprint",
        "semanticPayload": {
            "title": sprint.get("title", ref),
            "status": sprint.get("status", "done"),
            "swarm_plan_ref": ref,
            "concept_ref": sprint.get("concept_ref", "ams"),
            "summary": sprint.get("summary", ""),
        },
        "createdAt": NOW,
        "updatedAt": NOW,
    }


def note_slug(note: dict, idx: int) -> str:
    title = note.get("title", f"note-{idx}")
    slug = title.lower().replace(" ", "-").replace("/", "-")
    slug = "".join(c for c in slug if c.isalnum() or c == "-")[:50]
    return slug


def note_object(note: dict, idx: int) -> dict:
    concept_ref = note.get("concept_ref", "ams")
    slug = note_slug(note, idx)
    return {
        "objectId": f"roadmap-note:{concept_ref}:{slug}",
        "objectKind": "roadmap_note",
        "semanticPayload": {
            "title": note.get("title", ""),
            "text": note.get("text", ""),
            "kind": note.get("kind", "design-note"),
            "concept_ref": concept_ref,
            "is_relevant": note.get("is_relevant", False),
        },
        "createdAt": NOW,
        "updatedAt": NOW,
    }


def build_store(seed_path: Path, output_path: Path):
    with open(seed_path) as f:
        seed = yaml.safe_load(f)

    concepts = seed.get("concepts", [])
    sprints = seed.get("sprints", [])
    notes = seed.get("notes", [])

    objects = []
    containers_list = []
    link_nodes = []

    ln_counter = [0]

    def new_ln_id():
        ln_counter[0] += 1
        return f"ln_{ln_counter[0]:03d}"

    # Track per-container link node lists for building head/tail
    # { container_path: [ln_id, ...] }
    container_members: dict[str, list[tuple[str, str]]] = {}  # path -> [(ln_id, object_id)]

    def add_member(container_path: str, object_id: str):
        ln_id = new_ln_id()
        container_members.setdefault(container_path, []).append((ln_id, object_id))

    # ── Root bucket ──────────────────────────────────────────────────────────
    root_path = "smartlist/roadmap"
    objects.append(bucket_object(root_path, "smartlist", "roadmap"))
    objects.append(container_object(root_path))

    # ── Status buckets ───────────────────────────────────────────────────────
    for status in ("planned", "active", "done", "parked", "canceled"):
        path = f"smartlist/roadmap/status/{status}"
        objects.append(bucket_object(path, "smartlist/roadmap/status", status))
        objects.append(container_object(path))

    # ── Collect enrichment dimensions ─────────────────────────────────────────
    concept_slugs = {c["slug"] for c in concepts}

    # Layer slugs: map numeric layer → "N-name", fallback to raw normalisation
    def _layer_slug(raw) -> str:
        if raw is None or raw == "":
            return ""
        try:
            n = int(raw)
            name = LAYER_NAMES.get(n)
            if name:
                return f"{n}-{name}"
        except (ValueError, TypeError):
            pass
        return str(raw).lower().replace(" ", "-")

    all_layers: set[str] = set()
    all_themes: set[str] = set()
    for concept in concepts:
        raw_layer = concept.get("layer", concept.get("tdd_layer", ""))
        if raw_layer is not None and raw_layer != "":
            slug = _layer_slug(raw_layer)
            if slug:
                all_layers.add(slug)
        for theme in concept.get("themes", []):
            all_themes.add(theme)

    # ── Layer buckets ─────────────────────────────────────────────────────────
    for layer_slug in sorted(all_layers):
        path = f"smartlist/roadmap/layer/{layer_slug}"
        objects.append(bucket_object(path, "smartlist/roadmap/layer", layer_slug))
        objects.append(container_object(path))

    # ── Theme buckets ─────────────────────────────────────────────────────────
    for theme in sorted(all_themes):
        path = f"smartlist/roadmap/theme/{theme}"
        objects.append(bucket_object(path, "smartlist/roadmap/theme", theme))
        objects.append(container_object(path))

    # ── Concept buckets + per-concept sub-buckets ─────────────────────────────
    for concept in concepts:
        slug = concept["slug"]
        concept_path = f"smartlist/roadmap/concept/{slug}"
        sub_paths = [
            concept_path,
            f"{concept_path}/sprints",
            f"{concept_path}/notes",
            f"{concept_path}/relevance",
            f"{concept_path}/depends-on",
            f"{concept_path}/enables",
        ]
        for sub in sub_paths:
            parent = "/".join(sub.split("/")[:-1]) if "/" in sub else "smartlist/roadmap"
            title = sub.split("/")[-1]
            objects.append(bucket_object(sub, parent, title))
            objects.append(container_object(sub))

    # ── Concept objects ───────────────────────────────────────────────────────
    edges = []
    for concept in concepts:
        slug = concept["slug"]
        obj = concept_object(concept)
        objects.append(obj)
        # Attach to root + concept path
        add_member(root_path, obj["objectId"])
        add_member(f"smartlist/roadmap/concept/{slug}", obj["objectId"])
        # Attach to layer bucket
        raw_layer = concept.get("layer", concept.get("tdd_layer", ""))
        if raw_layer is not None and raw_layer != "":
            layer_s = _layer_slug(raw_layer)
            if layer_s:
                add_member(f"smartlist/roadmap/layer/{layer_s}", obj["objectId"])
        # Attach to theme buckets
        for theme in concept.get("themes", []):
            add_member(f"smartlist/roadmap/theme/{theme}", obj["objectId"])

    # ── Edge objects (depends_on / enables) ────────────────────────────────────
    for concept in concepts:
        slug = concept["slug"]
        for dep_slug in concept.get("depends_on", []):
            if dep_slug in concept_slugs:
                edge = edge_object(slug, dep_slug, "depends_on")
                edges.append(edge)
                objects.append(edge)
                # Attach target concept object (not edge) to the depends-on SmartList
                add_member(f"smartlist/roadmap/concept/{slug}/depends-on", f"roadmap-concept:{dep_slug}")
        for en_slug in concept.get("enables", []):
            if en_slug in concept_slugs:
                edge = edge_object(slug, en_slug, "enables")
                edges.append(edge)
                objects.append(edge)
                # Attach target concept object (not edge) to the enables SmartList
                add_member(f"smartlist/roadmap/concept/{slug}/enables", f"roadmap-concept:{en_slug}")

    # ── Sprint objects ────────────────────────────────────────────────────────
    for sprint in sprints:
        obj = sprint_object(sprint)
        objects.append(obj)
        concept_ref = sprint.get("concept_ref", "ams")
        status = sprint.get("status", "done")
        # Attach to concept/sprints
        if concept_ref in concept_slugs:
            add_member(f"smartlist/roadmap/concept/{concept_ref}/sprints", obj["objectId"])
        # Attach to status list
        add_member(f"smartlist/roadmap/status/{status}", obj["objectId"])

    # ── Note objects ──────────────────────────────────────────────────────────
    for idx, note in enumerate(notes):
        obj = note_object(note, idx)
        objects.append(obj)
        concept_ref = note.get("concept_ref", "ams")
        # Attach to concept/notes
        if concept_ref in concept_slugs:
            add_member(f"smartlist/roadmap/concept/{concept_ref}/notes", obj["objectId"])
            if note.get("is_relevant"):
                add_member(f"smartlist/roadmap/concept/{concept_ref}/relevance", obj["objectId"])

    # ── Atlas SmartLists ──────────────────────────────────────────────────────
    # Concepts sorted by layer asc, then slug alpha
    def _concept_layer_key(c):
        raw = c.get("layer", c.get("tdd_layer", ""))
        try:
            return (int(raw), c["slug"])
        except (ValueError, TypeError):
            return (999, c["slug"])

    sorted_concepts = sorted(concepts, key=_concept_layer_key)

    # Atlas root bucket
    atlas_root = "smartlist/roadmap/atlas"
    objects.append(bucket_object(atlas_root, "smartlist/roadmap", "atlas"))
    objects.append(container_object(atlas_root))

    # overview: all concept objects, layer-ordered
    overview_path = "smartlist/roadmap/atlas/overview"
    objects.append(bucket_object(overview_path, atlas_root, "overview"))
    objects.append(container_object(overview_path))
    for c in sorted_concepts:
        add_member(overview_path, f"roadmap-concept:{c['slug']}")

    # mid: same concept ordering + active/planned sprints
    mid_path = "smartlist/roadmap/atlas/mid"
    objects.append(bucket_object(mid_path, atlas_root, "mid"))
    objects.append(container_object(mid_path))
    for c in sorted_concepts:
        add_member(mid_path, f"roadmap-concept:{c['slug']}")
    for sprint in sprints:
        if sprint.get("status") in ("active", "planned"):
            add_member(mid_path, f"roadmap-sprint:{sprint['swarm_plan_ref']}")

    # detail/<slug>: concept + all its sprints + all its notes
    # Build lookup maps for sprints and notes by concept_ref
    sprints_by_concept: dict[str, list] = {}
    for sprint in sprints:
        ref = sprint.get("concept_ref", "ams")
        sprints_by_concept.setdefault(ref, []).append(sprint)

    notes_by_concept: dict[str, list] = {}
    for idx, note in enumerate(notes):
        ref = note.get("concept_ref", "ams")
        notes_by_concept.setdefault(ref, []).append((idx, note))

    for concept in concepts:
        slug = concept["slug"]
        detail_path = f"smartlist/roadmap/atlas/detail/{slug}"
        objects.append(bucket_object(detail_path, atlas_root, f"detail/{slug}"))
        objects.append(container_object(detail_path))
        add_member(detail_path, f"roadmap-concept:{slug}")
        for sprint in sprints_by_concept.get(slug, []):
            add_member(detail_path, f"roadmap-sprint:{sprint['swarm_plan_ref']}")
        for idx, note in notes_by_concept.get(slug, []):
            add_member(detail_path, note_object(note, idx)["objectId"])

    # ── Build link nodes and containers ──────────────────────────────────────
    # Collect all known container paths
    all_container_paths = set()
    for obj in objects:
        if obj["objectKind"] == "container":
            path = obj["objectId"].replace("smartlist-members:", "")
            all_container_paths.add(path)

    for path in all_container_paths:
        members = container_members.get(path, [])
        if not members:
            containers_list.append(container_dict(path))
            continue

        # Build doubly-linked list
        lns = []
        for i, (ln_id, obj_id) in enumerate(members):
            prev_ln = members[i - 1][0] if i > 0 else None
            next_ln = members[i + 1][0] if i < len(members) - 1 else None
            lns.append({
                "linkNodeId": ln_id,
                "containerId": f"smartlist-members:{path}",
                "objectId": obj_id,
                "prevLinknodeId": prev_ln,
                "nextLinknodeId": next_ln,
            })
            link_nodes.append(lns[-1])

        head = members[0][0]
        tail = members[-1][0]
        containers_list.append(container_dict(path, head_ln=head, tail_ln=tail))

    store = {
        "objects": objects,
        "containers": containers_list,
        "linkNodes": link_nodes,
    }

    output_path.parent.mkdir(parents=True, exist_ok=True)
    with open(output_path, "w", encoding="utf-8") as f:
        json.dump(store, f, indent=2, ensure_ascii=False)

    n_concepts = len(concepts)
    n_sprints = len(sprints)
    n_notes = len(notes)
    n_edges = len(edges)
    n_layers = len(all_layers)
    n_themes = len(all_themes)
    print(f"roadmap.memory.jsonl built: {n_concepts} concepts, {n_sprints} sprints, {n_notes} notes, {n_edges} edges")
    print(f"  layers={n_layers}, themes={n_themes}, atlas=overview+mid+{n_concepts} detail views")
    print(f"  objects={len(objects)}, containers={len(containers_list)}, linkNodes={len(link_nodes)}")
    print(f"  output: {output_path}")


def main():
    repo_root = Path(__file__).parent.parent
    default_seed = repo_root / "shared-memory/system-memory/roadmap-seed.yaml"
    default_output = repo_root / "shared-memory/system-memory/roadmap.memory.jsonl"

    parser = argparse.ArgumentParser(description="Build roadmap.memory.jsonl from roadmap-seed.yaml")
    parser.add_argument("--seed", type=Path, default=default_seed, help="Path to roadmap-seed.yaml")
    parser.add_argument("--output", type=Path, default=default_output, help="Path to output .jsonl file")
    args = parser.parse_args()

    if not args.seed.exists():
        print(f"ERROR: seed file not found: {args.seed}")
        raise SystemExit(1)

    build_store(args.seed, args.output)


if __name__ == "__main__":
    main()
