use std::fs;
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};

use crate::persistence::deserialize_snapshot;
use crate::store::AmsStore;

pub fn derive_snapshot_input_path(input: &Path) -> Result<PathBuf> {
    if input.is_file() && input.extension().is_some_and(|ext| ext == "json") {
        return Ok(input.to_path_buf());
    }

    let file_name = input
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("input path '{}' is missing a file name", input.display()))?;

    if !file_name.ends_with(".memory.jsonl") {
        return Err(anyhow!(
            "unsupported input '{}'; expected a '.ams.json' snapshot or '.memory.jsonl' store path",
            input.display()
        ));
    }

    let stem = input
        .file_stem()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("failed to derive snapshot path from '{}'", input.display()))?;

    Ok(input.with_file_name(format!("{stem}.ams.json")))
}

pub fn resolve_snapshot_input_path(input: &Path) -> Result<PathBuf> {
    let resolved = derive_snapshot_input_path(input)?;
    if !resolved.is_file() {
        return Err(anyhow!(
            "snapshot '{}' was not found next to '{}'",
            resolved.display(),
            input.display()
        ));
    }

    Ok(resolved)
}

pub fn import_snapshot_file(input: &Path) -> Result<(AmsStore, PathBuf)> {
    let resolved = resolve_snapshot_input_path(input)?;
    let json = fs::read_to_string(&resolved)
        .with_context(|| format!("failed to read snapshot '{}'", resolved.display()))?;
    let store = deserialize_snapshot(&json)
        .with_context(|| format!("failed to import snapshot '{}'", resolved.display()))?;
    Ok((store, resolved))
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    const CSHARP_SNAPSHOT_FIXTURE: &str = r#"{
  "objects": [
    {
      "objectId": "ctr:ordered",
      "objectKind": "container",
      "createdAt": "2026-03-13T00:00:00+00:00",
      "updatedAt": "2026-03-13T00:00:00+00:00"
    },
    {
      "objectId": "obj:a",
      "objectKind": "thing",
      "inSituRef": "fixture://a",
      "semanticPayload": {
        "tags": [
          "fixture",
          "alpha"
        ],
        "summary": "Fixture object A",
        "provenance": {
          "source": "csharp-fixture"
        }
      },
      "createdAt": "2026-03-13T00:00:00+00:00",
      "updatedAt": "2026-03-13T00:00:00+00:00"
    },
    {
      "objectId": "obj:b",
      "objectKind": "thing",
      "createdAt": "2026-03-13T00:00:00+00:00",
      "updatedAt": "2026-03-13T00:00:00+00:00"
    }
  ],
  "containers": [
    {
      "containerId": "ctr:ordered",
      "containerKind": "smartlist",
      "expectationMetadata": {
        "interpretation": "ordered_frame"
      },
      "policies": {
        "uniqueMembers": true
      },
      "anchors": [
        "fixture-root"
      ],
      "absoluteSemantics": {
        "absoluteKind": "other"
      },
      "headLinknodeId": "ln-b",
      "tailLinknodeId": "ln-a",
      "metadata": {
        "owner": "fixture"
      },
      "hypothesisState": {
        "lane": {
          "key": "lane",
          "value": "parity",
          "updatedAt": "2026-03-13T00:00:00+00:00"
        }
      }
    }
  ],
  "linkNodes": [
    {
      "linkNodeId": "ln-a",
      "containerId": "ctr:ordered",
      "objectId": "obj:b",
      "prevLinknodeId": "ln-b",
      "relDelta": 2,
      "metadata": {
        "position": "tail"
      }
    },
    {
      "linkNodeId": "ln-b",
      "containerId": "ctr:ordered",
      "objectId": "obj:a",
      "nextLinknodeId": "ln-a",
      "relDelta": 1
    }
  ]
}"#;

    #[test]
    fn imports_csharp_snapshot_from_direct_ams_json_path() {
        let dir = tempdir().unwrap();
        let snapshot_path = dir.path().join("fixture.ams.json");
        fs::write(&snapshot_path, CSHARP_SNAPSHOT_FIXTURE).unwrap();

        let (store, resolved) = import_snapshot_file(&snapshot_path).unwrap();
        assert_eq!(resolved, snapshot_path);
        assert_eq!(store.objects().len(), 3);
        assert_eq!(store.containers().len(), 1);
        assert_eq!(store.link_nodes().len(), 2);

        let order = store
            .iterate_forward("ctr:ordered")
            .iter()
            .map(|node| node.object_id.as_str())
            .collect::<Vec<_>>();
        assert_eq!(order, vec!["obj:a", "obj:b"]);

        let container = store.containers().get("ctr:ordered").unwrap();
        assert!(container.policies.unique_members);
        assert_eq!(container.head_linknode_id.as_deref(), Some("ln-b"));
        assert_eq!(container.tail_linknode_id.as_deref(), Some("ln-a"));
    }

    #[test]
    fn resolves_csharp_snapshot_from_memory_jsonl_path() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("fixture.memory.jsonl");
        let snapshot_path = dir.path().join("fixture.memory.ams.json");
        fs::write(&db_path, "").unwrap();
        fs::write(&snapshot_path, CSHARP_SNAPSHOT_FIXTURE).unwrap();

        let resolved = resolve_snapshot_input_path(&db_path).unwrap();
        assert_eq!(resolved, snapshot_path);

        let (store, imported_path) = import_snapshot_file(&db_path).unwrap();
        assert_eq!(imported_path, snapshot_path);
        assert_eq!(store.link_nodes().len(), 2);
    }

    #[test]
    fn derives_snapshot_path_without_requiring_existing_file() {
        let db_path = PathBuf::from(r"C:\fixture\all.memory.jsonl");
        let derived = derive_snapshot_input_path(&db_path).unwrap();
        assert_eq!(derived, PathBuf::from(r"C:\fixture\all.memory.ams.json"));
    }
}
