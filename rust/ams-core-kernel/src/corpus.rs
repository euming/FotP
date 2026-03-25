use std::collections::BTreeMap;
use std::fs::File;
use std::io::{BufRead, BufReader};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context, Result};
use chrono::{DateTime, FixedOffset};
use serde_json::Value;
use uuid::Uuid;

use crate::importer::{import_snapshot_file, resolve_snapshot_input_path};
use crate::store::AmsStore;
use crate::write_service::resolve_authoritative_snapshot_input;

#[derive(Clone, Copy, Debug, Eq, PartialEq, Ord, PartialOrd)]
pub enum CardState {
    Active,
    Tombstoned,
    Retracted,
}

impl CardState {
    pub fn parse(value: &str) -> Result<Self> {
        match value.to_ascii_lowercase().as_str() {
            "active" => Ok(Self::Active),
            "tombstoned" => Ok(Self::Tombstoned),
            "retracted" => Ok(Self::Retracted),
            other => Err(anyhow!("unsupported card state '{other}'")),
        }
    }

    pub fn as_str(self) -> &'static str {
        match self {
            Self::Active => "Active",
            Self::Tombstoned => "Tombstoned",
            Self::Retracted => "Retracted",
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct CardRecord {
    pub card_id: Uuid,
    pub state: CardState,
    pub state_reason: Option<String>,
}

#[derive(Clone, Debug, PartialEq)]
pub struct BinderRecord {
    pub binder_id: Uuid,
    pub name: String,
}

#[derive(Clone, Debug, PartialEq)]
pub struct TagLinkMeta {
    pub relevance: f32,
    pub reason: Option<String>,
    pub added_by: Option<String>,
    pub created_at: Option<DateTime<FixedOffset>>,
}

impl Default for TagLinkMeta {
    fn default() -> Self {
        Self {
            relevance: 0.5,
            reason: None,
            added_by: None,
            created_at: None,
        }
    }
}

#[derive(Clone, Debug, PartialEq)]
pub struct TagLinkRecord {
    pub card_id: Uuid,
    pub binder_id: Uuid,
    pub meta: TagLinkMeta,
}

#[derive(Clone, Debug, PartialEq)]
pub struct CardPayloadRecord {
    pub card_id: Uuid,
    pub title: Option<String>,
    pub text: Option<String>,
    pub source: Option<String>,
    pub updated_at: Option<DateTime<FixedOffset>>,
}

#[derive(Clone, Debug)]
pub struct MaterializedCorpus {
    pub db_path: PathBuf,
    pub snapshot_path: Option<PathBuf>,
    pub snapshot: Option<AmsStore>,
    pub cards: BTreeMap<Uuid, CardRecord>,
    pub binders: BTreeMap<Uuid, BinderRecord>,
    pub tag_links: BTreeMap<(Uuid, Uuid), TagLinkRecord>,
    pub payloads: BTreeMap<Uuid, CardPayloadRecord>,
    pub unknown_record_types: BTreeMap<String, usize>,
}

impl MaterializedCorpus {
    pub fn binders_for_card(&self, card_id: Uuid) -> Vec<(&BinderRecord, &TagLinkRecord)> {
        let mut binders = self
            .tag_links
            .values()
            .filter(|link| link.card_id == card_id)
            .filter_map(|link| self.binders.get(&link.binder_id).map(|binder| (binder, link)))
            .collect::<Vec<_>>();
        binders.sort_by(|left, right| {
            left.0
                .name
                .cmp(&right.0.name)
                .then_with(|| left.0.binder_id.cmp(&right.0.binder_id))
        });
        binders
    }

    pub fn cards_in_binder(&self, binder_id: Uuid) -> Vec<(&CardRecord, Option<&CardPayloadRecord>, &TagLinkRecord)> {
        let mut cards = self
            .tag_links
            .values()
            .filter(|link| link.binder_id == binder_id)
            .filter_map(|link| {
                self.cards
                    .get(&link.card_id)
                    .map(|card| (card, self.payloads.get(&link.card_id), link))
            })
            .collect::<Vec<_>>();
        cards.sort_by(|left, right| {
            let left_title = left
                .1
                .and_then(|payload| payload.title.as_deref())
                .unwrap_or_default();
            let right_title = right
                .1
                .and_then(|payload| payload.title.as_deref())
                .unwrap_or_default();
            left_title
                .cmp(right_title)
                .then_with(|| left.0.card_id.cmp(&right.0.card_id))
        });
        cards
    }

    pub fn snapshot_container_id_for_card(&self, card_id: Uuid) -> String {
        format!("chat-session:{card_id}")
    }

    pub fn snapshot_contains_card_container(&self, card_id: Uuid) -> bool {
        self.snapshot
            .as_ref()
            .map(|store| store.containers().contains_key(&self.snapshot_container_id_for_card(card_id)))
            .unwrap_or(false)
    }
}

pub fn import_materialized_corpus(input: &Path) -> Result<MaterializedCorpus> {
    let file_name = input
        .file_name()
        .and_then(|value| value.to_str())
        .ok_or_else(|| anyhow!("input path '{}' is missing a file name", input.display()))?;
    if !file_name.ends_with(".memory.jsonl") {
        return Err(anyhow!(
            "unsupported corpus input '{}'; expected a '.memory.jsonl' store path",
            input.display()
        ));
    }

    let snapshot_path = {
        let candidate = resolve_authoritative_snapshot_input(input);
        if candidate.is_file() {
            Some(candidate)
        } else {
            resolve_snapshot_input_path(input).ok()
        }
    };
    let snapshot = snapshot_path
        .as_ref()
        .map(|path| import_snapshot_file(path).map(|(store, _)| store))
        .transpose()?;

    let file = File::open(input)
        .with_context(|| format!("failed to open corpus store '{}'", input.display()))?;
    let reader = BufReader::new(file);

    let mut cards = BTreeMap::new();
    let mut binders = BTreeMap::new();
    let mut tag_links = BTreeMap::new();
    let mut payloads = BTreeMap::new();
    let mut unknown_record_types = BTreeMap::new();
    let mut saw_header = false;

    for (line_no, line) in reader.lines().enumerate() {
        let line = line.with_context(|| format!("failed to read line {} from '{}'", line_no + 1, input.display()))?;
        if line.trim().is_empty() {
            continue;
        }

        let root: Value = serde_json::from_str(&line).with_context(|| {
            format!(
                "failed to parse JSONL record on line {} in '{}'",
                line_no + 1,
                input.display()
            )
        })?;
        let record_type = root
            .get("type")
            .and_then(Value::as_str)
            .ok_or_else(|| anyhow!("missing 'type' field in JSONL record on line {}", line_no + 1))?;

        match record_type {
            "format" => {
                let name = root
                    .get("name")
                    .and_then(Value::as_str)
                    .ok_or_else(|| anyhow!("format.name is required on line {}", line_no + 1))?;
                let version = root
                    .get("version")
                    .and_then(Value::as_i64)
                    .ok_or_else(|| anyhow!("format.version is required on line {}", line_no + 1))?;
                if name != "card-binder" {
                    return Err(anyhow!("unexpected format name '{name}' on line {}", line_no + 1));
                }
                if version != 1 {
                    return Err(anyhow!("unsupported format version '{version}' on line {}", line_no + 1));
                }
                saw_header = true;
            }
            "card" => {
                ensure_header(saw_header, line_no + 1)?;
                let card_id = parse_uuid(&root, "id", line_no + 1)?;
                let state = CardState::parse(
                    root.get("state")
                        .and_then(Value::as_str)
                        .ok_or_else(|| anyhow!("card.state is required on line {}", line_no + 1))?,
                )?;
                let state_reason = get_optional_string(&root, "state_reason");
                cards.insert(
                    card_id,
                    CardRecord {
                        card_id,
                        state,
                        state_reason,
                    },
                );
            }
            "binder" | "memAnchor" => {
                ensure_header(saw_header, line_no + 1)?;
                let binder_id = parse_uuid(&root, "id", line_no + 1)?;
                let name = get_optional_string(&root, "name").unwrap_or_else(|| "Binder".to_string());
                binders.insert(binder_id, BinderRecord { binder_id, name });
            }
            "taglink" => {
                ensure_header(saw_header, line_no + 1)?;
                let card_id = parse_uuid(&root, "card_id", line_no + 1)?;
                let binder_id = parse_uuid_alias(&root, &["binder_id", "memAnchor_id"], line_no + 1)?;
                let meta = parse_tag_link_meta(&root);

                cards.entry(card_id).or_insert(CardRecord {
                    card_id,
                    state: CardState::Active,
                    state_reason: None,
                });
                binders.entry(binder_id).or_insert(BinderRecord {
                    binder_id,
                    name: "Imported".to_string(),
                });

                tag_links.insert(
                    (card_id, binder_id),
                    TagLinkRecord {
                        card_id,
                        binder_id,
                        meta,
                    },
                );
            }
            "card_payload" => {
                ensure_header(saw_header, line_no + 1)?;
                let card_id = parse_uuid(&root, "card_id", line_no + 1)?;
                cards.entry(card_id).or_insert(CardRecord {
                    card_id,
                    state: CardState::Active,
                    state_reason: None,
                });
                payloads.insert(
                    card_id,
                    CardPayloadRecord {
                        card_id,
                        title: get_optional_string(&root, "title"),
                        text: get_optional_string(&root, "text"),
                        source: get_optional_string(&root, "source"),
                        updated_at: get_optional_datetime(&root, "updated_at"),
                    },
                );
            }
            other => {
                ensure_header(saw_header, line_no + 1)?;
                *unknown_record_types.entry(other.to_string()).or_insert(0) += 1;
            }
        }
    }

    if !saw_header {
        return Err(anyhow!("missing format header record"));
    }

    Ok(MaterializedCorpus {
        db_path: input.to_path_buf(),
        snapshot_path,
        snapshot,
        cards,
        binders,
        tag_links,
        payloads,
        unknown_record_types,
    })
}

fn ensure_header(saw_header: bool, line_no: usize) -> Result<()> {
    if saw_header {
        return Ok(());
    }

    Err(anyhow!(
        "format header must be the first record (line {})",
        line_no
    ))
}

fn parse_uuid(root: &Value, field: &str, line_no: usize) -> Result<Uuid> {
    let value = root
        .get(field)
        .and_then(Value::as_str)
        .ok_or_else(|| anyhow!("{field} is required on line {}", line_no))?;
    Uuid::parse_str(value).with_context(|| format!("invalid uuid in '{field}' on line {}", line_no))
}

fn parse_uuid_alias(root: &Value, fields: &[&str], line_no: usize) -> Result<Uuid> {
    for field in fields {
        if let Some(value) = root.get(*field).and_then(Value::as_str) {
            return Uuid::parse_str(value)
                .with_context(|| format!("invalid uuid in '{}' on line {}", field, line_no));
        }
    }

    Err(anyhow!(
        "{} is required on line {}",
        fields.join(" or "),
        line_no
    ))
}

fn get_optional_string(root: &Value, field: &str) -> Option<String> {
    root.get(field).and_then(Value::as_str).map(ToString::to_string)
}

fn get_optional_datetime(root: &Value, field: &str) -> Option<DateTime<FixedOffset>> {
    root.get(field)
        .and_then(Value::as_str)
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok())
}

fn parse_tag_link_meta(root: &Value) -> TagLinkMeta {
    let Some(meta) = root.get("meta") else {
        return TagLinkMeta::default();
    };

    TagLinkMeta {
        relevance: meta
            .get("Relevance")
            .and_then(Value::as_f64)
            .map(|value| value as f32)
            .unwrap_or(0.5),
        reason: meta.get("Reason").and_then(Value::as_str).map(ToString::to_string),
        added_by: meta
            .get("AddedBy")
            .and_then(Value::as_str)
            .map(ToString::to_string),
        created_at: meta
            .get("CreatedAt")
            .and_then(Value::as_str)
            .and_then(|value| DateTime::parse_from_rfc3339(value).ok()),
    }
}

#[cfg(test)]
mod tests {
    use std::fs;

    use tempfile::tempdir;

    use super::*;

    #[test]
    fn imports_jsonl_aliases_and_payloads_and_snapshot_when_present() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("fixture.memory.jsonl");
        let snapshot_path = dir.path().join("fixture.memory.ams.json");

        fs::write(
            &db_path,
            concat!(
                "{\"type\":\"format\",\"name\":\"card-binder\",\"version\":1}\n",
                "{\"type\":\"memAnchor\",\"id\":\"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\",\"name\":\"Topic: Alias\"}\n",
                "{\"type\":\"card\",\"id\":\"11111111-1111-1111-1111-111111111111\",\"state\":\"Active\"}\n",
                "{\"type\":\"taglink\",\"card_id\":\"11111111-1111-1111-1111-111111111111\",\"memAnchor_id\":\"aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa\",\"meta\":{\"Relevance\":0.9,\"Reason\":\"why\",\"AddedBy\":\"seed\"}}\n",
                "{\"type\":\"card_payload\",\"card_id\":\"11111111-1111-1111-1111-111111111111\",\"title\":\"Title\",\"text\":\"Body\"}\n",
                "{\"type\":\"future_record\",\"x\":1}\n"
            ),
        )
        .unwrap();
        fs::write(
            &snapshot_path,
            concat!(
                "{\n",
                "  \"objects\": [\n",
                "    {\n",
                "      \"objectId\": \"chat-session:11111111-1111-1111-1111-111111111111\",\n",
                "      \"objectKind\": \"container\",\n",
                "      \"createdAt\": \"2026-03-13T00:00:00+00:00\",\n",
                "      \"updatedAt\": \"2026-03-13T00:00:00+00:00\"\n",
                "    }\n",
                "  ],\n",
                "  \"containers\": [\n",
                "    {\n",
                "      \"containerId\": \"chat-session:11111111-1111-1111-1111-111111111111\",\n",
                "      \"containerKind\": \"chat_session\",\n",
                "      \"headLinknodeId\": null,\n",
                "      \"tailLinknodeId\": null\n",
                "    }\n",
                "  ],\n",
                "  \"linkNodes\": []\n",
                "}\n"
            ),
        )
        .unwrap();

        let corpus = import_materialized_corpus(&db_path).unwrap();
        assert_eq!(corpus.cards.len(), 1);
        assert_eq!(corpus.binders.len(), 1);
        assert_eq!(corpus.tag_links.len(), 1);
        assert_eq!(corpus.payloads.len(), 1);
        assert_eq!(corpus.unknown_record_types.get("future_record"), Some(&1));
        assert!(corpus.snapshot.is_some());
        assert!(corpus.snapshot_contains_card_container(
            Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap()
        ));
    }

    #[test]
    fn taglinks_and_payloads_materialize_missing_endpoints() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("missing-endpoints.memory.jsonl");
        fs::write(
            &db_path,
            concat!(
                "{\"type\":\"format\",\"name\":\"card-binder\",\"version\":1}\n",
                "{\"type\":\"taglink\",\"card_id\":\"22222222-2222-2222-2222-222222222222\",\"binder_id\":\"bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb\"}\n",
                "{\"type\":\"card_payload\",\"card_id\":\"33333333-3333-3333-3333-333333333333\",\"source\":\"fixture\"}\n"
            ),
        )
        .unwrap();

        let corpus = import_materialized_corpus(&db_path).unwrap();
        let imported_binder = corpus
            .binders
            .get(&Uuid::parse_str("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb").unwrap())
            .unwrap();
        assert_eq!(imported_binder.name, "Imported");
        assert!(corpus
            .cards
            .contains_key(&Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap()));
        assert!(corpus
            .cards
            .contains_key(&Uuid::parse_str("33333333-3333-3333-3333-333333333333").unwrap()));
    }

    #[test]
    fn enforces_format_header_before_records() {
        let dir = tempdir().unwrap();
        let db_path = dir.path().join("missing-header.memory.jsonl");
        fs::write(
            &db_path,
            "{\"type\":\"card\",\"id\":\"11111111-1111-1111-1111-111111111111\",\"state\":\"Active\"}\n",
        )
        .unwrap();

        let err = import_materialized_corpus(&db_path).unwrap_err();
        assert!(err
            .to_string()
            .contains("format header must be the first record"));
    }

    #[test]
    fn imports_real_memoryctl_fixture_shape() {
        let fixture = PathBuf::from(env!("CARGO_MANIFEST_DIR"))
            .join("..")
            .join("..")
            .join("tools")
            .join("memoryctl")
            .join("tests")
            .join("fixtures")
            .join("mixed-card-states.memory.jsonl");

        let corpus = import_materialized_corpus(&fixture).unwrap();
        assert_eq!(corpus.cards.len(), 3);
        assert_eq!(corpus.binders.len(), 2);
        assert_eq!(corpus.tag_links.len(), 3);
        assert_eq!(corpus.payloads.len(), 3);
        assert!(corpus.snapshot.is_none());
    }
}
