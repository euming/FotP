use std::fmt::Write;

use anyhow::{anyhow, Context, Result};
use uuid::Uuid;

use crate::corpus::{CardState, MaterializedCorpus};

pub fn corpus_summary(corpus: &MaterializedCorpus) -> String {
    let active = corpus
        .cards
        .values()
        .filter(|card| matches!(card.state, CardState::Active))
        .count();
    let tombstoned = corpus
        .cards
        .values()
        .filter(|card| matches!(card.state, CardState::Tombstoned))
        .count();
    let retracted = corpus
        .cards
        .values()
        .filter(|card| matches!(card.state, CardState::Retracted))
        .count();
    let payloads_with_text = corpus
        .payloads
        .values()
        .filter(|payload| payload.text.as_deref().is_some_and(|text| !text.is_empty()))
        .count();
    let cards_with_snapshot = corpus
        .cards
        .keys()
        .filter(|card_id| corpus.snapshot_contains_card_container(**card_id))
        .count();

    let mut out = String::new();
    let _ = writeln!(out, "db_source={}", corpus.db_path.display());
    let _ = writeln!(
        out,
        "snapshot_source={}",
        corpus
            .snapshot_path
            .as_ref()
            .map(|path| path.display().to_string())
            .unwrap_or_else(|| "<none>".to_string())
    );
    let _ = writeln!(out, "cards={}", corpus.cards.len());
    let _ = writeln!(out, "cards_active={active}");
    let _ = writeln!(out, "cards_tombstoned={tombstoned}");
    let _ = writeln!(out, "cards_retracted={retracted}");
    let _ = writeln!(out, "binders={}", corpus.binders.len());
    let _ = writeln!(out, "tag_links={}", corpus.tag_links.len());
    let _ = writeln!(out, "payloads={}", corpus.payloads.len());
    let _ = writeln!(out, "payloads_with_text={payloads_with_text}");
    let _ = writeln!(out, "cards_with_snapshot_container={cards_with_snapshot}");
    let _ = writeln!(out, "snapshot_loaded={}", corpus.snapshot.is_some());
    let _ = writeln!(
        out,
        "unknown_record_types={}",
        if corpus.unknown_record_types.is_empty() {
            "<none>".to_string()
        } else {
            corpus
                .unknown_record_types
                .iter()
                .map(|(record_type, count)| format!("{record_type}:{count}"))
                .collect::<Vec<_>>()
                .join(", ")
        }
    );
    out
}

pub fn list_cards(corpus: &MaterializedCorpus, state: Option<CardState>) -> String {
    let mut out = String::new();
    let cards = corpus
        .cards
        .values()
        .filter(|card| state.is_none_or(|expected| card.state == expected))
        .collect::<Vec<_>>();
    let _ = writeln!(out, "cards={}", cards.len());
    for card in cards {
        let title = corpus
            .payloads
            .get(&card.card_id)
            .and_then(|payload| payload.title.as_deref())
            .unwrap_or("<none>");
        let binder_count = corpus.binders_for_card(card.card_id).len();
        let _ = writeln!(
            out,
            "card={} state={} binders={} title={}",
            card.card_id,
            card.state.as_str(),
            binder_count,
            title
        );
    }
    out
}

pub fn show_card(corpus: &MaterializedCorpus, card_id: &str) -> Result<String> {
    let card_id = Uuid::parse_str(card_id).with_context(|| format!("invalid card id '{card_id}'"))?;
    let card = corpus
        .cards
        .get(&card_id)
        .ok_or_else(|| anyhow!("card '{}' not found", card_id))?;

    let payload = corpus.payloads.get(&card_id);
    let binders = corpus.binders_for_card(card_id);

    let mut out = String::new();
    let _ = writeln!(out, "card={}", card.card_id);
    let _ = writeln!(out, "state={}", card.state.as_str());
    let _ = writeln!(
        out,
        "state_reason={}",
        card.state_reason.as_deref().unwrap_or("<none>")
    );
    let _ = writeln!(
        out,
        "snapshot_container={}",
        corpus.snapshot_container_id_for_card(card.card_id)
    );
    let _ = writeln!(
        out,
        "snapshot_present={}",
        corpus.snapshot_contains_card_container(card.card_id)
    );
    if let Some(payload) = payload {
        let _ = writeln!(out, "payload_title={}", payload.title.as_deref().unwrap_or("<none>"));
        let _ = writeln!(out, "payload_source={}", payload.source.as_deref().unwrap_or("<none>"));
        let _ = writeln!(
            out,
            "payload_updated_at={}",
            payload
                .updated_at
                .map(|value| value.to_rfc3339())
                .unwrap_or_else(|| "<none>".to_string())
        );
        let _ = writeln!(out, "payload_text_len={}", payload.text.as_deref().map(str::len).unwrap_or(0));
    } else {
        let _ = writeln!(out, "payload=<none>");
    }
    let _ = writeln!(out, "binders={}", binders.len());
    for (binder, link) in binders {
        let _ = writeln!(
            out,
            "binder={} name={} relevance={} reason={}",
            binder.binder_id,
            binder.name,
            link.meta.relevance,
            link.meta.reason.as_deref().unwrap_or("<none>")
        );
    }
    Ok(out)
}

pub fn list_binders(corpus: &MaterializedCorpus, contains: Option<&str>) -> String {
    let mut binders = corpus.binders.values().collect::<Vec<_>>();
    binders.sort_by(|left, right| left.name.cmp(&right.name).then_with(|| left.binder_id.cmp(&right.binder_id)));

    let filtered = binders
        .into_iter()
        .filter(|binder| {
            contains.is_none_or(|value| binder.name.to_ascii_lowercase().contains(&value.to_ascii_lowercase()))
        })
        .collect::<Vec<_>>();

    let mut out = String::new();
    let _ = writeln!(out, "binders={}", filtered.len());
    for binder in filtered {
        let _ = writeln!(
            out,
            "binder={} name={} cards={}",
            binder.binder_id,
            binder.name,
            corpus.cards_in_binder(binder.binder_id).len()
        );
    }
    out
}

pub fn show_binder(corpus: &MaterializedCorpus, binder_id: &str) -> Result<String> {
    let binder_id = Uuid::parse_str(binder_id).with_context(|| format!("invalid binder id '{binder_id}'"))?;
    let binder = corpus
        .binders
        .get(&binder_id)
        .ok_or_else(|| anyhow!("binder '{}' not found", binder_id))?;
    let cards = corpus.cards_in_binder(binder_id);

    let mut out = String::new();
    let _ = writeln!(out, "binder={}", binder.binder_id);
    let _ = writeln!(out, "name={}", binder.name);
    let _ = writeln!(out, "cards={}", cards.len());
    for (card, payload, link) in cards {
        let _ = writeln!(
            out,
            "card={} state={} title={} relevance={}",
            card.card_id,
            card.state.as_str(),
            payload.and_then(|value| value.title.as_deref()).unwrap_or("<none>"),
            link.meta.relevance
        );
    }
    Ok(out)
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use uuid::Uuid;

    use crate::corpus::{BinderRecord, CardPayloadRecord, CardRecord, TagLinkMeta, TagLinkRecord};

    use super::*;

    fn make_corpus() -> MaterializedCorpus {
        let card_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let binder_id = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa").unwrap();
        MaterializedCorpus {
            db_path: PathBuf::from("fixture.memory.jsonl"),
            snapshot_path: None,
            snapshot: None,
            cards: BTreeMap::from([(
                card_id,
                CardRecord {
                    card_id,
                    state: CardState::Active,
                    state_reason: None,
                },
            )]),
            binders: BTreeMap::from([(
                binder_id,
                BinderRecord {
                    binder_id,
                    name: "Topic: Test".to_string(),
                },
            )]),
            tag_links: BTreeMap::from([(
                (card_id, binder_id),
                TagLinkRecord {
                    card_id,
                    binder_id,
                    meta: TagLinkMeta {
                        relevance: 0.9,
                        reason: Some("seed".to_string()),
                        added_by: None,
                        created_at: None,
                    },
                },
            )]),
            payloads: BTreeMap::from([(
                card_id,
                CardPayloadRecord {
                    card_id,
                    title: Some("Fixture title".to_string()),
                    text: Some("Fixture body".to_string()),
                    source: Some("fixture".to_string()),
                    updated_at: None,
                },
            )]),
            unknown_record_types: BTreeMap::new(),
        }
    }

    #[test]
    fn summary_reports_counts() {
        let summary = corpus_summary(&make_corpus());
        assert!(summary.contains("cards=1"));
        assert!(summary.contains("binders=1"));
        assert!(summary.contains("payloads_with_text=1"));
    }

    #[test]
    fn show_card_reports_binders_and_payload() {
        let output = show_card(&make_corpus(), "11111111-1111-1111-1111-111111111111").unwrap();
        assert!(output.contains("payload_title=Fixture title"));
        assert!(output.contains("binder=aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"));
    }
}
