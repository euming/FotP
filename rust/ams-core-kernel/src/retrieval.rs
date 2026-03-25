use std::collections::{BTreeMap, BTreeSet};

use anyhow::{Context, Result};
use chrono::{DateTime, FixedOffset};
use regex::Regex;
use uuid::Uuid;

use crate::active_inference::{compute_free_energy, compute_relevance_free_energy, extract_stereotype_prior, StereotypePrior};
use crate::fep_bootstrap::get_relevance_prior;
use crate::context::QueryContext;
use crate::corpus::{CardPayloadRecord, CardState, MaterializedCorpus};
use crate::route_memory::{compute_efe_biases, RouteMemoryBiasOptions, RouteMemoryStore, RouteMemoryTargetBias};

#[derive(Clone, Debug, PartialEq)]
pub struct EffectivePayload {
    pub title: Option<String>,
    pub text: Option<String>,
    pub source: Option<String>,
    pub updated_at: Option<DateTime<FixedOffset>>,
    pub origin: &'static str,
}

#[derive(Clone, Debug, PartialEq)]
pub struct RetrievalHit {
    pub card_id: Uuid,
    pub state: CardState,
    pub total_score: f64,
    pub text_score: f64,
    pub binder_score: f64,
    pub meta_score: f64,
    pub context_score: f64,
    pub route_memory_bias: f64,
    pub matched_tokens: Vec<String>,
    pub context_matches: Vec<String>,
    pub binder_names: Vec<String>,
    pub payload: Option<EffectivePayload>,
    pub scope_reason: String,
    pub route_memory_signal: Option<String>,
    pub free_energy_score: Option<f64>,
}

#[derive(Clone, Debug, Default, PartialEq)]
pub struct QueryOptions {
    pub top: usize,
    pub binder_filters: Vec<String>,
    pub seed_card: Option<Uuid>,
    pub state_filter: Option<CardState>,
    pub include_retracted: bool,
    pub use_fep: bool,
}

pub fn tokenize(query: &str) -> Vec<String> {
    static TOKEN_RX: std::sync::OnceLock<Regex> = std::sync::OnceLock::new();
    let token_rx = TOKEN_RX.get_or_init(|| Regex::new(r"[A-Za-z0-9_]+").expect("valid token regex"));
    if query.trim().is_empty() {
        return Vec::new();
    }

    let mut tokens = token_rx
        .find_iter(query)
        .map(|capture| capture.as_str().to_ascii_lowercase())
        .filter(|token| token.len() >= 2)
        .collect::<Vec<_>>();
    tokens.sort();
    tokens.dedup();
    tokens
}

pub fn query_cards(
    corpus: &MaterializedCorpus,
    query: &str,
    options: &QueryOptions,
    context: Option<&QueryContext>,
    route_memory: Option<&RouteMemoryStore>,
    bias_options: Option<&RouteMemoryBiasOptions>,
) -> Vec<RetrievalHit> {
    let top = options.top.max(1);
    let tokens = tokenize(query);
    let context_terms = context.map(QueryContext::context_terms).unwrap_or_default();
    let scoped_cards = scoped_candidate_cards(corpus, &options.binder_filters, options.seed_card);
    let default_bias_options = RouteMemoryBiasOptions::default();
    let effective_bias_options = bias_options.unwrap_or(&default_bias_options);

    // When FEP is enabled, collect stereotype priors and compute EFE-based biases
    let efe_biases: BTreeMap<Uuid, RouteMemoryTargetBias> = if options.use_fep {
        context
            .zip(route_memory)
            .zip(corpus.snapshot.as_ref())
            .map(|((ctx, rm), snapshot)| {
                let priors: Vec<StereotypePrior> = corpus
                    .cards
                    .keys()
                    .filter_map(|card_id| {
                        let container_id = corpus.snapshot_container_id_for_card(*card_id);
                        extract_stereotype_prior(snapshot, &container_id)
                    })
                    .collect();
                let efe_vec = compute_efe_biases(rm, ctx, &priors, effective_bias_options);
                efe_vec
                    .into_iter()
                    .map(|b| (b.target_card_id, b))
                    .collect()
            })
            .unwrap_or_default()
    } else {
        BTreeMap::new()
    };

    let route_biases = context
        .zip(route_memory)
        .map(|(context, route_memory)| route_memory.get_target_biases(context, effective_bias_options))
        .unwrap_or_default();

    let mut hits = corpus
        .cards
        .values()
        .filter(|card| options.include_retracted || !matches!(card.state, CardState::Retracted))
        .filter(|card| options.state_filter.is_none_or(|state| card.state == state))
        .filter(|card| scoped_cards.as_ref().is_none_or(|scope| scope.contains(&card.card_id)))
        .filter_map(|card| {
            let payload = effective_payload(corpus, card.card_id);
            let binder_names = corpus
                .binders_for_card(card.card_id)
                .into_iter()
                .map(|(binder, _)| binder.name.clone())
                .collect::<Vec<_>>();

            let text_score = score_text(tokens.as_slice(), payload.as_ref());
            let binder_score = score_binders(tokens.as_slice(), binder_names.as_slice());
            let meta_score = score_meta(corpus, card.card_id);
            let context_matches =
                matched_tokens(context_terms.as_slice(), payload.as_ref(), binder_names.as_slice());
            let context_score = score_context(context_matches.as_slice(), context);
            // Use EFE biases when FEP is enabled and available, otherwise fall back to heuristic
            let (route_memory_bias, route_memory_signal) = if options.use_fep && !efe_biases.is_empty() {
                score_route_memory(card.card_id, &efe_biases)
            } else {
                score_route_memory(card.card_id, &route_biases)
            };
            let total_score = text_score + binder_score + meta_score + context_score + route_memory_bias;
            if total_score <= 0.0 {
                return None;
            }

            let free_energy_score = if options.use_fep {
                corpus
                    .snapshot
                    .as_ref()
                    .and_then(|snapshot| {
                        let container_id = corpus.snapshot_container_id_for_card(card.card_id);

                        // Try relevance-based FE first (from bootstrapped priors)
                        let relevance_fe = context.and_then(|ctx| {
                            let param = get_relevance_prior(
                                snapshot,
                                &container_id,
                                ctx.scope_lens(),
                                &ctx.agent_role,
                            )?;
                            // Observation: use route_memory_bias as a proxy for "won"
                            // positive bias -> observation closer to 1.0
                            let obs = if route_memory_bias > 0.0 { 1.0 } else if route_memory_bias < 0.0 { 0.0 } else { 0.5 };
                            Some(compute_relevance_free_energy(param.mean, param.variance, obs))
                        });

                        if let Some(fe) = relevance_fe {
                            return Some(fe);
                        }

                        // Fall back to stereotype-based FE
                        let prior = extract_stereotype_prior(snapshot, &container_id)?;
                        let mut observations = BTreeMap::new();
                        observations.insert("text_score".to_string(), text_score);
                        observations.insert("binder_score".to_string(), binder_score);
                        observations.insert("context_score".to_string(), context_score);
                        let precisions = BTreeMap::new();
                        Some(compute_free_energy(&prior, &observations, &precisions))
                    })
            } else {
                None
            };
            let matched_tokens = matched_tokens(tokens.as_slice(), payload.as_ref(), binder_names.as_slice());
            Some(RetrievalHit {
                card_id: card.card_id,
                state: card.state,
                total_score,
                text_score,
                binder_score,
                meta_score,
                context_score,
                route_memory_bias,
                matched_tokens,
                context_matches,
                binder_names,
                payload,
                scope_reason: describe_scope(options, context, card.card_id, route_memory_signal.as_deref()),
                route_memory_signal,
                free_energy_score,
            })
        })
        .collect::<Vec<_>>();

    hits.sort_by(|left, right| {
        right
            .total_score
            .partial_cmp(&left.total_score)
            .unwrap_or(std::cmp::Ordering::Equal)
            .then_with(|| {
                right
                    .text_score
                    .partial_cmp(&left.text_score)
                    .unwrap_or(std::cmp::Ordering::Equal)
            })
            .then_with(|| left.card_id.cmp(&right.card_id))
    });
    hits.truncate(top);
    hits
}

pub fn render_query_hits(hits: &[RetrievalHit], explain: bool) -> String {
    let mut out = String::new();
    out.push_str(&format!("hits={}\n", hits.len()));
    for hit in hits {
        out.push_str(&format!(
            "card={} total={:.3} text={:.3} binder={:.3} meta={:.3} state={} title={}\n",
            hit.card_id,
            hit.total_score,
            hit.text_score,
            hit.binder_score,
            hit.meta_score,
            hit.state.as_str(),
            hit.payload
                .as_ref()
                .and_then(|payload| payload.title.as_deref())
                .unwrap_or("<none>")
        ));
        if explain {
            out.push_str(&format!(
                "  matched_tokens={}\n",
                if hit.matched_tokens.is_empty() {
                    "<none>".to_string()
                } else {
                    hit.matched_tokens.join(", ")
                }
            ));
            out.push_str(&format!(
                "  context_tokens={}\n",
                if hit.context_matches.is_empty() {
                    "<none>".to_string()
                } else {
                    hit.context_matches.join(", ")
                }
            ));
            out.push_str(&format!("  context_score={:.3}\n", hit.context_score));
            out.push_str(&format!("  route_memory_bias={:.3}\n", hit.route_memory_bias));
            out.push_str(&format!(
                "  route_memory_signal={}\n",
                hit.route_memory_signal.as_deref().unwrap_or("<none>")
            ));
            out.push_str(&format!(
                "  binders={}\n",
                if hit.binder_names.is_empty() {
                    "<none>".to_string()
                } else {
                    hit.binder_names.join(", ")
                }
            ));
            if let Some(fe) = hit.free_energy_score {
                out.push_str(&format!("  free_energy={:.3}\n", fe));
            }
            out.push_str(&format!("  scope={}\n", hit.scope_reason));
            out.push_str(&format!(
                "  payload_origin={}\n",
                hit.payload
                    .as_ref()
                    .map(|payload| payload.origin)
                    .unwrap_or("<none>")
            ));
        }
    }
    out
}

pub fn parse_binder_filters(value: Option<&str>) -> Vec<String> {
    value
        .into_iter()
        .flat_map(|raw| raw.split(','))
        .map(str::trim)
        .filter(|value| !value.is_empty())
        .map(ToString::to_string)
        .collect()
}

pub fn parse_seed_card(value: Option<&str>) -> Result<Option<Uuid>> {
    value
        .map(|raw| Uuid::parse_str(raw).with_context(|| format!("invalid seed card id '{raw}'")))
        .transpose()
}

fn scoped_candidate_cards(
    corpus: &MaterializedCorpus,
    binder_filters: &[String],
    seed_card: Option<Uuid>,
) -> Option<BTreeSet<Uuid>> {
    let binder_scope = if binder_filters.is_empty() {
        None
    } else {
        let normalized = binder_filters
            .iter()
            .map(|value| value.to_ascii_lowercase())
            .collect::<BTreeSet<_>>();
        Some(
            corpus
                .binders
                .values()
                .filter(|binder| normalized.contains(&binder.name.to_ascii_lowercase()))
                .flat_map(|binder| {
                    corpus
                        .cards_in_binder(binder.binder_id)
                        .into_iter()
                        .map(|(card, _, _)| card.card_id)
                })
                .collect::<BTreeSet<_>>(),
        )
    };

    let seed_scope = seed_card.map(|seed_card| {
        let binder_ids = corpus
            .binders_for_card(seed_card)
            .into_iter()
            .map(|(binder, _)| binder.binder_id)
            .collect::<Vec<_>>();
        let mut cards = BTreeSet::from([seed_card]);
        for binder_id in binder_ids {
            for (card, _, _) in corpus.cards_in_binder(binder_id) {
                cards.insert(card.card_id);
            }
        }
        cards
    });

    match (binder_scope, seed_scope) {
        (None, None) => None,
        (Some(scope), None) | (None, Some(scope)) => Some(scope),
        (Some(left), Some(right)) => Some(left.intersection(&right).copied().collect()),
    }
}

fn effective_payload(corpus: &MaterializedCorpus, card_id: Uuid) -> Option<EffectivePayload> {
    if let Some(payload) = corpus.payloads.get(&card_id) {
        return Some(to_effective_payload(payload, "jsonl"));
    }

    let snapshot = corpus.snapshot.as_ref()?;
    let container_id = corpus.snapshot_container_id_for_card(card_id);
    let container = snapshot.containers().get(&container_id)?;
    let title = container
        .metadata
        .as_ref()
        .and_then(|meta| meta.get("title"))
        .and_then(|value| value.as_str())
        .map(ToString::to_string)
        .or_else(|| Some(container_id.clone()));
    let source = container
        .metadata
        .as_ref()
        .and_then(|meta| meta.get("source"))
        .and_then(|value| value.as_str())
        .map(ToString::to_string);
    let updated_at = container
        .metadata
        .as_ref()
        .and_then(|meta| meta.get("started_at"))
        .and_then(|value| value.as_str())
        .and_then(|value| DateTime::parse_from_rfc3339(value).ok());

    let text = snapshot
        .iterate_forward(&container_id)
        .into_iter()
        .filter_map(|link| snapshot.objects().get(&link.object_id))
        .map(|object| {
            object
                .semantic_payload
                .as_ref()
                .and_then(|payload| payload.summary.clone())
                .unwrap_or_else(|| object.object_id.clone())
        })
        .collect::<Vec<_>>()
        .join("\n");

    Some(EffectivePayload {
        title,
        text: Some(text).filter(|value| !value.is_empty()),
        source,
        updated_at,
        origin: "snapshot",
    })
}

fn to_effective_payload(payload: &CardPayloadRecord, origin: &'static str) -> EffectivePayload {
    EffectivePayload {
        title: payload.title.clone(),
        text: payload.text.clone(),
        source: payload.source.clone(),
        updated_at: payload.updated_at,
        origin,
    }
}

fn score_text(tokens: &[String], payload: Option<&EffectivePayload>) -> f64 {
    let Some(payload) = payload else {
        return 0.0;
    };
    let haystack = format!(
        "{}\n{}",
        payload.title.as_deref().unwrap_or_default(),
        payload.text.as_deref().unwrap_or_default()
    )
    .to_ascii_lowercase();
    tokens
        .iter()
        .filter(|token| haystack.contains(token.as_str()))
        .count() as f64
}

fn score_binders(tokens: &[String], binder_names: &[String]) -> f64 {
    binder_names
        .iter()
        .map(|binder_name| {
            let binder_name = binder_name.to_ascii_lowercase();
            tokens
                .iter()
                .filter(|token| binder_name.contains(token.as_str()))
                .count() as f64
                * 0.25
        })
        .sum()
}

fn score_meta(corpus: &MaterializedCorpus, card_id: Uuid) -> f64 {
    corpus
        .binders_for_card(card_id)
        .into_iter()
        .map(|(_, link)| (link.meta.relevance as f64).clamp(0.0, 1.0) * 0.5)
        .sum()
}

fn score_context(context_matches: &[String], context: Option<&QueryContext>) -> f64 {
    if context.is_none() || context_matches.is_empty() {
        return 0.0;
    }
    (context_matches.len() as f64) * 0.2
}

fn score_route_memory(
    card_id: Uuid,
    route_biases: &BTreeMap<Uuid, RouteMemoryTargetBias>,
) -> (f64, Option<String>) {
    let Some(bias) = route_biases.get(&card_id) else {
        return (0.0, None);
    };
    if bias.bias.abs() < 0.0001 {
        return (0.0, None);
    }

    let direction = if bias.bias >= 0.0 { "reuse" } else { "suppress" };
    (
        bias.bias,
        Some(format!(
            "route-memory:{direction}(strong={},weak={},miss={})",
            bias.strong_wins, bias.weak_wins, bias.candidate_misses
        )),
    )
}

fn matched_tokens(tokens: &[String], payload: Option<&EffectivePayload>, binder_names: &[String]) -> Vec<String> {
    let payload_hay = payload.map(|payload| {
        format!(
            "{}\n{}",
            payload.title.as_deref().unwrap_or_default(),
            payload.text.as_deref().unwrap_or_default()
        )
        .to_ascii_lowercase()
    });
    let binder_hay = binder_names.join("\n").to_ascii_lowercase();

    tokens
        .iter()
        .filter(|token| {
            payload_hay
                .as_ref()
                .is_some_and(|payload| payload.contains(token.as_str()))
                || binder_hay.contains(token.as_str())
        })
        .cloned()
        .collect()
}

fn describe_scope(
    options: &QueryOptions,
    context: Option<&QueryContext>,
    card_id: Uuid,
    route_memory_signal: Option<&str>,
) -> String {
    let mut parts = Vec::new();
    if !options.binder_filters.is_empty() {
        parts.push(format!("binder:{}", options.binder_filters.join(",")));
    }
    if let Some(seed_card) = options.seed_card {
        parts.push(format!(
            "seed-card:{}{}",
            seed_card,
            if seed_card == card_id { " (self)" } else { "" }
        ));
    }
    if let Some(context) = context {
        parts.push(format!("scope_lens:{}", context.scope_lens()));
    }
    if let Some(route_memory_signal) = route_memory_signal {
        parts.push(route_memory_signal.to_string());
    }
    if parts.is_empty() {
        "global".to_string()
    } else {
        parts.join(" + ")
    }
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;
    use std::path::PathBuf;

    use crate::corpus::{BinderRecord, CardPayloadRecord, CardRecord, MaterializedCorpus, TagLinkMeta, TagLinkRecord};
    use crate::store::AmsStore;

    use super::*;

    fn make_corpus() -> MaterializedCorpus {
        let card_a = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap();
        let card_b = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2").unwrap();
        let card_c = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa3").unwrap();
        let binder_search = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let binder_ops = Uuid::parse_str("22222222-2222-2222-2222-222222222222").unwrap();
        MaterializedCorpus {
            db_path: PathBuf::from("fixture.memory.jsonl"),
            snapshot_path: None,
            snapshot: None,
            cards: BTreeMap::from([
                (
                    card_a,
                    CardRecord {
                        card_id: card_a,
                        state: CardState::Active,
                        state_reason: None,
                    },
                ),
                (
                    card_b,
                    CardRecord {
                        card_id: card_b,
                        state: CardState::Active,
                        state_reason: None,
                    },
                ),
                (
                    card_c,
                    CardRecord {
                        card_id: card_c,
                        state: CardState::Retracted,
                        state_reason: Some("policy".to_string()),
                    },
                ),
            ]),
            binders: BTreeMap::from([
                (
                    binder_search,
                    BinderRecord {
                        binder_id: binder_search,
                        name: "Topic: Search".to_string(),
                    },
                ),
                (
                    binder_ops,
                    BinderRecord {
                        binder_id: binder_ops,
                        name: "Topic: Ops".to_string(),
                    },
                ),
            ]),
            tag_links: BTreeMap::from([
                (
                    (card_a, binder_search),
                    TagLinkRecord {
                        card_id: card_a,
                        binder_id: binder_search,
                        meta: TagLinkMeta {
                            relevance: 0.9,
                            reason: Some("search core".to_string()),
                            added_by: None,
                            created_at: None,
                        },
                    },
                ),
                (
                    (card_b, binder_ops),
                    TagLinkRecord {
                        card_id: card_b,
                        binder_id: binder_ops,
                        meta: TagLinkMeta {
                            relevance: 0.7,
                            reason: Some("ops history".to_string()),
                            added_by: None,
                            created_at: None,
                        },
                    },
                ),
                (
                    (card_c, binder_search),
                    TagLinkRecord {
                        card_id: card_c,
                        binder_id: binder_search,
                        meta: TagLinkMeta {
                            relevance: 0.95,
                            reason: Some("deprecated".to_string()),
                            added_by: None,
                            created_at: None,
                        },
                    },
                ),
            ]),
            payloads: BTreeMap::from([
                (
                    card_a,
                    CardPayloadRecord {
                        card_id: card_a,
                        title: Some("Search cache invalidation fix".to_string()),
                        text: Some("Cache key normalization prevents stale results.".to_string()),
                        source: Some("fixture".to_string()),
                        updated_at: None,
                    },
                ),
                (
                    card_b,
                    CardPayloadRecord {
                        card_id: card_b,
                        title: Some("Ops retry tuning".to_string()),
                        text: Some("Retry backoff reduced queue pressure.".to_string()),
                        source: Some("fixture".to_string()),
                        updated_at: None,
                    },
                ),
            ]),
            unknown_record_types: BTreeMap::new(),
        }
    }

    #[test]
    fn tokenization_matches_legacy_rules() {
        let tokens = tokenize("Search cache, cache! A _b c x1");
        assert_eq!(tokens, vec!["_b", "cache", "search", "x1"]);
    }

    #[test]
    fn query_cards_uses_legacy_score_shape_and_skips_retracted_by_default() {
        let hits = query_cards(
            &make_corpus(),
            "search cache",
            &QueryOptions {
                top: 10,
                ..QueryOptions::default()
            },
            None,
            None,
            None,
        );
        assert_eq!(hits.len(), 2);
        assert_eq!(
            hits[0].card_id,
            Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap()
        );
        assert!(hits[0].text_score >= 2.0);
        assert!(hits[0].binder_score >= 0.25);
        assert!(!hits.iter().any(|hit| hit.card_id == Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa3").unwrap()));
    }

    #[test]
    fn query_cards_can_scope_by_binder_name() {
        let hits = query_cards(
            &make_corpus(),
            "retry ops",
            &QueryOptions {
                top: 10,
                binder_filters: vec!["Topic: Ops".to_string()],
                ..QueryOptions::default()
            },
            None,
            None,
            None,
        );
        assert_eq!(hits.len(), 1);
        assert_eq!(
            hits[0].card_id,
            Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2").unwrap()
        );
        assert!(hits[0].scope_reason.contains("binder:Topic: Ops"));
    }

    #[test]
    fn query_cards_can_scope_by_seed_card_shared_binders() {
        let hits = query_cards(
            &make_corpus(),
            "search",
            &QueryOptions {
                top: 10,
                seed_card: Some(Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa1").unwrap()),
                include_retracted: true,
                ..QueryOptions::default()
            },
            None,
            None,
            None,
        );
        assert_eq!(hits.len(), 2);
        assert!(hits.iter().all(|hit| hit.scope_reason.contains("seed-card")));
    }

    #[test]
    fn effective_payload_falls_back_to_snapshot_transcript() {
        let card_id = Uuid::parse_str("11111111-1111-1111-1111-111111111111").unwrap();
        let mut store = AmsStore::new();
        store.create_container(format!("chat-session:{card_id}"), "chat_session", "chat_session").unwrap();
        store
            .upsert_object("chat-msg:1", "chat_message", None, None, None)
            .unwrap();
        store.objects_mut().get_mut("chat-msg:1").unwrap().semantic_payload = Some(crate::model::SemanticPayload {
            embedding: None,
            tags: None,
            summary: Some("Snapshot transcript line".to_string()),
            provenance: None,
        });
        let container = store
            .containers_mut()
            .get_mut(&format!("chat-session:{card_id}"))
            .unwrap();
        container.metadata = Some(BTreeMap::from([
            ("title".to_string(), serde_json::Value::String("Snapshot title".to_string())),
            ("source".to_string(), serde_json::Value::String("snapshot".to_string())),
        ]));
        store.add_object(format!("chat-session:{card_id}"), "chat-msg:1", None, None).unwrap();

        let corpus = MaterializedCorpus {
            db_path: PathBuf::from("fixture.memory.jsonl"),
            snapshot_path: Some(PathBuf::from("fixture.memory.ams.json")),
            snapshot: Some(store),
            cards: BTreeMap::from([(
                card_id,
                CardRecord {
                    card_id,
                    state: CardState::Active,
                    state_reason: None,
                },
            )]),
            binders: BTreeMap::new(),
            tag_links: BTreeMap::new(),
            payloads: BTreeMap::new(),
            unknown_record_types: BTreeMap::new(),
        };

        let hits = query_cards(
            &corpus,
            "snapshot transcript",
            &QueryOptions {
                top: 5,
                ..QueryOptions::default()
            },
            None,
            None,
            None,
        );
        assert_eq!(hits.len(), 1);
        assert_eq!(hits[0].payload.as_ref().unwrap().origin, "snapshot");
        assert_eq!(hits[0].payload.as_ref().unwrap().title.as_deref(), Some("Snapshot title"));
    }

    #[test]
    fn query_cards_adds_context_bias_when_context_terms_match() {
        let corpus = make_corpus();
        let context = QueryContext {
            lineage: vec![crate::context::LineageScope {
                level: "self".to_string(),
                object_id: "task-thread:child".to_string(),
                node_id: "child".to_string(),
                title: "Search cache implementation".to_string(),
                current_step: "Implement search cache".to_string(),
                next_command: "cargo test".to_string(),
                branch_off_anchor: None,
                artifact_refs: vec!["src/MemoryGraph.Application/RetrievalService.cs".to_string()],
            }],
            agent_role: "implementer".to_string(),
            mode: "build".to_string(),
            failure_bucket: None,
            active_artifacts: Vec::new(),
            traversal_budget: 3,
            source: "explicit".to_string(),
        };
        let hits = query_cards(
            &corpus,
            "search cache",
            &QueryOptions {
                top: 10,
                ..QueryOptions::default()
            },
            Some(&context),
            None,
            None,
        );
        assert!(!hits.is_empty());
        assert!(hits[0].context_score > 0.0);
        assert!(hits[0].scope_reason.contains("scope_lens:local-first-lineage"));
    }

    #[test]
    fn query_cards_applies_route_memory_bias_and_exposes_signal() {
        let corpus = make_corpus();
        let context = QueryContext {
            lineage: vec![crate::context::LineageScope {
                level: "self".to_string(),
                object_id: "task-thread:child".to_string(),
                node_id: "child-thread".to_string(),
                title: "Search cache implementation".to_string(),
                current_step: "Implement search cache".to_string(),
                next_command: "cargo test".to_string(),
                branch_off_anchor: None,
                artifact_refs: vec![],
            }],
            agent_role: "implementer".to_string(),
            mode: "build".to_string(),
            failure_bucket: None,
            active_artifacts: Vec::new(),
            traversal_budget: 3,
            source: "explicit".to_string(),
        };
        let promoted = Uuid::parse_str("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaa2").unwrap();
        let route_memory = crate::route_memory::RouteMemoryStore::from_episodes([
            crate::route_memory::RouteReplayEpisodeEntry {
                frame: crate::route_memory::RouteReplayFrameInput {
                    scope_lens: "local-first-lineage".to_string(),
                    agent_role: "implementer".to_string(),
                    mode: "build".to_string(),
                    lineage_node_ids: vec!["child-thread".to_string()],
                    artifact_refs: None,
                    failure_bucket: None,
                },
                route: crate::route_memory::RouteReplayRouteInput {
                    ranking_source: "raw-lesson".to_string(),
                    path: "retrieval-graph:self-thread -> in-bucket".to_string(),
                    cost: 0.55,
                    risk_flags: Some(vec![]),
                },
                episode: crate::route_memory::RouteReplayEpisodeInput {
                    query_text: "retry ops".to_string(),
                    occurred_at: chrono::DateTime::parse_from_rfc3339("2026-03-10T08:00:00+00:00").unwrap(),
                    weak_result: false,
                    used_fallback: false,
                    winning_target_ref: promoted.to_string(),
                    top_target_refs: vec![promoted.to_string()],
                    user_feedback: None,
                    tool_outcome: None,
                },
                candidate_target_refs: vec![promoted.to_string()],
                winning_target_ref: promoted.to_string(),
            },
        ]);

        let hits = query_cards(
            &corpus,
            "retry ops",
            &QueryOptions {
                top: 10,
                ..QueryOptions::default()
            },
            Some(&context),
            Some(&route_memory),
            Some(&crate::route_memory::RouteMemoryBiasOptions::default()),
        );

        assert!(!hits.is_empty());
        assert_eq!(hits[0].card_id, promoted);
        assert!(hits[0].route_memory_bias > 0.0);
        assert!(hits[0].scope_reason.contains("route-memory:reuse"));
    }
}
