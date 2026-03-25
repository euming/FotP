use anyhow::Result;

use crate::context::{build_query_context, render_context, QueryContextOptions};
use crate::corpus::MaterializedCorpus;
use crate::corpus::CardState;
use crate::retrieval::{parse_binder_filters, parse_seed_card, query_cards, render_query_hits, QueryOptions};
use crate::route_memory::{RouteMemoryBiasOptions, RouteMemoryStore};

pub fn run_query_cards(
    corpus: &MaterializedCorpus,
    query: &str,
    top: usize,
    binder_filters: Option<&str>,
    seed_card: Option<&str>,
    state_filter: Option<CardState>,
    include_retracted: bool,
    explain: bool,
    context_options: QueryContextOptions,
    route_memory: Option<&RouteMemoryStore>,
    route_memory_bias_options: &RouteMemoryBiasOptions,
) -> Result<String> {
    let context = build_query_context(corpus, &context_options)?;
    let options = QueryOptions {
        top,
        binder_filters: parse_binder_filters(binder_filters),
        seed_card: parse_seed_card(seed_card)?,
        state_filter,
        include_retracted,
        ..QueryOptions::default()
    };
    let hits = query_cards(
        corpus,
        query,
        &options,
        context.as_ref(),
        route_memory,
        Some(route_memory_bias_options),
    );
    let mut out = String::new();
    out.push_str("# RUST AGENT MEMORY\n\n");
    out.push_str(&format!("Query: {query}\n"));
    let rendered_context = render_context(context.as_ref());
    if !rendered_context.is_empty() {
        out.push('\n');
        out.push_str(&rendered_context);
        out.push('\n');
    }
    out.push('\n');
    out.push_str(&render_query_hits(&hits, explain));
    out.push_str(&format!(
        "scope_lens={}\n",
        context
            .as_ref()
            .map(|context| context.scope_lens())
            .unwrap_or("global")
    ));
    Ok(out)
}
