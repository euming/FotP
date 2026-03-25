# Golden command outputs for characterization

These files snapshot current `memoryctl` command output against representative fixture DBs.

## Mapping

- `mixed-card-states.validate.txt`
- `mixed-card-states.query-explain.txt`
- `mixed-card-states.prompt.txt`

- `duplicate-link-attempts.validate.txt`
- `duplicate-link-attempts.list-memanchors.txt`
- `duplicate-link-attempts.export-graph.json`

- `missing-optional-payloads.validate.txt`
- `missing-optional-payloads.query-explain.txt`
- `missing-optional-payloads.prompt.txt`

## Notes

- Treat these as characterization baselines, not idealized behavior.
- If output changes intentionally, update fixtures/goldens in the same PR and call out rationale.
- Line endings should remain stable under repo defaults; avoid manual reformatting.
