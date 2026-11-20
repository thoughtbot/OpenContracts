# Hub client enricher

A new PDF enricher downloads the client list from Hub's API
(https://github.com/thoughtbot/hub/blob/main/doc/api/restful/README.md,
`GET /api/clients` with a personal token, every page) and highlights each
place a client's `name` or `legal_name` appears in the document, linking to
that client in Hub. Matching happens inside OpenContracts, so no document
text is sent to Hub. The highlight reuses the existing clickable-annotation
behaviour (`link_url` plus the `data` sidecar) instead of adding new UI.

## Job story

When I open a contract that was just uploaded and need to know which of our clients it involves
I want to see which clients we already have in Hub are named in it, and jump to their records
So I can check the existing relationship and open deals before I act on the contract, instead of searching Hub by hand

## Acceptance criteria

- [ ] Given an admin has saved a Hub personal token for the Hub client enricher and assigned the enricher to `application/pdf` in System Settings, and Hub has a client "Acme" with legal name "Acme Corporation", when a user uploads a PDF whose text contains "Acme Corporation", then once processing completes the document shows a `HUB_CLIENT` highlight over those words, clicking it opens `https://hub.thoughtbot.com/clients/acme`, and an editor of the document can delete it like any other highlight
- [ ] Given Hub has more clients than fit on one page of its API (e.g. 130 at 100 per page), when a PDF names a client that is only on the second page, then that client is still highlighted
- [ ] Given a client named "Acme", when a PDF mentions "ACME" but also "Acmeville", then only "ACME" is highlighted: matches ignore case but must be whole words
- [ ] Given the token is missing, Hub rejects it (401/403), or Hub is unreachable or times out, when a PDF is processed, then the document still reaches "completed" and is unlocked with no `HUB_CLIENT` highlights, and the worker log says why without including the token
- [ ] Given a `HUB_CLIENT` highlight, when anyone who can view the document reads it through the API, then it carries only the client's Hub id, name, slug and Hub URL, and no account manager, industry, description or any other Hub field
- [ ] Given the test suite, when `docker compose -f test.yml run django pytest opencontractserver/tests/test_hub_client_enricher.py` runs, then each criterion above has a passing test, with Hub's HTTP responses stubbed so no network call is made

## Where to look

Paths as of `01124709d`. Where the code was, not what to build.

- `opencontractserver/pipeline/enrichers/pdf_outline_enricher.py` — the only existing enricher. It has the same shape: read the PAWLs page tokens from `export_data`, then append non-structural `TOKEN_LABEL` entries to `labelled_text`
- `opencontractserver/utils/pdf_token_matching.py` — `page_text_tokens` and `union_bounds` turn a run of matched words into the bounds and token ids a highlight needs
- `opencontractserver/pipeline/rerankers/cohere_reranker.py` — a `SettingType.SECRET` key that appears in System Settings, plus `requests` with a timeout and a quiet fallback on every failure
- `opencontractserver/utils/importing.py:166` — where `link_url` and `data` on an enricher's annotation are saved. `frontend/src/components/annotator/utils/urlAnnotation.ts` makes any annotation with a `linkUrl` clickable, which is why there's no frontend work
- `opencontractserver/tests/test_pdf_outline_enricher.py` — the enricher test idiom: a hand-built export dict, then `enrich_document` called directly. `opencontractserver/tests/test_reranker.py:193` has the `_FakeResponse` used to stub `requests`
