"""Enricher that highlights the Hub clients named in a PDF.

Downloads the client list from Hub (``GET /api/clients`` with a personal
token) and emits one non-structural ``HUB_CLIENT`` token annotation wherever a
client's ``name`` or ``legal_name`` appears among the PAWLs tokens, with
``link_url`` pointing at the client in Hub. Matching happens here, so no
document text is sent to Hub.
"""

import logging
import re
from dataclasses import dataclass, field
from typing import ClassVar

import requests

from opencontractserver.constants.annotations import HUB_CLIENT_LABEL
from opencontractserver.constants.document_processing import (
    HUB_CLIENTS_API_URL,
    HUB_CLIENTS_PER_PAGE,
    HUB_REQUEST_TIMEOUT_SECONDS,
)
from opencontractserver.pipeline.base.enricher import BaseEnricher
from opencontractserver.pipeline.base.file_types import FileTypeEnum
from opencontractserver.pipeline.base.settings_schema import (
    PipelineSetting,
    SettingType,
)
from opencontractserver.types.dicts import (
    OpenContractDocExport,
    OpenContractsAnnotationPythonType,
    OpenContractsSinglePageAnnotationType,
)
from opencontractserver.utils.pdf_token_matching import (
    page_text_tokens,
    single_page_token_annotation,
)

logger = logging.getLogger(__name__)

_EDGE_PUNCTUATION = re.compile(r"^\W+|\W+$")


def _match_key(word: str) -> str:
    """Fold a word for matching: ignore case and punctuation at either end."""
    return _EDGE_PUNCTUATION.sub("", word.casefold())


class HubClientEnricher(BaseEnricher):
    """Emit HUB_CLIENT annotations where a Hub client's name appears."""

    title: str = "Hub Client Enricher"
    description: str = (
        "Downloads the client list from Hub and highlights each place a "
        "client's name or legal name appears, linking to that client in Hub."
    )
    author: str = "OpenContracts"
    dependencies: ClassVar[list[str]] = ["requests"]
    supported_file_types: ClassVar[list[FileTypeEnum]] = [FileTypeEnum.PDF]

    @dataclass
    class Settings:
        """Configuration schema for HubClientEnricher."""

        hub_token: str = field(
            default="",
            metadata={
                "pipeline_setting": PipelineSetting(
                    setting_type=SettingType.SECRET,
                    required=True,
                    description="Hub personal token (starts with 'hub_').",
                )
            },
        )

    def _enrich_document_impl(
        self,
        user_id: int,
        doc_id: int,
        export_data: OpenContractDocExport,
        **all_kwargs,
    ) -> OpenContractDocExport:
        from opencontractserver.annotations.models import TOKEN_LABEL

        token = all_kwargs.get("hub_token", "")
        if not token:
            logger.warning(
                "HubClientEnricher: no Hub token is configured; doc %s gets no "
                "Hub client highlights.",
                doc_id,
            )
            return export_data
        clients = []
        page_number = 1
        total_pages = 1
        while page_number <= total_pages:
            try:
                response = requests.get(
                    HUB_CLIENTS_API_URL,
                    params={"page": page_number, "per_page": HUB_CLIENTS_PER_PAGE},
                    headers={
                        "Accept": "application/vnd.hub+json; version=1",
                        "Content-Type": "application/json",
                        "Authorization": f"Token token={token}",
                    },
                    timeout=HUB_REQUEST_TIMEOUT_SECONDS,
                )
            except (
                requests.exceptions.ConnectionError,
                requests.exceptions.Timeout,
            ) as exc:
                logger.warning(
                    "HubClientEnricher: could not reach Hub: %s: %s; doc %s gets "
                    "no Hub client highlights.",
                    type(exc).__name__,
                    exc,
                    doc_id,
                )
                return export_data
            if response.status_code in (401, 403):
                logger.warning(
                    "HubClientEnricher: Hub rejected the token (HTTP %s); doc %s "
                    "gets no Hub client highlights.",
                    response.status_code,
                    doc_id,
                )
                return export_data
            if response.status_code != 200:
                logger.warning(
                    "HubClientEnricher: Hub answered HTTP %s; doc %s gets no Hub "
                    "client highlights.",
                    response.status_code,
                    doc_id,
                )
                return export_data
            body = response.json()
            clients.extend(body["clients"])
            total_pages = body["meta"]["total_pages"]
            page_number += 1

        # Longest names first, so "Acme Corporation" claims its words before
        # "Acme" can match the first of them.
        candidates = []
        for client in clients:
            for name in (client["legal_name"], client["name"]):
                words = [_match_key(word) for word in (name or "").split()]
                if any(words):
                    candidates.append((words, client))
        candidates.sort(key=lambda candidate: len(candidate[0]), reverse=True)
        # Index by first word so each token position only tries the names that
        # could start there, instead of every name at every position.
        candidates_by_first_word: dict[str, list[tuple[int, list[str], dict]]] = {}
        for rank, (words, client) in enumerate(candidates):
            candidates_by_first_word.setdefault(words[0], []).append(
                (rank, words, client)
            )

        new_entries: list[OpenContractsAnnotationPythonType] = []
        pawls_pages = export_data.get("pawls_file_content") or []
        for page_index, page in enumerate(pawls_pages):
            token_texts, original_indices = page_text_tokens(page)
            folded_texts = [_match_key(text) for text in token_texts]
            claimed: set[int] = set()
            matches = []
            for start, first_word in enumerate(folded_texts):
                for rank, words, client in candidates_by_first_word.get(first_word, ()):
                    if folded_texts[start : start + len(words)] == words:
                        matches.append((rank, start, words, client))
            # Claim in the same order as trying each candidate (longest
            # first) at every position left to right.
            matches.sort(key=lambda match: (match[0], match[1]))
            for _, start, words, client in matches:
                span = range(start, start + len(words))
                if claimed.intersection(span):
                    continue
                claimed.update(span)
                raw_text = " ".join(token_texts[start : start + len(words)])
                annotation_json: dict[
                    int | str, OpenContractsSinglePageAnnotationType
                ] = {
                    str(page_index): single_page_token_annotation(
                        page.get("tokens", []) or [],
                        page_index,
                        [original_indices[i] for i in span],
                        raw_text,
                    )
                }
                new_entries.append(
                    {
                        "id": f"enr_hub_client_{len(new_entries) + 1}",
                        "annotationLabel": HUB_CLIENT_LABEL,
                        "rawText": raw_text,
                        "page": page_index,
                        "annotation_json": annotation_json,
                        "parent_id": None,
                        "annotation_type": TOKEN_LABEL,
                        "structural": False,
                        "link_url": client["url"],
                        # Only these four of Hub's fields: anyone who can
                        # read the document can read this sidecar.
                        "data": {
                            "id": client["id"],
                            "name": client["name"],
                            "slug": client["slug"],
                            "url": client["url"],
                        },
                    }
                )

        export_data["labelled_text"] = (
            list(export_data.get("labelled_text") or []) + new_entries
        )
        return export_data
