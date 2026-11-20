"""Tests for HubClientEnricher.

The end-to-end tests upload a PDF through the ``uploadDocument`` mutation and
let the real ingest chain run under eager Celery. The PDF parser is a hermetic
in-test parser (referenced by dotted path from ``PipelineSettings``) that
returns fixed PAWLs words, and Hub's HTTP responses are stubbed, so no parser
service or network call is involved.
"""

from typing import ClassVar, Optional, cast
from unittest.mock import patch

import pytest
import requests
from django.contrib.auth import get_user_model
from django.test import TestCase, TransactionTestCase

from config.graphql.schema import schema
from config.graphql.testing import Client
from opencontractserver.corpuses.models import Corpus
from opencontractserver.documents.models import Document, PipelineSettings
from opencontractserver.pipeline.base.parser import BaseParser
from opencontractserver.pipeline.enrichers.hub_client_enricher import (
    HubClientEnricher,
)
from opencontractserver.tests.fixtures import SAMPLE_PDF_FILE_ONE_PATH
from opencontractserver.types.dicts import OpenContractDocExport
from opencontractserver.types.enums import PermissionTypes
from opencontractserver.utils.files import base_64_encode_bytes
from opencontractserver.utils.ids import from_global_id, to_global_id
from opencontractserver.utils.permissioning import set_permissions_for_obj_to_user

User = get_user_model()

HUB_ENRICHER_PATH = (
    "opencontractserver.pipeline.enrichers.hub_client_enricher.HubClientEnricher"
)
PDF_WORDS = ["This", "agreement", "is", "with", "Acme", "Corporation", "today."]


def _pawls_page(words: list[str]) -> dict:
    """One PAWLs page whose tokens are ``words``, left to right on one line."""
    tokens = []
    x = 72.0
    for word in words:
        width = 6.0 * len(word)
        tokens.append(
            {"x": x, "y": 700.0, "width": width, "height": 12.0, "text": word}
        )
        x += width + 4.0
    return {"page": {"width": 612.0, "height": 792.0, "index": 0}, "tokens": tokens}


class _WordsPdfParser(BaseParser):
    """Deterministic PDF parser: one page whose tokens are ``PDF_WORDS``."""

    title = "Hub Enricher Test PDF Parser"
    description = "Returns fixed PAWLs words for the Hub client enricher tests."
    author = "Integration Test"
    dependencies: ClassVar[list[str]] = []

    def _parse_document_impl(
        self, user_id: int, doc_id: int, **kwargs
    ) -> Optional[OpenContractDocExport]:
        return cast(
            OpenContractDocExport,
            {
                "title": "Hub Enricher PDF",
                "content": "",
                "description": "",
                "pawls_file_content": [_pawls_page(PDF_WORDS)],
                "page_count": 1,
                "doc_labels": [],
                "labelled_text": [],
            },
        )


_WORDS_PDF_PARSER_PATH = f"{__name__}._WordsPdfParser"


class _FakeResponse:
    def __init__(self, status_code: int = 200, payload: dict | None = None):
        self.status_code = status_code
        self._payload = payload if payload is not None else {}
        self.text = str(self._payload)

    def json(self):
        return self._payload


class _TestContext:
    def __init__(self, user):
        self.user = user


UPLOAD_MUTATION = """
    mutation Upload($file: String!, $corpusId: ID) {
        uploadDocument(
            base64FileString: $file,
            addToCorpusId: $corpusId,
            filename: "contract.pdf",
            title: "Contract",
            description: "A contract",
            customMeta: {},
            makePublic: false
        ) {
            ok
            message
            document { id }
        }
    }
"""

DOCUMENT_QUERY = """
    query Doc($id: ID!, $corpusId: ID) {
        document(id: $id) {
            id
            processingStatus
            backendLock
            allAnnotations(corpusId: $corpusId) {
                id
                rawText
                linkUrl
                data
                annotationLabel { text }
            }
        }
    }
"""

REMOVE_ANNOTATION_MUTATION = """
    mutation Remove($id: String!) {
        removeAnnotation(annotationId: $id) {
            ok
            message
        }
    }
"""

SAVE_SECRET_MUTATION = """
    mutation Secret($path: String!, $secrets: GenericScalar!) {
        updateComponentSecrets(componentPath: $path, secrets: $secrets) {
            ok
            message
        }
    }
"""

ASSIGN_ENRICHER_MUTATION = """
    mutation Assign($enrichers: GenericScalar) {
        updatePipelineSettings(preferredEnrichers: $enrichers) {
            ok
            message
        }
    }
"""


@pytest.mark.usefixtures("enable_doc_processing_signals")
class HubClientEnricherEndToEndTests(TransactionTestCase):
    """Upload a PDF and read the Hub client highlights back through GraphQL."""

    def _use_words_pdf_parser(self) -> None:
        pipeline_settings = PipelineSettings.get_instance(use_cache=False)
        pipeline_settings.preferred_parsers = {
            **(pipeline_settings.preferred_parsers or {}),
            "application/pdf": _WORDS_PDF_PARSER_PATH,
        }
        pipeline_settings.save()
        PipelineSettings.clear_cache()
        self.addCleanup(PipelineSettings.clear_cache)

    def _admin_configures_hub_enricher(self, token: Optional[str]) -> None:
        admin = User.objects.create_superuser(
            username="hub_admin", password="pw", email="hub_admin@example.com"
        )
        admin_client = Client(schema, context_value=_TestContext(admin))
        if token is not None:
            saved = admin_client.execute(
                SAVE_SECRET_MUTATION,
                variables={"path": HUB_ENRICHER_PATH, "secrets": {"hub_token": token}},
            )
            self.assertTrue(saved["data"]["updateComponentSecrets"]["ok"], saved)
        assigned = admin_client.execute(
            ASSIGN_ENRICHER_MUTATION,
            variables={"enrichers": {"application/pdf": [HUB_ENRICHER_PATH]}},
        )
        self.assertTrue(assigned["data"]["updatePipelineSettings"]["ok"], assigned)
        PipelineSettings.clear_cache()

    @staticmethod
    def _corpus_owned_by(user) -> Corpus:
        corpus = Corpus.objects.create(title="Contracts", creator=user)
        set_permissions_for_obj_to_user(user, corpus, [PermissionTypes.ALL])
        return corpus

    def _upload_and_read(self, client: Client, corpus: Corpus) -> dict:
        corpus_id = to_global_id("CorpusType", corpus.id)
        uploaded = client.execute(
            UPLOAD_MUTATION,
            variables={
                "file": base_64_encode_bytes(SAMPLE_PDF_FILE_ONE_PATH.read_bytes()),
                "corpusId": corpus_id,
            },
        )
        self.assertTrue(uploaded["data"]["uploadDocument"]["ok"], uploaded)
        doc_id = uploaded["data"]["uploadDocument"]["document"]["id"]
        result = client.execute(
            DOCUMENT_QUERY, variables={"id": doc_id, "corpusId": corpus_id}
        )
        self.assertIsNone(result.get("errors"), result)
        return result["data"]["document"]

    @staticmethod
    def _hub_highlights(document: dict) -> list[dict]:
        return [
            a
            for a in document["allAnnotations"]
            if a["annotationLabel"]["text"] == "HUB_CLIENT"
        ]

    def test_uploaded_pdf_highlights_client_legal_name_linking_to_hub(self):
        self._use_words_pdf_parser()
        self._admin_configures_hub_enricher(token="hub_personal_token")
        uploader = User.objects.create_user(username="uploader", password="pw")
        client = Client(schema, context_value=_TestContext(uploader))
        corpus = self._corpus_owned_by(uploader)
        hub_clients = _FakeResponse(
            payload={
                "clients": [
                    {
                        "id": 3,
                        "name": "Acme",
                        "legal_name": "Acme Corporation",
                        "slug": "acme",
                        "url": "https://hub.thoughtbot.com/clients/acme",
                    }
                ],
                "meta": {
                    "page": 1,
                    "per_page": 100,
                    "total_pages": 1,
                    "total_count": 1,
                },
            }
        )

        with patch("requests.get", return_value=hub_clients) as hub_get:
            document = self._upload_and_read(client, corpus)

        self.assertEqual(
            hub_get.call_args.kwargs["headers"]["Authorization"],
            "Token token=hub_personal_token",
        )
        self.assertEqual(document["processingStatus"], "COMPLETED")
        self.assertFalse(document["backendLock"])
        highlights = self._hub_highlights(document)
        self.assertEqual(
            [(h["rawText"], h["linkUrl"]) for h in highlights],
            [("Acme Corporation", "https://hub.thoughtbot.com/clients/acme")],
        )

    def test_editor_can_delete_a_hub_client_highlight(self):
        self._use_words_pdf_parser()
        self._admin_configures_hub_enricher(token="hub_personal_token")
        uploader = User.objects.create_user(username="uploader", password="pw")
        client = Client(schema, context_value=_TestContext(uploader))
        corpus = self._corpus_owned_by(uploader)
        hub_clients = _FakeResponse(
            payload={
                "clients": [
                    {
                        "id": 3,
                        "name": "Acme",
                        "legal_name": "Acme Corporation",
                        "slug": "acme",
                        "url": "https://hub.thoughtbot.com/clients/acme",
                    }
                ],
                "meta": {
                    "page": 1,
                    "per_page": 100,
                    "total_pages": 1,
                    "total_count": 1,
                },
            }
        )
        with patch("requests.get", return_value=hub_clients):
            document = self._upload_and_read(client, corpus)
        [highlight] = self._hub_highlights(document)

        removed = client.execute(
            REMOVE_ANNOTATION_MUTATION, variables={"id": highlight["id"]}
        )
        reread = client.execute(
            DOCUMENT_QUERY,
            variables={
                "id": document["id"],
                "corpusId": to_global_id("CorpusType", corpus.id),
            },
        )

        self.assertTrue(removed["data"]["removeAnnotation"]["ok"], removed)
        self.assertEqual(self._hub_highlights(reread["data"]["document"]), [])

    def test_viewer_reads_only_id_name_slug_and_url_of_the_hub_client(self):
        self._use_words_pdf_parser()
        self._admin_configures_hub_enricher(token="hub_personal_token")
        uploader = User.objects.create_user(username="uploader", password="pw")
        viewer = User.objects.create_user(username="viewer", password="pw")
        corpus = self._corpus_owned_by(uploader)
        hub_clients = _FakeResponse(
            payload={
                "clients": [
                    {
                        "id": 3,
                        "name": "Acme",
                        "legal_name": "Acme Corporation",
                        "slug": "acme",
                        "status": "Current",
                        "industry": "Finance",
                        "website": "https://acme.example.com/",
                        "location": "Boston, MA",
                        "description": "Makes everything",
                        "account_manager": {
                            "id": 7,
                            "name": "Ada Lovelace",
                            "slug": "ada-lovelace",
                        },
                        "url": "https://hub.thoughtbot.com/clients/acme",
                    }
                ],
                "meta": {
                    "page": 1,
                    "per_page": 100,
                    "total_pages": 1,
                    "total_count": 1,
                },
            }
        )
        with patch("requests.get", return_value=hub_clients):
            uploaded = self._upload_and_read(
                Client(schema, context_value=_TestContext(uploader)), corpus
            )
        document = Document.objects.get(pk=from_global_id(uploaded["id"])[1])
        set_permissions_for_obj_to_user(viewer, corpus, [PermissionTypes.READ])
        set_permissions_for_obj_to_user(viewer, document, [PermissionTypes.READ])

        read = Client(schema, context_value=_TestContext(viewer)).execute(
            DOCUMENT_QUERY,
            variables={
                "id": uploaded["id"],
                "corpusId": to_global_id("CorpusType", corpus.id),
            },
        )

        self.assertEqual(
            [h["data"] for h in self._hub_highlights(read["data"]["document"])],
            [
                {
                    "id": 3,
                    "name": "Acme",
                    "slug": "acme",
                    "url": "https://hub.thoughtbot.com/clients/acme",
                }
            ],
        )

    def test_missing_token_completes_without_highlights_and_logs_why(self):
        self._use_words_pdf_parser()
        self._admin_configures_hub_enricher(token=None)
        uploader = User.objects.create_user(username="uploader", password="pw")
        client = Client(schema, context_value=_TestContext(uploader))
        corpus = self._corpus_owned_by(uploader)

        with patch("requests.get") as hub_get, self.assertLogs(level="INFO") as logs:
            document = self._upload_and_read(client, corpus)

        self.assertEqual(document["processingStatus"], "COMPLETED")
        self.assertFalse(document["backendLock"])
        self.assertEqual(self._hub_highlights(document), [])
        hub_get.assert_not_called()
        self.assertIn(
            "HubClientEnricher: no Hub token is configured", "\n".join(logs.output)
        )

    def test_token_rejected_with_401_completes_without_highlights_and_logs_why(self):
        self._use_words_pdf_parser()
        self._admin_configures_hub_enricher(token="hub_expired_token")
        uploader = User.objects.create_user(username="uploader", password="pw")
        client = Client(schema, context_value=_TestContext(uploader))
        corpus = self._corpus_owned_by(uploader)
        rejected = _FakeResponse(
            status_code=401,
            payload={"error": {"id": "unauthorized", "message": "Bad token"}},
        )

        with patch("requests.get", return_value=rejected), self.assertLogs(
            level="INFO"
        ) as logs:
            document = self._upload_and_read(client, corpus)

        self.assertEqual(document["processingStatus"], "COMPLETED")
        self.assertFalse(document["backendLock"])
        self.assertEqual(self._hub_highlights(document), [])
        output = "\n".join(logs.output)
        self.assertIn("HubClientEnricher: Hub rejected the token (HTTP 401)", output)
        self.assertNotIn("hub_expired_token", output)

    def test_token_rejected_with_403_completes_without_highlights_and_logs_why(self):
        self._use_words_pdf_parser()
        self._admin_configures_hub_enricher(token="hub_shared_token")
        uploader = User.objects.create_user(username="uploader", password="pw")
        client = Client(schema, context_value=_TestContext(uploader))
        corpus = self._corpus_owned_by(uploader)
        forbidden = _FakeResponse(
            status_code=403,
            payload={"error": {"id": "forbidden", "message": "Not a person"}},
        )

        with patch("requests.get", return_value=forbidden), self.assertLogs(
            level="INFO"
        ) as logs:
            document = self._upload_and_read(client, corpus)

        self.assertEqual(document["processingStatus"], "COMPLETED")
        self.assertFalse(document["backendLock"])
        self.assertEqual(self._hub_highlights(document), [])
        output = "\n".join(logs.output)
        self.assertIn("HubClientEnricher: Hub rejected the token (HTTP 403)", output)
        self.assertNotIn("hub_shared_token", output)

    def test_hub_unreachable_completes_without_highlights_and_logs_why(self):
        self._use_words_pdf_parser()
        self._admin_configures_hub_enricher(token="hub_personal_token")
        uploader = User.objects.create_user(username="uploader", password="pw")
        client = Client(schema, context_value=_TestContext(uploader))
        corpus = self._corpus_owned_by(uploader)

        with patch(
            "requests.get",
            side_effect=requests.exceptions.ConnectionError("Name does not resolve"),
        ), self.assertLogs(level="INFO") as logs:
            document = self._upload_and_read(client, corpus)

        self.assertEqual(document["processingStatus"], "COMPLETED")
        self.assertFalse(document["backendLock"])
        self.assertEqual(self._hub_highlights(document), [])
        output = "\n".join(logs.output)
        self.assertIn(
            "HubClientEnricher: could not reach Hub: ConnectionError: "
            "Name does not resolve",
            output,
        )
        self.assertNotIn("hub_personal_token", output)

    def test_hub_timeout_completes_without_highlights_and_logs_why(self):
        self._use_words_pdf_parser()
        self._admin_configures_hub_enricher(token="hub_personal_token")
        uploader = User.objects.create_user(username="uploader", password="pw")
        client = Client(schema, context_value=_TestContext(uploader))
        corpus = self._corpus_owned_by(uploader)

        with patch(
            "requests.get",
            side_effect=requests.exceptions.ReadTimeout("Read timed out"),
        ), self.assertLogs(level="INFO") as logs:
            document = self._upload_and_read(client, corpus)

        self.assertEqual(document["processingStatus"], "COMPLETED")
        self.assertFalse(document["backendLock"])
        self.assertEqual(self._hub_highlights(document), [])
        output = "\n".join(logs.output)
        self.assertIn(
            "HubClientEnricher: could not reach Hub: ReadTimeout: Read timed out",
            output,
        )
        self.assertNotIn("hub_personal_token", output)

    def test_hub_server_error_completes_without_highlights_and_logs_why(self):
        self._use_words_pdf_parser()
        self._admin_configures_hub_enricher(token="hub_personal_token")
        uploader = User.objects.create_user(username="uploader", password="pw")
        client = Client(schema, context_value=_TestContext(uploader))
        corpus = self._corpus_owned_by(uploader)
        unavailable = _FakeResponse(status_code=503, payload={"error": "down"})

        with patch("requests.get", return_value=unavailable), self.assertLogs(
            level="INFO"
        ) as logs:
            document = self._upload_and_read(client, corpus)

        self.assertEqual(document["processingStatus"], "COMPLETED")
        self.assertFalse(document["backendLock"])
        self.assertEqual(self._hub_highlights(document), [])
        output = "\n".join(logs.output)
        self.assertIn("HubClientEnricher: Hub answered HTTP 503", output)
        self.assertNotIn("hub_personal_token", output)


class HubClientEnricherTests(TestCase):
    """Behaviour of ``enrich_document`` over a hand-built export, Hub stubbed."""

    def test_client_only_on_the_second_page_of_hub_is_highlighted(self):
        export = {
            "pawls_file_content": [_pawls_page(["Signed", "by", "Initech", "today"])],
            "labelled_text": [],
        }
        first_page = _FakeResponse(
            payload={
                "clients": [
                    {
                        "id": 1,
                        "name": "Globex",
                        "legal_name": "Globex Corporation",
                        "slug": "globex",
                        "url": "https://hub.thoughtbot.com/clients/globex",
                    }
                ],
                "meta": {
                    "page": 1,
                    "per_page": 100,
                    "total_pages": 2,
                    "total_count": 130,
                },
            }
        )
        second_page = _FakeResponse(
            payload={
                "clients": [
                    {
                        "id": 2,
                        "name": "Initech",
                        "legal_name": "Initech LLC",
                        "slug": "initech",
                        "url": "https://hub.thoughtbot.com/clients/initech",
                    }
                ],
                "meta": {
                    "page": 2,
                    "per_page": 100,
                    "total_pages": 2,
                    "total_count": 130,
                },
            }
        )

        with patch("requests.get", side_effect=[first_page, second_page]) as hub_get:
            result = HubClientEnricher().enrich_document(
                1, 1, cast(OpenContractDocExport, export), hub_token="hub_token"
            )

        self.assertEqual(
            [(a["rawText"], a["link_url"]) for a in result["labelled_text"]],
            [("Initech", "https://hub.thoughtbot.com/clients/initech")],
        )
        self.assertEqual(
            [call.kwargs["params"]["page"] for call in hub_get.call_args_list],
            [1, 2],
        )

    def test_match_ignores_case_but_needs_the_whole_word(self):
        export = {
            "pawls_file_content": [
                _pawls_page(["ACME", "will", "open", "an", "office", "in", "Acmeville"])
            ],
            "labelled_text": [],
        }
        hub_clients = _FakeResponse(
            payload={
                "clients": [
                    {
                        "id": 3,
                        "name": "Acme",
                        "legal_name": "Acme Corporation",
                        "slug": "acme",
                        "url": "https://hub.thoughtbot.com/clients/acme",
                    }
                ],
                "meta": {
                    "page": 1,
                    "per_page": 100,
                    "total_pages": 1,
                    "total_count": 1,
                },
            }
        )

        with patch("requests.get", return_value=hub_clients):
            result = HubClientEnricher().enrich_document(
                1, 1, cast(OpenContractDocExport, export), hub_token="hub_token"
            )

        self.assertEqual(
            [(a["rawText"], a["link_url"]) for a in result["labelled_text"]],
            [("ACME", "https://hub.thoughtbot.com/clients/acme")],
        )

    def test_client_without_a_legal_name_does_not_stop_other_highlights(self):
        export = {
            "pawls_file_content": [_pawls_page(["Signed", "by", "Initech", "today"])],
            "labelled_text": [],
        }
        hub_clients = _FakeResponse(
            payload={
                "clients": [
                    {
                        "id": 1,
                        "name": "Globex",
                        "legal_name": None,
                        "slug": "globex",
                        "url": "https://hub.thoughtbot.com/clients/globex",
                    },
                    {
                        "id": 2,
                        "name": "Hooli",
                        "legal_name": "  ",
                        "slug": "hooli",
                        "url": "https://hub.thoughtbot.com/clients/hooli",
                    },
                    {
                        "id": 3,
                        "name": "Initech",
                        "legal_name": "Initech LLC",
                        "slug": "initech",
                        "url": "https://hub.thoughtbot.com/clients/initech",
                    },
                ],
                "meta": {
                    "page": 1,
                    "per_page": 100,
                    "total_pages": 1,
                    "total_count": 3,
                },
            }
        )

        with patch("requests.get", return_value=hub_clients):
            result = HubClientEnricher().enrich_document(
                1, 1, cast(OpenContractDocExport, export), hub_token="hub_token"
            )

        self.assertEqual(
            [(a["rawText"], a["link_url"]) for a in result["labelled_text"]],
            [("Initech", "https://hub.thoughtbot.com/clients/initech")],
        )

    def test_match_ignores_punctuation_around_the_words(self):
        export = {
            "pawls_file_content": [
                _pawls_page(
                    [
                        "between",
                        "Acme",
                        "Corporation,",
                        "a",
                        "Delaware",
                        "company",
                        '("Acme")',
                    ]
                )
            ],
            "labelled_text": [],
        }
        hub_clients = _FakeResponse(
            payload={
                "clients": [
                    {
                        "id": 3,
                        "name": "Acme",
                        "legal_name": "Acme Corporation",
                        "slug": "acme",
                        "url": "https://hub.thoughtbot.com/clients/acme",
                    }
                ],
                "meta": {
                    "page": 1,
                    "per_page": 100,
                    "total_pages": 1,
                    "total_count": 1,
                },
            }
        )

        with patch("requests.get", return_value=hub_clients):
            result = HubClientEnricher().enrich_document(
                1, 1, cast(OpenContractDocExport, export), hub_token="hub_token"
            )

        self.assertEqual(
            [a["rawText"] for a in result["labelled_text"]],
            ["Acme Corporation,", '("Acme")'],
        )
