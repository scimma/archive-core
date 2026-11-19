import json
from unittest import mock

import httpx
import pytest

from archive import zenodo_client
from archive.zenodo_client import ZenodoClient, ZenodoError, ZenodoNotConfiguredError

pytest_plugins = ('pytest_asyncio',)

CONFIG = {"zenodo_base_url": "https://zenodo.example/api/", "zenodo_community": "scimma"}
METADATA = {"title": "T", "description": "D", "creators": [{"name": "A"}]}


@pytest.fixture
def token(monkeypatch):
    monkeypatch.setenv("ZENODO_TOKEN", "secret")


def fake_zenodo(requests_seen, fail_publish=False):
    def handler(request: httpx.Request) -> httpx.Response:
        requests_seen.append(request)
        path = request.url.path
        if path.endswith("/deposit/depositions") and request.method == "POST":
            return httpx.Response(201, json={"id": 7, "links": {"bucket": "https://zenodo.example/api/files/b"}})
        if path.endswith("/deposit/depositions/7") and request.method == "PUT":
            return httpx.Response(200, json={})
        if "/files/b/" in path and request.method == "PUT":
            return httpx.Response(201, json={})
        if path.endswith("/actions/publish"):
            if fail_publish:
                return httpx.Response(400, json={"message": "nope"})
            return httpx.Response(202, json={"doi": "10.5072/zenodo.7", "links": {"record_html": "https://z/7"}})
        return httpx.Response(404)
    return httpx.AsyncClient(transport=httpx.MockTransport(handler))


def test_not_configured_without_token(monkeypatch):
    monkeypatch.delenv("ZENODO_TOKEN", raising=False)
    assert not ZenodoClient(CONFIG).is_configured()


def test_not_configured_without_base_url(token):
    assert not ZenodoClient({}).is_configured()


def test_configured(token):
    assert ZenodoClient(CONFIG).is_configured()


@pytest.mark.asyncio
async def test_mint_raises_when_not_configured(monkeypatch):
    monkeypatch.delenv("ZENODO_TOKEN", raising=False)
    with pytest.raises(ZenodoNotConfiguredError):
        await ZenodoClient(CONFIG).mint_doi_for_package(METADATA, [])


@pytest.mark.asyncio
async def test_mint_sequence_and_streaming_upload(token):
    seen = []

    async def stream():
        yield b"abc"
        yield b"def"

    async with fake_zenodo(seen) as http:
        result = await ZenodoClient(CONFIG, http).mint_doi_for_package(
            METADATA, [("a.json", b"{}"), ("b.bin", stream())])
    assert result["doi"] == "10.5072/zenodo.7"
    assert [(r.method, r.url.path.split("/api")[-1]) for r in seen] == [
        ("POST", "/deposit/depositions"),
        ("PUT", "/deposit/depositions/7"),
        ("PUT", "/files/b/a.json"),
        ("PUT", "/files/b/b.bin"),
        ("POST", "/deposit/depositions/7/actions/publish"),
    ]
    assert all(r.url.params["access_token"] == "secret" for r in seen)
    assert seen[3].content == b"abcdef"
    assert seen[3].headers["content-type"] == "application/octet-stream"


@pytest.mark.asyncio
async def test_zenodo_error_is_reported(token):
    async with fake_zenodo([], fail_publish=True) as http:
        with pytest.raises(ZenodoError, match="publish deposition failed.*nope"):
            await ZenodoClient(CONFIG, http).mint_doi_for_package(METADATA, [("a.json", b"{}")])


def fake_secrets_manager(secret_string=None, error=None):
    """Patch boto3 so that reading the secret returns secret_string (or raises error)."""
    session = mock.MagicMock()
    client = mock.MagicMock()
    if error is not None:
        client.get_secret_value = mock.MagicMock(side_effect=error)
    else:
        client.get_secret_value = mock.MagicMock(return_value={"SecretString": secret_string})
    session.client = mock.MagicMock(return_value=client)
    return mock.patch("boto3.session.Session", mock.MagicMock(return_value=session)), session, client


SECRET_CONFIG = {**CONFIG, "zenodo_aws_secret_name": "zenodo-token", "zenodo_aws_secret_region": "ac-pole-1"}


@pytest.fixture
def no_token(monkeypatch):
    monkeypatch.delenv("ZENODO_TOKEN", raising=False)


def test_token_from_plain_text_secret(no_token):
    patcher, session, client = fake_secrets_manager("  abc123\n")
    with patcher:
        assert ZenodoClient(SECRET_CONFIG).is_configured()
    session.client.assert_called_with(service_name="secretsmanager", region_name="ac-pole-1")
    client.get_secret_value.assert_called_with(SecretId="zenodo-token")


@pytest.mark.parametrize("key", ["token", "ZENODO_TOKEN"])
def test_token_from_json_secret(no_token, key):
    patcher, _, _ = fake_secrets_manager(json.dumps({key: "abc123"}))
    with patcher:
        assert zenodo_client.token_from_secret("ac-pole-1", "zenodo-token") == "abc123"


def test_json_secret_without_token_is_an_error(no_token):
    patcher, _, _ = fake_secrets_manager(json.dumps({"other": "x"}))
    with patcher, pytest.raises(ValueError):
        zenodo_client.token_from_secret("ac-pole-1", "zenodo-token")


def test_environment_token_takes_priority_over_secret(monkeypatch):
    monkeypatch.setenv("ZENODO_TOKEN", "from-env")
    patcher, session, _ = fake_secrets_manager("from-secret")
    with patcher:
        z = ZenodoClient(SECRET_CONFIG)
        assert z.is_configured()
    session.client.assert_not_called()
    assert z._get_token() == "from-env"


def test_constructing_never_reads_the_secret(no_token):
    patcher, session, _ = fake_secrets_manager("abc123")
    with patcher:
        ZenodoClient(SECRET_CONFIG)
    session.client.assert_not_called()


def test_secret_is_read_once_and_kept(no_token):
    expected_secret = "abc123"
    patcher, _, client = fake_secrets_manager(expected_secret)
    with patcher:
        zenodo = ZenodoClient(SECRET_CONFIG)
        assert zenodo.is_configured()
    assert client.get_secret_value.call_count == 1
    assert zenodo._get_token() == expected_secret


def test_unreadable_secret_means_not_configured_and_is_retried(no_token, caplog):
    patcher, _, client = fake_secrets_manager(error=RuntimeError("access denied"))
    with patcher:
        zenodo = ZenodoClient(SECRET_CONFIG)
        assert not zenodo.is_configured()
        assert "access denied" in caplog.text
        client.get_secret_value.side_effect = None
        expected_secret = "abc123"
        client.get_secret_value.return_value = {"SecretString": expected_secret}
        assert zenodo.is_configured()
        assert zenodo._get_token() == expected_secret


def test_no_aws_call_without_base_url(no_token):
    patcher, session, _ = fake_secrets_manager("abc123")
    with patcher:
        assert not ZenodoClient({"zenodo_aws_secret_name": "zenodo-token"}).is_configured()
    session.client.assert_not_called()


@pytest.mark.asyncio
async def test_mint_uses_the_token_from_the_secret(no_token):
    seen = []
    patcher, _, _ = fake_secrets_manager("from-secret")
    with patcher:
        async with fake_zenodo(seen) as http:
            await ZenodoClient(SECRET_CONFIG, http).mint_doi_for_package(METADATA, [("a.json", b"{}")])
    assert seen and all(r.url.params["access_token"] == "from-secret" for r in seen)
