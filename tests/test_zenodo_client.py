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
