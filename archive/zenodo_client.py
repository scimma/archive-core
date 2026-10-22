"""
Optional Zenodo (Sandbox or production) DOI-minting client.

This module is intentionally inert until a mint is actually attempted:
importing it, and running the archive service with no Zenodo configuration
present at all, must both continue to work normally - Zenodo configuration
is only required at the moment mint_doi_for_package() is actually called.

Configuration:
    --zenodo-base-url / ZENODO_BASE_URL   e.g. https://sandbox.zenodo.org/api
    --zenodo-community / ZENODO_COMMUNITY  (optional) community slug
    ZENODO_TOKEN (environment variable only, like HOPAUTH_PASSWORD/DB_PASSWORD -
                  deliberately not loadable from a TOML config file)
"""
import logging
import os

import httpx

from . import utility_api

logger = logging.getLogger(__name__)


class ZenodoError(Exception):
    """Raised for any failure talking to Zenodo, with a human-readable message."""


class ZenodoNotConfiguredError(ZenodoError):
    """Raised when a mint is attempted but no Zenodo configuration is present."""


def add_parser_options(parser):
    EnvDefault = utility_api.EnvDefault
    parser.add_argument("--zenodo-base-url", help="Base URL for the Zenodo deposit API",
                        type=str, action=EnvDefault, envvar="ZENODO_BASE_URL", required=False)
    parser.add_argument("--zenodo-community",
                        help="Optional Zenodo community slug to request newly minted "
                        "depositions join", type=str, action=EnvDefault,
                        envvar="ZENODO_COMMUNITY", required=False)


def is_configured(config) -> bool:
    """Return True if enough configuration is present to attempt a mint."""
    return bool(config.get("zenodo_base_url")) and bool(os.environ.get("ZENODO_TOKEN"))


def _require_configured(config):
    if not is_configured(config):
        raise ZenodoNotConfiguredError(
            "Zenodo is not configured on this archive instance. Set zenodo-base-url "
            "and the ZENODO_TOKEN environment variable to enable DOI minting."
        )


def _base_url(config) -> str:
    return config["zenodo_base_url"].rstrip("/")


def _auth_params() -> dict:
    return {"access_token": os.environ["ZENODO_TOKEN"]}


def _raise_for_status(response: httpx.Response, context: str):
    if response.status_code >= 400:
        detail = ""
        try:
            body = response.json()
            detail = body.get("message", "") or str(body)
        except ValueError:
            detail = response.text
        raise ZenodoError(f"Zenodo {context} failed (HTTP {response.status_code}): {detail}")


async def create_deposition(config, client: httpx.AsyncClient):
    """Step 1: Create an empty deposition draft. Returns the deposition dict."""
    response = await client.post(
        f"{_base_url(config)}/deposit/depositions",
        params=_auth_params(),
        json={},
    )
    _raise_for_status(response, "create deposition")
    return response.json()


async def set_metadata(config, client: httpx.AsyncClient, deposition_id, metadata):
    """
    Step 2: Attach metadata to a deposition.

    metadata: {
        'title': str,
        'description': str,
        'creators': [{'name': str, 'affiliation': str (optional)}, ...],
        'keywords': [str, ...] (optional),
    }
    """
    payload = {
        "metadata": {
            "title": metadata["title"],
            "upload_type": "dataset",
            "description": metadata["description"],
            "creators": metadata["creators"],
        }
    }
    if metadata.get("keywords"):
        payload["metadata"]["keywords"] = metadata["keywords"]

    community = config.get("zenodo_community")
    if community:
        # Best-effort only: on current Zenodo (InvenioRDM), community
        # membership is a separate post-publish request/review step, not a
        # metadata field. Harmless to include; may be honored on legacy
        # instances.
        payload["metadata"]["communities"] = [{"identifier": community}]

    response = await client.put(
        f"{_base_url(config)}/deposit/depositions/{deposition_id}",
        params=_auth_params(),
        json=payload,
    )
    _raise_for_status(response, "set metadata")
    return response.json()


async def upload_file(client: httpx.AsyncClient, bucket_url, filename, content):
    """
    Step 3: Upload a single file to the deposition's bucket.

    `content` may be raw bytes, or any object httpx accepts as a streaming
    request body (e.g. an async iterator of byte chunks) - callers should
    supply a stream rather than buffering a large file fully into memory
    where possible.

    Zenodo's bucket API requires the generic application/octet-stream
    Content-Type regardless of the file's actual type.
    """
    response = await client.put(
        f"{bucket_url}/{filename}",
        params=_auth_params(),
        headers={"Content-Type": "application/octet-stream"},
        content=content,
    )
    _raise_for_status(response, f"upload file {filename}")
    return response.json()


async def publish_deposition(config, client: httpx.AsyncClient, deposition_id):
    """
    Step 4: Publish the deposition. This mints the DOI and is irreversible
    (a new version must be created afterward; the record cannot be deleted).
    """
    response = await client.post(
        f"{_base_url(config)}/deposit/depositions/{deposition_id}/actions/publish",
        params=_auth_params(),
    )
    _raise_for_status(response, "publish deposition")
    return response.json()


async def mint_doi_for_package(config, metadata, files):
    """
    High-level orchestration used by Archive_access.mint_doi().

    files: list of (filename, content) pairs, where content is bytes or a
           streamable body accepted by httpx.

    Returns the published deposition dict (includes 'doi' and
    'links'['record_html']).
    """
    _require_configured(config)
    async with httpx.AsyncClient(timeout=60.0) as client:
        deposition = await create_deposition(config, client)
        await set_metadata(config, client, deposition["id"], metadata)

        for filename, content in files:
            await upload_file(client, deposition["links"]["bucket"], filename, content)

        return await publish_deposition(config, client, deposition["id"])
