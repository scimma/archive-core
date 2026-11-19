"""
Optional Zenodo (Sandbox or production) DOI-minting client.

This module is intentionally inert until a mint is actually attempted:
importing it, and running the archive service with no Zenodo configuration
present at all, must both continue to work normally - Zenodo configuration
is only required at the moment ZenodoClient.mint_doi_for_package() is
actually called.

Configuration:
    --zenodo-base-url / ZENODO_BASE_URL   e.g. https://sandbox.zenodo.org/api
    --zenodo-community / ZENODO_COMMUNITY  (optional) community slug

The API token is a secret, so it is deliberately not loadable from a TOML
config file. It is taken from either of:
    ZENODO_TOKEN (environment variable, like HOPAUTH_PASSWORD/DB_PASSWORD),
        which takes priority if set; or
    --zenodo-aws-secret-name / ZENODO_AWS_SECRET_NAME
    --zenodo-aws-secret-region / ZENODO_AWS_SECRET_REGION
        the name and region of an AWS Secrets Manager secret holding the
        token, either as plain text or as JSON with a "token" (or
        "ZENODO_TOKEN") key. It is read the first time it is needed, and
        the result is kept.
"""
import json
import logging
import os
from typing import Optional

import boto3
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
    parser.add_argument("--zenodo-aws-secret-name",
                        help="Name of an AWS secret from which to read the Zenodo API token",
                        type=str, action=EnvDefault, envvar="ZENODO_AWS_SECRET_NAME",
                        required=False)
    parser.add_argument("--zenodo-aws-secret-region",
                        help="Name of the AWS region in which to look for the AWS secret from "
                        "which to read the Zenodo API token", type=str, action=EnvDefault,
                        envvar="ZENODO_AWS_SECRET_REGION", default="us-west-2", required=False)


def token_from_secret(secret_region: str, secret_name: str) -> str:
    """
    Read a Zenodo API token from an AWS Secrets Manager secret.

    The secret may contain the token as plain text, or a JSON object with
    the token under the key "token" or "ZENODO_TOKEN".
    """
    session = boto3.session.Session()
    client = session.client(
        service_name='secretsmanager',
        region_name=secret_region
    )
    value = client.get_secret_value(SecretId=secret_name)['SecretString']
    try:
        data = json.loads(value)
    except json.JSONDecodeError:
        data = None
    if isinstance(data, dict):
        value = data.get("token") or data.get("ZENODO_TOKEN") or ""
    value = value.strip()
    if not value:
        raise ValueError(f"No Zenodo token found in secret {secret_name}")
    return value


class ZenodoClient:
    """
    Client for the Zenodo deposit API, configured once from the archive's
    configuration and then used to mint DOIs.

    Constructing a ZenodoClient never fails and never contacts Zenodo, even
    when no Zenodo configuration is present; use is_configured() to find out
    whether minting is possible.
    """

    def __init__(self, config, http_client: Optional[httpx.AsyncClient] = None):
        """
        config: the archive configuration; zenodo_base_url and (optionally)
                zenodo_community are read from it.
        http_client: optional httpx client to use for all requests, e.g. a
                     test double. If not supplied, a client is created for
                     the duration of each minting operation.
        """
        self.base_url = (config.get("zenodo_base_url") or "").rstrip("/")
        self.community = config.get("zenodo_community")
        self._token = os.environ.get("ZENODO_TOKEN")
        self._secret_name = config.get("zenodo_aws_secret_name")
        self._secret_region = config.get("zenodo_aws_secret_region") or "us-west-2"
        self._http_client = http_client

    def _get_token(self) -> Optional[str]:
        """
        Return the API token, or None if there is none. If it has to be read
        from an AWS secret, that happens here, on first need, so that an
        unreachable secret cannot prevent the archive from starting, and
        so it is tried again on later requests until it succeeds.
        """
        if not self._token and self._secret_name:
            try:
                self._token = token_from_secret(self._secret_region, self._secret_name)
            except Exception as err:
                logger.error(f"Unable to read the Zenodo token from AWS secret "
                             f"{self._secret_name}: {err}")
        return self._token

    def is_configured(self) -> bool:
        """Return True if enough configuration is present to attempt a mint."""
        return bool(self.base_url) and bool(self._get_token())

    def _require_configured(self):
        if not self.is_configured():
            raise ZenodoNotConfiguredError(
                "Zenodo is not configured on this archive instance. Set zenodo-base-url "
                "and either the ZENODO_TOKEN environment variable or zenodo-aws-secret-name "
                "to enable DOI minting."
            )

    @property
    def _auth_params(self) -> dict:
        return {"access_token": self._get_token()}

    @staticmethod
    def _raise_for_status(response: httpx.Response, context: str):
        if response.status_code >= 400:
            detail = ""
            try:
                body = response.json()
                detail = body.get("message", "") or str(body)
            except ValueError:
                detail = response.text
            raise ZenodoError(f"Zenodo {context} failed (HTTP {response.status_code}): {detail}")

    async def create_deposition(self, client: httpx.AsyncClient):
        """Step 1: Create an empty deposition draft. Returns the deposition dict."""
        response = await client.post(
            f"{self.base_url}/deposit/depositions",
            params=self._auth_params,
            json={},
        )
        self._raise_for_status(response, "create deposition")
        return response.json()

    async def set_metadata(self, client: httpx.AsyncClient, deposition_id, metadata):
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

        if self.community:
            # Best-effort only: on current Zenodo (InvenioRDM), community
            # membership is a separate post-publish request/review step, not a
            # metadata field. Harmless to include; may be honored on legacy
            # instances.
            payload["metadata"]["communities"] = [{"identifier": self.community}]

        response = await client.put(
            f"{self.base_url}/deposit/depositions/{deposition_id}",
            params=self._auth_params,
            json=payload,
        )
        self._raise_for_status(response, "set metadata")
        return response.json()

    async def upload_file(self, client: httpx.AsyncClient, bucket_url, filename, content):
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
            params=self._auth_params,
            headers={"Content-Type": "application/octet-stream"},
            content=content,
        )
        self._raise_for_status(response, f"upload file {filename}")
        return response.json()

    async def publish_deposition(self, client: httpx.AsyncClient, deposition_id):
        """
        Step 4: Publish the deposition. This mints the DOI and is irreversible
        (a new version must be created afterward; the record cannot be deleted).
        """
        response = await client.post(
            f"{self.base_url}/deposit/depositions/{deposition_id}/actions/publish",
            params=self._auth_params,
        )
        self._raise_for_status(response, "publish deposition")
        return response.json()

    async def mint_doi_for_package(self, metadata, files):
        """
        High-level orchestration used by Archive_access.mint_doi().

        files: list of (filename, content) pairs, where content is bytes or a
               streamable body accepted by httpx.

        Returns the published deposition dict (includes 'doi' and
        'links'['record_html']).
        """
        self._require_configured()
        if self._http_client is not None:
            return await self._mint(self._http_client, metadata, files)
        async with httpx.AsyncClient(timeout=60.0) as client:
            return await self._mint(client, metadata, files)

    async def _mint(self, client: httpx.AsyncClient, metadata, files):
        deposition = await self.create_deposition(client)
        await self.set_metadata(client, deposition["id"], metadata)

        for filename, content in files:
            await self.upload_file(client, deposition["links"]["bucket"], filename, content)

        return await self.publish_deposition(client, deposition["id"])
