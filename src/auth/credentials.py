from __future__ import annotations

import os
import time
from dataclasses import dataclass
from typing import Any

import structlog
from py_clob_client.client import ClobClient
from py_clob_client.headers.headers import create_level_1_headers
from py_clob_client.http_helpers.helpers import get, post
from py_clob_client.signer import Signer

logger = structlog.get_logger(__name__)

DEFAULT_CLOB_HOST = "https://clob.polymarket.com"
_CREATE_API_KEY_PATH = "/auth/api-key"
_DERIVE_API_KEY_PATH = "/auth/derive-api-key"
_ALLOWED_SIGNATURE_TYPES = {0, 1, 2}


class CredentialValidationError(ValueError):
    pass


@dataclass(slots=True)
class ManagedApiCreds:
    api_key: str
    api_secret: str
    api_passphrase: str
    nonce: int
    created_at: float
    last_derived_at: float

    def age_seconds(self, now: float | None = None) -> float:
        now_ts = now if now is not None else time.time()
        return max(0.0, float(now_ts) - float(self.created_at))

    def as_env(self) -> dict[str, str]:
        return {
            "API_KEY": self.api_key,
            "API_SECRET": self.api_secret,
            "API_PASSPHRASE": self.api_passphrase,
            "API_CREDS_NONCE": str(self.nonce),
            "API_CREDS_CREATED_AT": str(self.created_at),
            "API_CREDS_LAST_DERIVED_AT": str(self.last_derived_at),
        }



def _build_managed_creds(payload: dict[str, Any], nonce: int, ts: float | None = None) -> ManagedApiCreds:
    timestamp = ts if ts is not None else time.time()
    return ManagedApiCreds(
        api_key=str(payload["apiKey"]),
        api_secret=str(payload["secret"]),
        api_passphrase=str(payload["passphrase"]),
        nonce=int(nonce),
        created_at=float(timestamp),
        last_derived_at=float(timestamp),
    )


def create_api_key(private_key: str, chain_id: int, *, host: str = DEFAULT_CLOB_HOST, nonce: int = 0) -> ManagedApiCreds:
    signer = Signer(private_key, chain_id)
    endpoint = f"{host}{_CREATE_API_KEY_PATH}"
    headers = create_level_1_headers(signer, nonce)
    payload = post(endpoint, headers=headers)
    return _build_managed_creds(payload, nonce)


def derive_api_key(private_key: str, chain_id: int, nonce: int, *, host: str = DEFAULT_CLOB_HOST) -> ManagedApiCreds:
    signer = Signer(private_key, chain_id)
    endpoint = f"{host}{_DERIVE_API_KEY_PATH}"
    headers = create_level_1_headers(signer, nonce)
    payload = get(endpoint, headers=headers)
    return _build_managed_creds(payload, nonce)


def rotate_api_credentials(
    private_key: str,
    chain_id: int,
    *,
    host: str = DEFAULT_CLOB_HOST,
    current_creds: ManagedApiCreds | None,
    rotation_seconds: int,
    nonce: int,
) -> ManagedApiCreds:
    if current_creds is not None and current_creds.age_seconds() < rotation_seconds:
        return current_creds

    if current_creds is not None:
        return derive_api_key(private_key, chain_id, nonce, host=host)
    return create_api_key(private_key, chain_id, host=host, nonce=nonce)


def persist_creds_to_env(creds: ManagedApiCreds) -> None:
    for key, value in creds.as_env().items():
        os.environ[key] = value


def init_client(
    *,
    signer: str,
    derived_creds: Any,
    signature_type: int | None,
    funder_address: str,
    host: str,
    chain_id: int,
    clob_client_cls: type[ClobClient] = ClobClient,
):
    if signature_type is None or int(signature_type) not in _ALLOWED_SIGNATURE_TYPES:
        raise CredentialValidationError("SIGNATURE_TYPE must be one of {0, 1, 2}.")

    funder = funder_address.strip()
    if not funder:
        raise CredentialValidationError("FUNDER_ADDRESS is required for live trading.")

    client = clob_client_cls(
        host=host,
        chain_id=chain_id,
        key=signer,
        creds=derived_creds,
        signature_type=int(signature_type),
        funder=funder,
    )
    if derived_creds is not None and hasattr(client, "set_api_creds"):
        client.set_api_creds(derived_creds)
    return client
