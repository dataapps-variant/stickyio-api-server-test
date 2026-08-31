"""
Sticky.io auth helper.
Resolves Sticky.io basic-auth credentials in a backward-compatible way.

Preferred (new/safe): caller passes ?entity=CN → looked up from Secret Manager.
Legacy (old/insecure): caller passes ?cred=<base64 user:pass> → decoded directly.

Once all callers migrate to entity=, the cred= path can be removed.
"""
import base64
import logging
from functools import lru_cache
from typing import Tuple

logger = logging.getLogger(__name__)

# Map of entity codes → Secret Manager secret name.
# Add/remove entries as new entities are onboarded.
_ENTITY_SECRETS = {
    "CN": "sticky-cred-cn",
    "CT": "sticky-cred-ct",
    "JF": "sticky-cred-jf",
    "FS": "sticky-cred-fs",
    "AT": "sticky-cred-at",
    "PD": "sticky-cred-pd",
    "DT": "sticky-cred-dt",
}


@lru_cache(maxsize=32)
def _fetch_secret(secret_name: str) -> str:
    """Fetch and cache a secret value from Google Secret Manager."""
    # Imported here so the module is importable in environments without the SDK
    from google.cloud import secretmanager
    import os

    project = os.environ.get("GCP_PROJECT", "variant-finance-data-project")
    client = secretmanager.SecretManagerServiceClient()
    name = f"projects/{project}/secrets/{secret_name}/versions/latest"
    response = client.access_secret_version(request={"name": name})
    return response.payload.data.decode("utf-8")


def _decode_basic_auth(encoded: str) -> Tuple[str, str]:
    """Decode a base64 'user:pass' string into (user, pass)."""
    decoded = base64.b64decode(encoded).decode("utf-8")
    if ":" not in decoded:
        raise ValueError("Decoded credential is not in 'user:pass' form")
    return tuple(decoded.split(":", 1))  # type: ignore


def resolve_sticky_auth(entity: str = "_", cred: str = "_") -> Tuple[str, str]:
    """
    Return (username, password) for Sticky.io basic auth.

    Preference order:
      1. entity=<CODE>   → look up secret from Secret Manager (safe)
      2. cred=<base64>   → decode directly (legacy, insecure — logged as warning)
      3. neither given   → raises ValueError
    """
    if entity and entity != "_":
        entity_up = entity.upper().strip()
        if entity_up not in _ENTITY_SECRETS:
            raise ValueError(f"Unknown entity code '{entity}'. Known: {list(_ENTITY_SECRETS)}")
        secret_value = _fetch_secret(_ENTITY_SECRETS[entity_up])
        # Secret is stored as plain 'user:pass' — split it.
        if ":" not in secret_value:
            raise ValueError(f"Secret {_ENTITY_SECRETS[entity_up]} is not in 'user:pass' form")
        user, pw = secret_value.split(":", 1)
        return user, pw

    if cred and cred != "_":
        logger.warning(
            "Legacy 'cred' query parameter used — please migrate caller to entity=<CODE>. "
            "Credentials in URLs are logged and pose a security risk."
        )
        return _decode_basic_auth(cred)

    raise ValueError("Must provide either 'entity' (preferred) or 'cred' (legacy) query parameter")
