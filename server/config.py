"""Server configuration for the Virtue Foundation Facility Finder.

In deployment (Databricks Apps) the platform injects the app's managed
identity credentials; ``Config()`` and ``WorkspaceClient()`` pick them up
automatically. Outside the app (local development), ambient or profile-based
auth is used. No tokens are ever hardcoded or manually extracted here — the
SDK's credentials provider resolves them at request time.
"""
import os
from functools import lru_cache
from typing import Any
from urllib.parse import urlparse

IS_DATABRICKS_APP = bool(os.environ.get("DATABRICKS_APP_NAME"))
WAREHOUSE_ID_ENV = "DATABRICKS_WAREHOUSE_ID"


@lru_cache(maxsize=1)
def get_workspace_client() -> Any:
    """Workspace client via the app's managed identity (or local profile)."""
    from databricks.sdk import WorkspaceClient

    if IS_DATABRICKS_APP:
        # Apps runtime injects OAuth credentials for the app's service principal.
        return WorkspaceClient()
    # Local development: ambient serverless auth, or an explicit profile.
    profile = os.environ.get("DATABRICKS_PROFILE")
    if profile:
        return WorkspaceClient(profile=profile)
    return WorkspaceClient()


@lru_cache(maxsize=1)
def get_sdk_config() -> Any:
    """databricks.sdk.core.Config for the app runtime's managed credentials.

    In the Apps runtime this resolves the injected OAuth (m2m) credentials
    (DATABRICKS_HOST / DATABRICKS_CLIENT_ID / DATABRICKS_CLIENT_SECRET);
    locally it resolves ambient or profile-based auth.
    """
    from databricks.sdk.core import Config

    profile = None if IS_DATABRICKS_APP else os.environ.get("DATABRICKS_PROFILE")
    if profile:
        return Config(profile=profile)
    return Config()


def get_warehouse_id() -> str:
    """SQL warehouse ID for DBSQL connections (read-only queries)."""
    warehouse_id = os.environ.get(WAREHOUSE_ID_ENV, "").strip()
    if not warehouse_id:
        raise RuntimeError(
            f"{WAREHOUSE_ID_ENV} is not configured; the app needs a SQL "
            "warehouse to serve facility queries."
        )
    return warehouse_id


@lru_cache(maxsize=1)
def get_workspace_hostname() -> str:
    """Bare workspace hostname (no scheme, no path), validated.

    The SQL connector's server_hostname must be a bare hostname. An empty
    value, or one still carrying a scheme or path, is rejected here instead
    of surfacing as an opaque connect-time failure.
    """
    host = (get_sdk_config().host or "").strip()
    if not host:
        host = (get_workspace_client().config.host or "").strip()
    if not host:
        raise RuntimeError("Workspace host could not be resolved.")
    parsed = urlparse(host if "//" in host else f"https://{host}")
    hostname = (parsed.hostname or "").strip()
    if not hostname or parsed.path not in ("", "/"):
        raise RuntimeError(
            f"Invalid workspace host {host!r}: expected a bare hostname or a "
            "URL with no path component."
        )
    return hostname
