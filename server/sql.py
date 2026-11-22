"""Thin DBSQL query wrapper for the Virtue Foundation Facility Finder.

Every query uses named (:name) parameters — user input is never interpolated
into SQL text. Read-only SELECTs only. Authentication follows the documented
Databricks Apps SQL pattern: the SDK credentials provider supplies and
refreshes auth headers via the app runtime's managed identity. Tokens are
never extracted, manually cached, or hardcoded.
"""
import logging
import time
from functools import lru_cache
from typing import Any, Dict, List

from databricks import sql

from . import config

# uvicorn.error output is verifiably shipped to the app logs.
log = logging.getLogger("uvicorn.error")


@lru_cache(maxsize=1)
def _sdk_config() -> Any:
    return config.get_sdk_config()


def _connect_kwargs() -> Dict[str, Any]:
    """DBSQL connect kwargs using the SDK credentials provider.

    ``credentials_provider=lambda: cfg.authenticate`` hands the connector a
    callable that returns fresh auth headers on every request — no manual
    token handling anywhere.
    """
    cfg = _sdk_config()
    return {
        # Validated bare hostname: nonempty, no scheme, no path.
        "server_hostname": config.get_workspace_hostname(),
        "http_path": f"/sql/1.0/warehouses/{config.get_warehouse_id()}",
        "credentials_provider": lambda: cfg.authenticate,
    }


def run_query(query: str, params: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Execute a query with named parameters; return rows as plain dicts.

    Logs connect, execute, and fetch durations separately for diagnostics.
    """
    kwargs = _connect_kwargs()
    t0 = time.perf_counter()
    with sql.connect(**kwargs) as conn:
        t1 = time.perf_counter()
        with conn.cursor() as cur:
            cur.execute(query, parameters=params)
            t2 = time.perf_counter()
            cols = [d[0] for d in cur.description]
            rows = cur.fetchall()
        t3 = time.perf_counter()
    log.warning(
        "DBSQL timing: connect_ms=%.0f execute_ms=%.0f fetch_ms=%.0f",
        (t1 - t0) * 1000.0,
        (t2 - t1) * 1000.0,
        (t3 - t2) * 1000.0,
    )
    return [dict(zip(cols, r)) for r in rows]


def run_query_pair(
    query1: str,
    params1: Dict[str, Any],
    query2: str,
    params2: Dict[str, Any],
) -> tuple:
    """Execute TWO statements on ONE connection and return (rows1, rows2).

    Used for the count+data statement pair of a search: one connect/auth
    handshake instead of two. Same guarantees as run_query — named
    parameters only, read-only SELECTs, per-stage timings logged without
    query text or parameter values (so no phrases or coordinates appear).
    """
    kwargs = _connect_kwargs()
    t0 = time.perf_counter()
    out: List[List[Dict[str, Any]]] = []
    timings: List[tuple] = []
    with sql.connect(**kwargs) as conn:
        t1 = time.perf_counter()
        for query, params in ((query1, params1), (query2, params2)):
            ta = time.perf_counter()
            with conn.cursor() as cur:
                cur.execute(query, parameters=params)
                tb = time.perf_counter()
                cols = [d[0] for d in cur.description]
                rows = cur.fetchall()
                tc = time.perf_counter()
            timings.append((tb - ta, tc - tb))
            out.append([dict(zip(cols, r)) for r in rows])
    log.warning(
        "DBSQL pair timing: connect_ms=%.0f exec1_ms=%.0f fetch1_ms=%.0f "
        "exec2_ms=%.0f fetch2_ms=%.0f",
        (t1 - t0) * 1000.0,
        timings[0][0] * 1000.0, timings[0][1] * 1000.0,
        timings[1][0] * 1000.0, timings[1][1] * 1000.0,
    )
    return out[0], out[1]
