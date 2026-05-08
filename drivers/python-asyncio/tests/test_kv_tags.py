from __future__ import annotations

import json
from typing import Any

import httpx
import pytest

from reddb_asyncio.client import KvClient
from reddb_asyncio.http import HttpClient
from reddb_asyncio.redwire import _kv_invalidate_tags_sql, _kv_put_sql


@pytest.mark.asyncio
async def test_kv_client_forwards_tagged_put_and_invalidate_tags() -> None:
    class Transport:
        def __init__(self) -> None:
            self.calls: list[tuple[str, tuple[Any, ...], dict[str, Any]]] = []

        async def kv_put(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
            self.calls.append(("kv_put", args, kwargs))
            return {"affected": 1}

        async def kv_invalidate_tags(self, *args: Any, **kwargs: Any) -> dict[str, Any]:
            self.calls.append(("kv_invalidate_tags", args, kwargs))
            return {"affected": 2}

    transport = Transport()
    kv = KvClient(transport)

    assert await kv.put(
        "sessions",
        "user:1",
        {"role": "admin"},
        tags=["tenant:1"],
        ttl_ms=5000,
        if_not_exists=True,
    ) == {"affected": 1}
    assert await kv.invalidate_tags("sessions", ["tenant:1"]) == {"affected": 2}
    assert transport.calls == [
        (
            "kv_put",
            ("sessions", "user:1", {"role": "admin"}),
            {"tags": ["tenant:1"], "ttl_ms": 5000, "if_not_exists": True},
        ),
        ("kv_invalidate_tags", ("sessions", ["tenant:1"]), {}),
    ]


@pytest.mark.asyncio
async def test_http_tagged_put_and_invalidate_tags_use_query_sql() -> None:
    queries: list[str] = []

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/query"
        body = json.loads(request.content.decode("utf-8"))
        queries.append(body["query"])
        return httpx.Response(200, json={"ok": True, "result": {"affected": len(queries)}})

    raw = httpx.AsyncClient(
        base_url="http://testserver",
        transport=httpx.MockTransport(handler),
    )
    client = HttpClient(base_url="http://testserver", client=raw)
    try:
        assert await client.kv_put(
            "sessions",
            "user:1",
            {"role": "admin"},
            tags=["tenant:1", "vip'user"],
            ttl_ms=5000,
            if_not_exists=True,
        ) == {"affected": 1}
        assert await client.kv_invalidate_tags("sessions", ["active", "tenant:1"]) == {
            "affected": 2
        }
    finally:
        await client.close()
        await raw.aclose()

    assert queries == [
        "PUT 'sessions'.'user:1' = '{\"role\":\"admin\"}' EXPIRE 5000 ms TAGS ['tenant:1', 'vip''user'] IF NOT EXISTS",
        "INVALIDATE TAGS ['active', 'tenant:1'] FROM sessions",
    ]


def test_redwire_kv_tags_sql_builders() -> None:
    assert (
        _kv_put_sql(
            "sessions",
            "user:1",
            {"role": "admin"},
            tags=["tenant:1", "vip'user"],
            ttl_ms=5000,
            if_not_exists=True,
        )
        == "PUT 'sessions'.'user:1' = '{\"role\":\"admin\"}' EXPIRE 5000 ms TAGS ['tenant:1', 'vip''user'] IF NOT EXISTS"
    )
    assert (
        _kv_invalidate_tags_sql("sessions", ["active", "tenant:1"])
        == "INVALIDATE TAGS ['active', 'tenant:1'] FROM sessions"
    )
