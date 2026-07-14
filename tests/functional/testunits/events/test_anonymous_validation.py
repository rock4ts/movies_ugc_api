from collections.abc import Callable
from http import HTTPStatus
from typing import Any

import pytest


@pytest.mark.parametrize(
    "event_payload",
    [
        {
            "event_id": "5ad4c3fa-d79e-41e5-a45f-e58fe74de370",
            "anonymous_id": "0f6d6a9d-303f-4e18-b7e1-db8454cc2948",
            "timestamp": "2026-01-01T10:00:00+00:00",
            "payload": {"event_type": "unknown_event"},
        },
        {
            "event_id": "not-a-uuid",
            "anonymous_id": "0f6d6a9d-303f-4e18-b7e1-db8454cc2948",
            "timestamp": "2026-01-01T10:00:00+00:00",
            "payload": {
                "event_type": "click",
                "element_id": "btn",
                "element_type": "button",
            },
        },
        {
            "event_id": "5ad4c3fa-d79e-41e5-a45f-e58fe74de370",
            "anonymous_id": "not-a-uuid",
            "timestamp": "2026-01-01T10:00:00+00:00",
            "payload": {
                "event_type": "click",
                "element_id": "btn",
                "element_type": "button",
            },
        },
        {
            "event_id": "5ad4c3fa-d79e-41e5-a45f-e58fe74de370",
            "anonymous_id": "0f6d6a9d-303f-4e18-b7e1-db8454cc2948",
            "timestamp": "invalid-timestamp",
            "payload": {
                "event_type": "click",
                "element_id": "btn",
                "element_type": "button",
            },
        },
        {
            "event_id": "5ad4c3fa-d79e-41e5-a45f-e58fe74de370",
            "timestamp": "2026-01-01T10:00:00+00:00",
            "payload": {
                "event_type": "click",
                "element_id": "btn",
                "element_type": "button",
            },
        },
        {
            "event_id": "5ad4c3fa-d79e-41e5-a45f-e58fe74de370",
            "anonymous_id": "0f6d6a9d-303f-4e18-b7e1-db8454cc2948",
            "timestamp": "2026-01-01T10:00:00+00:00",
            "payload": {"event_type": "click", "element_id": "btn"},
        },
        {
            "event_id": "5ad4c3fa-d79e-41e5-a45f-e58fe74de370",
            "anonymous_id": "0f6d6a9d-303f-4e18-b7e1-db8454cc2948",
            "timestamp": "2026-01-01T10:00:00+00:00",
            "payload": {"element_id": "btn", "element_type": "button"},
        },
    ],
)
def test_ingest_anonymous_event_rejects_invalid_payload(
    event_payload: dict[str, Any],
    make_post_request: Callable[
        [str, dict[str, Any] | None, dict[str, str] | None],
        tuple[dict[str, Any] | list[Any] | str, int, Any],
    ],
) -> None:
    body, status, _ = make_post_request("/anonymous-events", event_payload)

    assert status in {HTTPStatus.BAD_REQUEST, HTTPStatus.UNPROCESSABLE_ENTITY}
    assert isinstance(body, dict)
    assert "error" in body


def test_ingest_anonymous_event_rejects_malformed_json(
    make_raw_post_request: Callable[
        [str, str, dict[str, str] | None], tuple[dict[str, Any] | list[Any] | str, int, Any]
    ],
) -> None:
    body, status, _ = make_raw_post_request(
        "/anonymous-events",
        '{"payload":{"event_type":"click"},}',
        {"Content-Type": "application/json"},
    )

    assert status == HTTPStatus.BAD_REQUEST
    assert isinstance(body, dict)
    assert "error" in body

