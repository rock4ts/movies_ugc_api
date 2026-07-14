from collections.abc import Callable
from http import HTTPStatus
from typing import Any
from uuid import uuid4


def test_ingest_event_rejects_missing_token(
    event_factory: Callable[[str, str | None], dict[str, Any]],
    make_post_request: Callable[
        [str, dict[str, Any] | None, dict[str, str] | None],
        tuple[dict[str, Any] | list[Any] | str, int, Any],
    ],
) -> None:
    event_payload = event_factory("click")
    body, status, _ = make_post_request("/events", event_payload)

    assert status == HTTPStatus.UNAUTHORIZED
    assert isinstance(body, dict)
    assert "error" in body


def test_ingest_event_rejects_invalid_token(
    event_factory: Callable[[str, str | None], dict[str, Any]],
    make_post_request: Callable[
        [str, dict[str, Any] | None, dict[str, str] | None],
        tuple[dict[str, Any] | list[Any] | str, int, Any],
    ],
) -> None:
    event_payload = event_factory("click")
    body, status, _ = make_post_request(
        "/events",
        event_payload,
        {"Authorization": "Bearer invalid-token"},
    )

    assert status == HTTPStatus.UNAUTHORIZED
    assert isinstance(body, dict)
    assert "error" in body


def test_ingest_event_rejects_expired_token(
    event_factory: Callable[[str, str | None], dict[str, Any]],
    make_access_token: Callable[..., str],
    auth_headers: Callable[[str], dict[str, str]],
    make_post_request: Callable[
        [str, dict[str, Any] | None, dict[str, str] | None],
        tuple[dict[str, Any] | list[Any] | str, int, Any],
    ],
) -> None:
    user_id = str(uuid4())
    event_payload = event_factory("click", user_id)
    token = make_access_token(user_id=user_id, expires_in_minutes=-1)
    body, status, _ = make_post_request("/events", event_payload, auth_headers(token))

    assert status == HTTPStatus.UNAUTHORIZED
    assert isinstance(body, dict)
    assert "error" in body


def test_ingest_event_rejects_mismatched_user_id(
    event_factory: Callable[[str, str | None], dict[str, Any]],
    make_access_token: Callable[..., str],
    auth_headers: Callable[[str], dict[str, str]],
    make_post_request: Callable[
        [str, dict[str, Any] | None, dict[str, str] | None],
        tuple[dict[str, Any] | list[Any] | str, int, Any],
    ],
) -> None:
    event_payload = event_factory("click", str(uuid4()))
    token = make_access_token(user_id=str(uuid4()))
    body, status, _ = make_post_request("/events", event_payload, auth_headers(token))

    assert status == HTTPStatus.FORBIDDEN
    assert isinstance(body, dict)
    assert "error" in body


def test_ingest_event_accepts_matching_user_id(
    authenticated_event: Callable[[str], tuple[dict[str, Any], dict[str, str]]],
    post_event_and_consume: Callable[
        [dict[str, Any], dict[str, str] | None],
        tuple[dict[str, Any] | list[Any] | str, int, dict[str, Any]],
    ],
) -> None:
    event_payload, headers = authenticated_event("click")
    body, status, _ = post_event_and_consume(event_payload, headers)

    assert status == HTTPStatus.ACCEPTED
    assert isinstance(body, dict)
    assert body["status"] == "accepted"
