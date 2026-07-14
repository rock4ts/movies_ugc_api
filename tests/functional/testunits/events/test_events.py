from collections.abc import Callable
from http import HTTPStatus
from typing import Any

import pytest


@pytest.mark.parametrize(
    "event_type",
    [
        "click",
        "page_view",
        "movie_quality_changed",
        "movie_completed",
        "search_filter_used",
    ],
)
def test_ingest_event_accepts_payload_and_publishes_to_kafka(
    event_type: str,
    authenticated_event: Callable[[str], tuple[dict[str, Any], dict[str, str]]],
    post_event_and_consume: Callable[
        [dict[str, Any], dict[str, str] | None],
        tuple[dict[str, Any] | list[Any] | str, int, dict[str, Any]],
    ],
) -> None:
    event_payload, headers = authenticated_event(event_type)

    body, status, kafka_message = post_event_and_consume(event_payload, headers)

    assert status == HTTPStatus.ACCEPTED
    assert isinstance(body, dict)
    assert body["status"] == "accepted"
    assert body["event_id"] == event_payload["event_id"]

    assert kafka_message["key"] == event_payload["user_id"]
    assert kafka_message["value"]["event_id"] == event_payload["event_id"]
    assert kafka_message["value"]["user_id"] == event_payload["user_id"]
    assert kafka_message["value"]["payload"] == event_payload["payload"]
