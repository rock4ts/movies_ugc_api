from collections.abc import Callable
from http import HTTPStatus
from typing import Any

import pytest

from tests.functional.settings import test_settings


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
def test_ingest_anonymous_event_accepts_payload_and_publishes_to_kafka(
    event_type: str,
    anonymous_event_factory: Callable[[str, str | None], dict[str, Any]],
    post_anonymous_event_and_consume: Callable[
        [dict[str, Any], dict[str, str] | None],
        tuple[dict[str, Any] | list[Any] | str, int, dict[str, Any]],
    ],
) -> None:
    event_payload = anonymous_event_factory(event_type)

    body, status, kafka_message = post_anonymous_event_and_consume(event_payload)

    assert status == HTTPStatus.ACCEPTED
    assert isinstance(body, dict)
    assert body["status"] == "accepted"
    assert body["event_id"] == event_payload["event_id"]

    assert kafka_message["topic"] == test_settings.kafka_anonymous_topic
    assert kafka_message["key"] == event_payload["anonymous_id"]
    assert kafka_message["value"]["event_id"] == event_payload["event_id"]
    assert kafka_message["value"]["anonymous_id"] == event_payload["anonymous_id"]
    assert kafka_message["value"]["payload"] == event_payload["payload"]

