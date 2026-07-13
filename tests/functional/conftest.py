from collections.abc import Callable, Generator
from datetime import UTC, datetime
from typing import Any
import time
import uuid

from confluent_kafka import OFFSET_END, Consumer, KafkaError, TopicPartition
from confluent_kafka.admin import AdminClient
import httpx
import orjson
import pytest

from tests.functional.settings import test_settings


def _api_path(path: str) -> str:
    prefix = test_settings.api_prefix.rstrip("/")
    if not path.startswith("/"):
        path = f"/{path}"
    return f"{prefix}{path}"


def _response_payload(response: httpx.Response) -> dict[str, Any] | list[Any] | str:
    try:
        return response.json()
    except ValueError:
        return response.text


def _base_event() -> dict[str, Any]:
    return {
        "event_id": str(uuid.uuid4()),
        "user_id": str(uuid.uuid4()),
        "timestamp": datetime.now(tz=UTC).isoformat(),
    }


def make_click_event() -> dict[str, Any]:
    return {
        **_base_event(),
        "payload": {
            "event_type": "click",
            "element_id": "btn_watch",
            "element_type": "button",
        },
    }


def make_page_view_event() -> dict[str, Any]:
    return {
        **_base_event(),
        "payload": {
            "event_type": "page_view",
            "page": "/movies/42",
        },
    }


def make_movie_quality_changed_event() -> dict[str, Any]:
    return {
        **_base_event(),
        "payload": {
            "event_type": "movie_quality_changed",
            "movie_id": str(uuid.uuid4()),
            "previous_quality": "720p",
            "new_quality": "1080p",
        },
    }


def make_movie_completed_event() -> dict[str, Any]:
    return {
        **_base_event(),
        "payload": {
            "event_type": "movie_completed",
            "movie_id": str(uuid.uuid4()),
        },
    }


def make_search_filter_used_event() -> dict[str, Any]:
    return {
        **_base_event(),
        "payload": {
            "event_type": "search_filter_used",
            "filters": {"genre": "drama", "year_from": 2010, "rating_gte": 7.5},
        },
    }


@pytest.fixture(scope="function")
def http_client() -> Generator[httpx.Client, None, None]:
    with httpx.Client(
        base_url=test_settings.api_url,
        timeout=test_settings.request_timeout_seconds,
    ) as client:
        yield client


@pytest.fixture(scope="function")
def make_post_request(
    http_client: httpx.Client,
) -> Callable[
    [str, dict[str, Any] | None], tuple[dict[str, Any] | list[Any] | str, int, httpx.Response]
]:
    def inner(
        path: str,
        body: dict[str, Any] | None = None,
    ) -> tuple[dict[str, Any] | list[Any] | str, int, httpx.Response]:
        response = http_client.post(_api_path(path), json=body)
        return _response_payload(response), response.status_code, response

    return inner


@pytest.fixture(scope="function")
def make_raw_post_request(
    http_client: httpx.Client,
) -> Callable[
    [str, str, dict[str, str] | None],
    tuple[dict[str, Any] | list[Any] | str, int, httpx.Response],
]:
    def inner(
        path: str,
        raw_body: str,
        headers: dict[str, str] | None = None,
    ) -> tuple[dict[str, Any] | list[Any] | str, int, httpx.Response]:
        response = http_client.post(_api_path(path), content=raw_body, headers=headers)
        return _response_payload(response), response.status_code, response

    return inner


@pytest.fixture(scope="function")
def event_factory() -> Callable[[str], dict[str, Any]]:
    event_builders: dict[str, Callable[[], dict[str, Any]]] = {
        "click": make_click_event,
        "page_view": make_page_view_event,
        "movie_quality_changed": make_movie_quality_changed_event,
        "movie_completed": make_movie_completed_event,
        "search_filter_used": make_search_filter_used_event,
    }

    def build(event_type: str) -> dict[str, Any]:
        builder = event_builders[event_type]
        return builder()

    return build


@pytest.fixture(scope="session", autouse=True)
def clear_kafka_topic() -> None:
    admin = AdminClient({"bootstrap.servers": test_settings.kafka_bootstrap_servers})
    metadata = admin.list_topics(topic=test_settings.kafka_topic, timeout=10)
    topic_metadata = metadata.topics.get(test_settings.kafka_topic)
    if topic_metadata is None:
        return

    partitions_to_clear = [
        TopicPartition(test_settings.kafka_topic, partition_id, OFFSET_END)
        for partition_id in topic_metadata.partitions
    ]
    if not partitions_to_clear:
        return

    futures = admin.delete_records(partitions_to_clear, operation_timeout=30.0)
    for future in futures.values():
        future.result()


@pytest.fixture(scope="session")
def kafka_consumer() -> Generator[Consumer, None, None]:
    consumer = Consumer(
        {
            "bootstrap.servers": test_settings.kafka_bootstrap_servers,
            "group.id": f"ugc-api-functional-{uuid.uuid4().hex}",
            "auto.offset.reset": "earliest",
            "enable.auto.commit": False,
        }
    )
    consumer.subscribe([test_settings.kafka_topic])
    yield consumer
    consumer.close()


@pytest.fixture(scope="function")
def consume_kafka_event_by_id(kafka_consumer: Consumer) -> Callable[[str], dict[str, Any]]:
    def inner(event_id: str) -> dict[str, Any]:
        deadline = time.monotonic() + test_settings.kafka_wait_timeout_seconds
        while time.monotonic() < deadline:
            message = kafka_consumer.poll(test_settings.kafka_poll_timeout_seconds)
            if message is None:
                continue

            if message.error() is not None:
                if message.error().code() == KafkaError._PARTITION_EOF:
                    continue
                raise AssertionError(f"Kafka poll failed: {message.error()}")

            value_raw = message.value()
            if value_raw is None:
                continue

            payload = orjson.loads(value_raw)
            if payload.get("event_id") != event_id:
                continue

            key_raw = message.key()
            key = key_raw.decode("utf-8") if key_raw is not None else None
            return {
                "key": key,
                "value": payload,
                "topic": message.topic(),
                "partition": message.partition(),
                "offset": message.offset(),
            }

        raise AssertionError(
            f"Kafka message with event_id={event_id} was not received in "
            f"{test_settings.kafka_wait_timeout_seconds} seconds."
        )

    return inner


@pytest.fixture(scope="function")
def post_event_and_consume(
    make_post_request: Callable[
        [str, dict[str, Any] | None], tuple[dict[str, Any] | list[Any] | str, int, httpx.Response]
    ],
    consume_kafka_event_by_id: Callable[[str], dict[str, Any]],
) -> Callable[[dict[str, Any]], tuple[dict[str, Any] | list[Any] | str, int, dict[str, Any]]]:
    def inner(
        event_payload: dict[str, Any],
    ) -> tuple[dict[str, Any] | list[Any] | str, int, dict[str, Any]]:
        body, status, _ = make_post_request("/events", event_payload)
        consumed_message = consume_kafka_event_by_id(event_payload["event_id"])
        return body, status, consumed_message

    return inner
