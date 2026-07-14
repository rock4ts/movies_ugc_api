"""Kafka producer integration for event publishing."""

import logging

import orjson
from confluent_kafka import KafkaError, KafkaException, Message, Producer

from app.config import KafkaSettings
from app.models import AnonymousEvent, UserEvent

LOGGER = logging.getLogger(__name__)


class KafkaPublishError(Exception):
    """Raised when the producer cannot accept a new event."""


class KafkaProducerService:
    """Service responsible for Kafka producer lifecycle and publishing."""

    def __init__(self, settings: KafkaSettings) -> None:
        """Initialize a producer with runtime settings."""
        self._topic: str = settings.topic
        self._anonymous_topic: str = settings.anonymous_topic
        config: dict[str, str | bool | int] = {
            "bootstrap.servers": settings.bootstrap_servers,
            "acks": settings.acks,
            "enable.idempotence": settings.enable_idempotence,
            "linger.ms": settings.linger_ms,
            "batch.size": settings.batch_size,
        }
        self._producer: Producer = Producer(config)

    def publish(self, event: UserEvent) -> None:
        """Queue a single event for asynchronous Kafka delivery."""
        self._publish(topic=self._topic, key=str(event.user_id), event_payload=event.model_dump(mode="json"))

    def publish_anonymous(self, event: AnonymousEvent) -> None:
        """Queue a single anonymous event for asynchronous Kafka delivery."""
        self._publish(
            topic=self._anonymous_topic,
            key=str(event.anonymous_id),
            event_payload=event.model_dump(mode="json"),
        )

    def _publish(self, topic: str, key: str, event_payload: dict[str, object]) -> None:
        """Queue a serialized event payload for asynchronous Kafka delivery."""
        message_value = orjson.dumps(event_payload)

        try:
            self._producer.produce(
                topic=topic,
                key=key,
                value=message_value,
                on_delivery=self._delivery_callback,
            )
            _ = self._producer.poll(0)
        except BufferError as exc:
            raise KafkaPublishError("Kafka producer queue is full.") from exc
        except KafkaException as exc:
            raise KafkaPublishError("Kafka producer is unavailable.") from exc

    def close(self) -> None:
        """Flush queued messages during graceful shutdown."""
        undelivered_messages = self._producer.flush(timeout=10.0)
        if undelivered_messages > 0:
            LOGGER.warning(
                "Kafka producer closed with pending messages.",
                extra={"pending_messages": undelivered_messages},
            )

    @staticmethod
    def _delivery_callback(error: KafkaError | None, message: Message) -> None:
        """Log asynchronous delivery failures reported by librdkafka."""
        if error is not None:
            LOGGER.error(
                "Kafka delivery failed.",
                extra={
                    "topic": message.topic(),
                    "partition": message.partition(),
                    "offset": message.offset(),
                    "error": str(error),
                },
            )
