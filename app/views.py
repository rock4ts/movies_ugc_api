"""HTTP endpoints for event ingestion."""

from collections.abc import Callable
from functools import wraps
from typing import cast

from flask import current_app, g, request
from flask_openapi3 import APIBlueprint, Tag

from app.models import (
    AccessTokenPayload,
    AnonymousEvent,
    ErrorResponse,
    EventAcceptedResponse,
    UserEvent,
)
from app.producer import KafkaProducerService
from app.security import decode_access_token, ensure_user_id_matches, parse_bearer_token

events_tag = Tag(name="events", description="User activity events ingestion API")
events_api = APIBlueprint("events", __name__, url_prefix="/api/v1", abp_tags=[events_tag])


def require_access_token(
    handler: Callable[..., tuple[dict[str, object], int]],
) -> Callable[..., tuple[dict[str, object], int]]:
    """Protect a view with access token validation."""

    @wraps(handler)
    def wrapped(*args: object, **kwargs: object) -> tuple[dict[str, object], int]:
        token = parse_bearer_token(request.headers.get("Authorization"))
        g.access_token = decode_access_token(token)
        return handler(*args, **kwargs)

    return wrapped


@events_api.post(
    "/events",
    responses={
        202: EventAcceptedResponse,
        400: ErrorResponse,
        401: ErrorResponse,
        403: ErrorResponse,
        422: ErrorResponse,
        503: ErrorResponse,
        500: ErrorResponse,
    },
    security=[{"bearerAuth": []}],
    summary="Ingest a user activity event",
    description="Accepts one event and publishes it to Kafka with user_id as key.",
)
@require_access_token
def ingest_event(body: UserEvent) -> tuple[dict[str, object], int]:
    """Accept an event and enqueue it for Kafka publishing."""
    token_payload = cast(AccessTokenPayload, g.access_token)
    ensure_user_id_matches(token_payload, body.user_id)

    producer = cast(KafkaProducerService, current_app.extensions["kafka_producer"])
    producer.publish(body)

    response = EventAcceptedResponse(event_id=body.event_id)
    return response.model_dump(mode="json"), 202


@events_api.post(
    "/anonymous-events",
    responses={
        202: EventAcceptedResponse,
        400: ErrorResponse,
        422: ErrorResponse,
        503: ErrorResponse,
        500: ErrorResponse,
    },
    summary="Ingest an anonymous activity event",
    description="Accepts one anonymous event and publishes it to Kafka with anonymous_id as key.",
)
def ingest_anonymous_event(body: AnonymousEvent) -> tuple[dict[str, object], int]:
    """Accept an anonymous event and enqueue it for Kafka publishing."""
    producer = cast(KafkaProducerService, current_app.extensions["kafka_producer"])
    producer.publish_anonymous(body)

    response = EventAcceptedResponse(event_id=body.event_id)
    return response.model_dump(mode="json"), 202
