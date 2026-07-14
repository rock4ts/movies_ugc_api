"""HTTP endpoints for event ingestion."""

from typing import cast

from flask import current_app, g, request
from flask_openapi3 import APIBlueprint, Tag

from app.models import AccessTokenPayload, ErrorResponse, EventAcceptedResponse, UserEvent
from app.producer import KafkaProducerService
from app.security import decode_access_token, ensure_user_id_matches, parse_bearer_token

events_tag = Tag(name="events", description="User activity events ingestion API")
events_api = APIBlueprint("events", __name__, url_prefix="/api/v1", abp_tags=[events_tag])


@events_api.before_request
def require_access_token() -> None:
    token = parse_bearer_token(request.headers.get("Authorization"))
    g.access_token = decode_access_token(token)


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
def ingest_event(body: UserEvent) -> tuple[dict[str, object], int]:
    """Accept an event and enqueue it for Kafka publishing."""
    token_payload = cast(AccessTokenPayload, g.access_token)
    ensure_user_id_matches(token_payload, body.user_id)

    producer = cast(KafkaProducerService, current_app.extensions["kafka_producer"])
    producer.publish(body)

    response = EventAcceptedResponse(event_id=body.event_id)
    return response.model_dump(mode="json"), 202
