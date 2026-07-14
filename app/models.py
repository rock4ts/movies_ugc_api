"""Pydantic models used by the UGC API service."""

from datetime import datetime
from enum import Enum
from typing import Any, Literal
from uuid import UUID

from pydantic import BaseModel, Field


class EventType(str, Enum):
    """Supported event types for ingestion."""

    CLICK = "click"
    PAGE_VIEW = "page_view"
    MOVIE_QUALITY_CHANGED = "movie_quality_changed"
    MOVIE_COMPLETED = "movie_completed"
    SEARCH_FILTER_USED = "search_filter_used"


class EventPayload(BaseModel):
    """Base schema for a discriminated event payload variant."""


class BaseUserEvent(BaseModel):
    """Common envelope for incoming user activity events."""

    event_id: UUID = Field(..., description="Unique event identifier.")
    user_id: UUID = Field(..., description="Unique user identifier.")
    timestamp: datetime = Field(..., description="Event timestamp in ISO-8601 format.")


class BaseAnonymousEvent(BaseModel):
    """Common envelope for incoming anonymous activity events."""

    event_id: UUID = Field(..., description="Unique event identifier.")
    anonymous_id: UUID = Field(..., description="Client-generated anonymous identifier.")
    timestamp: datetime = Field(..., description="Event timestamp in ISO-8601 format.")


class ClickPayload(EventPayload):
    """Payload for a user click on an interface element."""

    event_type: Literal[EventType.CLICK] = Field(
        ...,
        description="Discriminator value identifying a click event.",
    )
    element_id: str = Field(..., description="Identifier of the clicked interface element.")
    element_type: str = Field(..., description="Type of the clicked interface element.")


class PageViewPayload(EventPayload):
    """Payload for a page or application screen view."""

    event_type: Literal[EventType.PAGE_VIEW] = Field(
        ...,
        description="Discriminator value identifying a page view event.",
    )
    page: str = Field(..., description="URL or identifier of the viewed page or screen.")


class MovieQualityChangedPayload(EventPayload):
    """Payload for a user-selected movie playback quality change."""

    event_type: Literal[EventType.MOVIE_QUALITY_CHANGED] = Field(
        ...,
        description="Discriminator value identifying a movie quality change event.",
    )
    movie_id: UUID = Field(..., description="Identifier of the movie being played.")
    previous_quality: str = Field(..., description="Quality before the change.")
    new_quality: str = Field(..., description="Quality selected by the user.")


class MovieCompletedPayload(EventPayload):
    """Payload for a movie watched by the user until completion."""

    event_type: Literal[EventType.MOVIE_COMPLETED] = Field(
        ...,
        description="Discriminator value identifying a movie completion event.",
    )
    movie_id: UUID = Field(..., description="Identifier of the completed movie.")


class SearchFilterUsedPayload(EventPayload):
    """Payload for filters applied by a user while searching for films."""

    event_type: Literal[EventType.SEARCH_FILTER_USED] = Field(
        ...,
        description="Discriminator value identifying a search filter usage event.",
    )
    filters: dict[str, Any] = Field(..., description="Applied filters grouped by filter name.")


EventPayloadUnion = (
    ClickPayload
    | PageViewPayload
    | MovieQualityChangedPayload
    | MovieCompletedPayload
    | SearchFilterUsedPayload
)


class UserEvent(BaseUserEvent):
    payload: EventPayloadUnion = Field(
        ...,
        description="Event-specific payload content; shape depends on event_type.",
        discriminator="event_type",
    )


class AnonymousEvent(BaseAnonymousEvent):
    payload: EventPayloadUnion = Field(
        ...,
        description="Event-specific payload content; shape depends on event_type.",
        discriminator="event_type",
    )


class EventAcceptedResponse(BaseModel):
    """Response returned when an event is accepted for Kafka publishing."""

    status: str = Field(default="accepted", description="Ingestion status.")
    event_id: UUID = Field(..., description="Accepted event identifier.")


class ErrorResponse(BaseModel):
    """Generic API error response schema."""

    error: str = Field(..., description="Human-readable error message.")
    details: Any | None = Field(default=None, description="Additional details about the error.")


class AccessTokenPayload(BaseModel):
    """Validated claims from an access JWT."""

    type: Literal["access"]
    is_superuser: bool
    role: str | None = None
    access_labels: list[str]
    sub: UUID
    iat: datetime
    exp: datetime
    jti: UUID
    tv: int
