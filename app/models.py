"""Pydantic models used by the UGC API service."""

from datetime import datetime
from enum import Enum
from typing import Any
from uuid import UUID

from pydantic import BaseModel, Field


class EventType(str, Enum):
    """Supported event types for ingestion."""

    CLICK = "click"
    PAGE_VIEW = "page_view"
    VIDEO_QUALITY_CHANGED = "video_quality_changed"
    VIDEO_COMPLETED = "video_completed"
    SEARCH_FILTER_USED = "search_filter_used"


class UserEvent(BaseModel):
    """Generic envelope for incoming user activity events."""

    event_id: UUID = Field(..., description="Unique event identifier.")
    event_type: EventType = Field(..., description="Type of the user event.")
    user_id: UUID = Field(..., description="Unique user identifier.")
    timestamp: datetime = Field(..., description="Event timestamp in ISO-8601 format.")
    payload: dict[str, Any] = Field(..., description="Event-specific payload without strict schema.")


class EventAcceptedResponse(BaseModel):
    """Response returned when an event is accepted for Kafka publishing."""

    status: str = Field(default="accepted", description="Ingestion status.")
    event_id: UUID = Field(..., description="Accepted event identifier.")


class ErrorResponse(BaseModel):
    """Generic API error response schema."""

    error: str = Field(..., description="Human-readable error message.")
    details: Any | None = Field(default=None, description="Additional details about the error.")
