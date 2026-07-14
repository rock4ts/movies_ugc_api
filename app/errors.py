"""Error handling utilities for API responses."""

import logging
from typing import Any

from flask import Flask, Response, jsonify, make_response
from pydantic import ValidationError
from werkzeug.exceptions import Forbidden, HTTPException, Unauthorized

from app.producer import KafkaPublishError

LOGGER = logging.getLogger(__name__)


def validation_error_response(error: ValidationError) -> Response:
    """Build a JSON response for request validation failures."""
    LOGGER.warning("Validation failed for incoming request.", extra={"errors": error.errors()})
    payload = {"error": "Invalid input.", "details": error.errors()}
    return make_response(jsonify(payload), 400)


def register_error_handlers(app: Flask) -> None:
    """Register application-wide JSON error handlers."""

    @app.errorhandler(KafkaPublishError)
    def handle_kafka_publish_error(error: KafkaPublishError) -> tuple[dict[str, Any], int]:
        LOGGER.error("Kafka publishing failed.", extra={"error": str(error)})
        return {"error": "Publish failed.", "details": "Broker did not accept the message."}, 503

    @app.errorhandler(HTTPException)
    def handle_http_exception(error: HTTPException) -> tuple[dict[str, Any], int]:
        if error.code == 400:
            LOGGER.warning("Invalid request JSON payload.", extra={"error": error.description})
        return {"error": error.description or "Request failed."}, error.code or 500

    @app.errorhandler(Unauthorized)
    def handle_unauthorized_error(error: Unauthorized) -> tuple[dict[str, Any], int, dict[str, str]]:
        return (
            {"error": error.description or "Unauthorized."},
            401,
            {"WWW-Authenticate": "Bearer"},
        )

    @app.errorhandler(Forbidden)
    def handle_forbidden_error(error: Forbidden) -> tuple[dict[str, Any], int]:
        return {"error": error.description or "Forbidden."}, 403

    @app.errorhandler(Exception)
    def handle_unexpected_exception(error: Exception) -> tuple[dict[str, Any], int]:
        LOGGER.exception("Unexpected application exception.", exc_info=error)
        return {"error": "Internal server error."}, 500
