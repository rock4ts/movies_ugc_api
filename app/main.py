"""Application factory and local entrypoint."""

import atexit
import logging

from flask_openapi3.models.info import Info
from flask_openapi3.openapi import OpenAPI

from app.config import app_settings, kafka_settings
from app.errors import register_error_handlers, validation_error_response
from app.logging_config import setup_logging
from app.producer import KafkaProducerService
from app.views import events_api

LOGGER = logging.getLogger(__name__)


def create_app() -> OpenAPI:
    """Create and configure the Flask OpenAPI application."""
    setup_logging(app_settings.debug)

    app = OpenAPI(
        __name__,
        info=Info(title="UGC API", version="1.0.0"),
        security_schemes={
            "bearerAuth": {
                "type": "http",
                "scheme": "bearer",
                "bearerFormat": "JWT",
            }
        },
        validation_error_callback=validation_error_response,
    )
    app.config["ENV"] = app_settings.flask_env
    app.config["DEBUG"] = app_settings.debug

    producer = KafkaProducerService(settings=kafka_settings)
    app.extensions["kafka_producer"] = producer
    app.register_api(events_api)
    register_error_handlers(app)

    def shutdown_producer() -> None:
        """Flush pending Kafka messages at process shutdown."""
        LOGGER.info("Application shutdown started.")
        producer.close()
        LOGGER.info("Application shutdown completed.")

    _ = atexit.register(shutdown_producer)

    LOGGER.info(
        "Application started.",
        extra={"flask_env": app_settings.flask_env, "kafka_topic": kafka_settings.topic},
    )
    return app


if __name__ == "__main__":
    application = create_app()
    application.run(host="0.0.0.0", port=8000, debug=application.config["DEBUG"])
