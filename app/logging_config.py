"""Logging configuration utilities."""

import logging


def setup_logging(debug: bool) -> None:
    """Configure root logging for the service."""
    level = logging.DEBUG if debug else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
