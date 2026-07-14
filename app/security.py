"""JWT authentication helpers."""

from uuid import UUID

import jwt
from pydantic import ValidationError
from werkzeug.exceptions import Forbidden, Unauthorized

from app.models import AccessTokenPayload
from app.config import jwt_settings


def parse_bearer_token(authorization_header: str | None) -> str:
    """Extract Bearer token from Authorization header."""
    if not authorization_header:
        raise Unauthorized("Missing Authorization header.", www_authenticate="Bearer")

    scheme, _, token = authorization_header.partition(" ")
    if scheme.lower() != "bearer" or not token.strip():
        raise Unauthorized("Invalid Authorization header format.", www_authenticate="Bearer")
    return token.strip()


def decode_access_token(token: str) -> AccessTokenPayload:
    """Decode and validate access JWT payload."""
    try:
        payload = jwt.decode(token, jwt_settings.public_key, algorithms=[jwt_settings.algorithm])
    except jwt.ExpiredSignatureError as error:
        raise Unauthorized("Token has expired.", www_authenticate="Bearer") from error
    except jwt.InvalidTokenError as error:
        raise Unauthorized("Invalid token.", www_authenticate="Bearer") from error

    try:
        token_payload = AccessTokenPayload.model_validate(payload)
    except ValidationError as error:
        raise Unauthorized("Invalid token payload.", www_authenticate="Bearer") from error

    if token_payload.type != "access":
        raise Unauthorized("Token is not an access token.", www_authenticate="Bearer")

    return token_payload


def ensure_user_id_matches(payload: AccessTokenPayload, user_id: UUID) -> None:
    """Ensure event user_id belongs to authenticated user."""
    if payload.sub != user_id:
        raise Forbidden("Token user does not match event user_id.")



