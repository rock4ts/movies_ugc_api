from dataclasses import dataclass
import os


@dataclass(frozen=True)
class FunctionalTestSettings:
    api_url: str = os.getenv("UGC_TEST_API_URL", "http://localhost:18000")
    api_prefix: str = os.getenv("UGC_TEST_API_PREFIX", "/api/v1")
    request_timeout_seconds: float = float(os.getenv("UGC_TEST_TIMEOUT", "10"))

    kafka_bootstrap_servers: str = os.getenv("UGC_TEST_KAFKA_BOOTSTRAP", "localhost:29093")
    kafka_topic: str = os.getenv("UGC_TEST_KAFKA_TOPIC", "ugc-events-test")
    kafka_anonymous_topic: str = os.getenv(
        "UGC_TEST_KAFKA_ANONYMOUS_TOPIC", "ugc-anonymous-events-test"
    )
    kafka_poll_timeout_seconds: float = float(os.getenv("UGC_TEST_KAFKA_POLL_TIMEOUT", "1"))
    kafka_wait_timeout_seconds: float = float(os.getenv("UGC_TEST_KAFKA_WAIT_TIMEOUT", "15"))


@dataclass(frozen=True)
class JWTTestSettings:
    private_key_path: str = os.getenv(
        "UGC_TEST_PRIVATE_KEY_PATH", "tests/docker/certs/jwt-private.pem"
    )
    jwt_algorithm: str = os.getenv("UGC_TEST_JWT_ALGORITHM", "RS256")


test_settings = FunctionalTestSettings()
jwt_test_settings = JWTTestSettings()
