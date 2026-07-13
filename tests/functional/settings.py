from dataclasses import dataclass
import os


@dataclass(frozen=True)
class FunctionalTestSettings:
    api_url: str = os.getenv("UGC_TEST_API_URL", "http://localhost:18000")
    api_prefix: str = os.getenv("UGC_TEST_API_PREFIX", "/api/v1")
    request_timeout_seconds: float = float(os.getenv("UGC_TEST_TIMEOUT", "10"))

    kafka_bootstrap_servers: str = os.getenv("UGC_TEST_KAFKA_BOOTSTRAP", "localhost:29093")
    kafka_topic: str = os.getenv("UGC_TEST_KAFKA_TOPIC", "ugc-events-test")
    kafka_poll_timeout_seconds: float = float(os.getenv("UGC_TEST_KAFKA_POLL_TIMEOUT", "1"))
    kafka_wait_timeout_seconds: float = float(os.getenv("UGC_TEST_KAFKA_WAIT_TIMEOUT", "15"))


test_settings = FunctionalTestSettings()
