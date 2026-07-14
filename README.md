# ugc_api

UGC ingestion API that validates incoming events and publishes them to Kafka.

## End-to-end tests

Functional tests exercise the running API against a real Kafka broker. They POST events over HTTP, assert on API responses, and consume published messages from Kafka to verify end-to-end delivery.

### Prerequisites

- Docker and Docker Compose
- Python 3.12+ with a virtual environment
- JWT test key pair in `tests/docker/certs/`

### Generate JWT test certificates

Create a local RSA key pair used by functional tests:

- `tests/docker/certs/jwt-private.pem` for minting test access tokens
- `tests/docker/certs/jwt-public.pem` mounted into the API container for verification

From `ugc_api/`:

- `mkdir -p tests/docker/certs`
- `openssl genrsa -out tests/docker/certs/jwt-private.pem 2048`
- `openssl rsa -in tests/docker/certs/jwt-private.pem -pubout -out tests/docker/certs/jwt-public.pem`

### Run tests

1. Install dependencies:
   - `pip install -r requirements-dev.txt`
2. Start test infrastructure:
   - `docker compose -f docker-compose.tests.yml up --build -d`
3. Run tests:
   - `pytest tests/functional -q`
4. Stop infrastructure:
   - `docker compose -f docker-compose.tests.yml down -v`

`docker-compose.tests.yml` starts three services:

- **kafka** — Apache Kafka 4.1.0 exposed on `localhost:29093`
- **kafka-init** — creates the `ugc-events-test` topic (6 partitions)
- **ugc-api** — the API container built from the project Dockerfile, exposed on `localhost:18000`, configured via `.env.tests`

Before each test session, the Kafka topic is cleared so tests start from an empty stream.

### Test layout

```
tests/functional/
├── conftest.py          # HTTP/Kafka fixtures and event builders
├── settings.py          # configurable test settings
└── testunits/events/
    ├── test_events.py       # happy-path ingestion for all event types
    └── test_validation.py   # invalid payload and malformed JSON rejection
```

### Coverage

**Happy path** (`test_events.py`) — parametrized over all supported event types:

- `click`
- `page_view`
- `movie_quality_changed`
- `movie_completed`
- `search_filter_used`

Each case verifies that `POST /api/v1/events` returns `202 Accepted` and that the event is published to Kafka with the user ID as the message key and the full payload in the value.

**Authentication** (`test_auth.py`) — verifies the API:

- rejects requests without an `Authorization` header (`401`)
- rejects invalid or expired tokens (`401`)
- rejects requests where `payload.user_id` does not match JWT `sub` (`403`)
- accepts valid tokens with matching `user_id` (`202`)

**Validation** (`test_validation.py`) — verifies the API rejects:

- unknown `event_type` values
- invalid UUIDs and timestamps
- incomplete payloads (missing required fields)
- malformed JSON bodies

### Test environment variables

The tests use defaults from `tests/functional/settings.py`, but can be overridden:

- `UGC_TEST_API_URL` (default `http://localhost:18000`)
- `UGC_TEST_API_PREFIX` (default `/api/v1`)
- `UGC_TEST_TIMEOUT` (default `10`)
- `UGC_TEST_KAFKA_BOOTSTRAP` (default `localhost:29093`)
- `UGC_TEST_KAFKA_TOPIC` (default `ugc-events-test`)
- `UGC_TEST_KAFKA_POLL_TIMEOUT` (default `1`)
- `UGC_TEST_KAFKA_WAIT_TIMEOUT` (default `15`)

The API container inside Docker Compose reads its own settings from `.env.tests` (Kafka bootstrap, topic name, producer options).
