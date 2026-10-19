# Tech Stack — Campus Sensor Monitoring and Alerts

## 1. Overview

This project uses a Python-based backend stack to ingest campus sensor readings, evaluate alert rules, and allow organizers to manage alerts. The system must support individual and batch ingestion, single-reading and time-window rules, a sensor simulator, and load testing for a burst of **10,000 readings in one second**.

The stack is selected to balance development speed, maintainability, data integrity, and the ability to measure and improve throughput.

## 2. Agreed Technology Stack

| Layer | Technology | Purpose |
|---|---|---|
| Programming language | Python | Main backend and worker language |
| API framework | FastAPI | REST API for readings, rules, and alerts |
| ASGI server | Uvicorn | Runs the FastAPI application |
| Request/data validation | Pydantic | Validates readings, batches, and rule configurations |
| Primary database | PostgreSQL | Persists readings, rules, alerts, and alert history |
| Database toolkit | SQLAlchemy | Database models and data access |
| Schema migrations | Alembic | Versioned database schema changes |
| Event buffering | Redis Streams | Buffers ingestion events and distributes them to workers |
| Background processing | Python worker | Evaluates rules and processes readings asynchronously |
| Automated testing | pytest | Unit and integration tests |
| Load testing | Locust | Measures throughput, latency, failures, and behavior under bursts |
| Local environment | Docker Compose | Runs the API, worker, PostgreSQL, and Redis consistently |
| Version control | Git and GitHub | Source control and collaboration |

## 3. Architecture

```text
Sensors / Sensor Simulator
          |
          v
FastAPI Ingestion API
  - Validate requests
  - Accept individual readings and batches
  - Publish accepted events
          |
          v
Redis Streams
  - Buffer events
  - Track pending messages
          |
          v
Python Background Worker
  - Persist readings
  - Evaluate single-reading rules
  - Evaluate time-window rules
  - Create or update alerts
          |
          v
PostgreSQL
  - Sensor readings
  - Alert rules
  - Alerts and lifecycle history

Organizers use FastAPI endpoints to manage rules and
view, acknowledge, and resolve alerts.
```

The API and worker can live in the same repository and share application code, while running as separate processes. This keeps the project straightforward without requiring microservices.

## 4. Why This Stack

### Python and FastAPI
- Python supports rapid development and is already familiar to the project developer.
- FastAPI provides typed request handling, validation integration, asynchronous endpoint support, and automatic interactive API documentation.
- Compared with Flask, it provides more API-oriented functionality out of the box.
- Compared with Django, it has less framework overhead for a backend that primarily exposes APIs.
- Node.js and Java with Spring Boot are viable alternatives, but switching languages or frameworks is not necessary for this project.

### PostgreSQL
- Sensor readings, rules, and alerts have clear relationships.
- Transactions and constraints help maintain consistent alert state.
- Indexes and SQL aggregation support time-based queries and window calculations.
- MySQL would also work, but PostgreSQL is a strong fit and no switch is needed.
- MongoDB's flexible document model is not a compelling advantage for this predominantly structured, relational workload.
- SQLite is useful for small prototypes but is not the selected database for concurrent burst-load testing.

### Redis Streams
- Provides an event stream and consumer groups for background processing.
- Helps decouple request ingestion from rule evaluation and database work.
- Can absorb short bursts when consumers temporarily process more slowly than events arrive.
- It is simpler to operate for this project than introducing Kafka immediately.
- RabbitMQ is a valid alternative if more advanced message routing is needed. Kafka can be reconsidered if measured scale, replay requirements, or independent consumers justify the added operational complexity.

### SQLAlchemy and Alembic
- SQLAlchemy centralizes database access and supports both ORM-based operations and explicit SQL when needed.
- Alembic keeps schema changes versioned and repeatable across development and evaluation environments.

### pytest and Locust
- pytest verifies validation, rule evaluation, alert lifecycle, and failure behavior.
- Locust measures the actual request rate, latency, errors, and behavior during the target burst.

### Docker Compose
- Makes local setup repeatable by defining the API, worker, PostgreSQL, and Redis services together.
- Kubernetes is not required for the initial implementation.

## 5. Key Design Rules

1. **Do not treat the stack as a performance guarantee.** The 10,000-readings-per-second target must be measured on a documented test environment.
2. **Use batch writes where appropriate.** Avoid a separate database transaction for every reading if it prevents the system from meeting its target.
3. **Separate ingestion from processing.** Keep expensive rule evaluation out of the critical request path where the chosen durability contract allows it.
4. **Define when ingestion is considered successful.** Do not report durable acceptance before the event has reached the agreed durability boundary.
5. **Acknowledge stream messages only after successful processing and database commit.** Implement retries and idempotency so a worker restart does not silently lose events or create uncontrolled duplicates.
6. **Configure Redis persistence deliberately.** Redis Streams alone does not guarantee survival of every event under every failure configuration.
7. **Index for real queries.** Likely candidates include sensor ID, metric type, timestamp, rule ID, and alert status. Confirm indexes with query plans and measurements.
8. **Keep connection pools bounded.** Do not create a new database connection for every request.
9. **Make time-window semantics explicit.** Define rolling versus fixed windows, late data behavior, and when an alert should be emitted.
10. **Optimize from measurements.** Introduce extra infrastructure only when a demonstrated bottleneck or requirement justifies it.

## 6. Initial Functional Responsibilities

- **FastAPI:** Individual and batch reading endpoints; rule CRUD and enable/disable operations; alert listing, acknowledgement, and resolution; health endpoint.
- **Pydantic:** Input validation and clear validation errors.
- **Redis Streams:** Event buffering and consumer-group delivery to workers.
- **Python worker:** Reading persistence, single-reading rule evaluation, time-window evaluation, and alert creation/update.
- **PostgreSQL:** Durable storage for readings, rules, alerts, and relevant lifecycle timestamps.
- **Sensor simulator:** Generates configurable normal readings and threshold breaches, with individual and batch modes.
- **pytest:** Tests expected behavior and edge cases.
- **Locust:** Tests ingestion performance and helps verify the 10,000-readings-in-one-second target.

## 7. Initial Scope Decisions

The first implementation will **not** introduce the following unless requirements or measurements justify them:

- Kafka
- Kubernetes
- A separate time-series database
- Microservices split across multiple repositories
- A separate caching layer
- Celery or another task queue in addition to Redis Streams
- A frontend dashboard
- A notification provider for email, SMS, or push notifications

The API's interactive documentation can be used for initial demonstrations and manual testing.

## 8. Definition of Stack Readiness

The stack is ready for feature development when:

- The FastAPI service starts successfully.
- PostgreSQL is available and schema migrations run.
- Redis Streams is available and a test event can be published and consumed.
- The Python worker can process a test event and persist the result.
- pytest runs successfully.
- Docker Compose provides a documented local setup.
- Configuration and secrets are supplied through environment variables, not committed to Git.

## 9. Performance Validation

The required performance test is a **burst of 10,000 readings in one second**. The test report should include:

- Test environment and versions
- Load-test configuration and request shape
- Number of readings attempted, accepted, rejected, and failed
- Achieved throughput
- Request latency and percentiles where available
- Rule-processing and alert delay
- Evidence of persisted data and expected alerts
- CPU, memory, database, and stream/consumer utilization where available
- Bottlenecks, tuning performed, and known limitations

A one-second burst test does not prove that the system can sustain 10,000 readings per second indefinitely. Report the tested workload precisely and do not claim the target is met until the results support that conclusion.

## 10. Final Decision

**Agreed stack:** Python + FastAPI + Uvicorn + Pydantic + PostgreSQL + SQLAlchemy + Alembic + Redis Streams + Python background worker + pytest + Locust + Docker Compose + Git/GitHub.

Implement the simplest complete version with this stack first. Add infrastructure only when testing demonstrates a concrete need.
