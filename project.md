# Project Plan — Campus Sensor Monitoring and Alerts

## Objective
Build a reliable backend that ingests campus sensor readings, evaluates organizer-defined alert rules, and allows organizers to manage generated alerts. The system must support individual and batch ingestion, single-reading and time-window rules, a sensor simulator, and a load-test report targeting a burst of **10,000 readings in one second**.

## Phase 1 — Requirements and Technical Design
**Goal:** Turn the problem statement into a clear, testable specification.

### Tasks
- Define the sensor-reading schema: `sensorId`, `type`, `value`, `timestamp`, and optional metadata.
- Specify input validation, timestamp handling, supported metric types, and batch error behavior.
- Define alert-rule fields, comparison operators, thresholds, optional time windows, aggregation methods, and enabled/disabled status.
- Define alert statuses: `OPEN`, `ACKNOWLEDGED`, and `RESOLVED`.
- Decide how duplicate submissions, repeated matches, retries, and late or out-of-order readings should behave.
- Sketch the API endpoints, database schema, and data flow.
- Confirm the agreed technology stack and write acceptance criteria.

### Deliverables
- Requirements and design notes
- API endpoint list
- Database schema draft
- Acceptance checklist

**Exit criteria:** Each requirement has a design decision and a way to verify it.

## Phase 2 — Project Setup and Core APIs
**Goal:** Establish a runnable foundation and implement the main data-management APIs.

### Tasks
- Set up the repository, folder structure, dependencies, environment variables, and local development configuration.
- Configure FastAPI, PostgreSQL, Redis Streams, migrations, structured logging, error handling, and a health endpoint.
- Create data models and database schema for readings, rules, and alerts.
- Implement individual and batch reading ingestion with validation and documented error behavior.
- Implement rule endpoints to create, list, view, edit, enable, and disable rules.
- Implement alert endpoints to list and view alerts, acknowledge them, and resolve them.
- Add pagination, useful filters, and indexes for common queries.

### Deliverables
- Runnable service and local environment
- Database schema and migrations
- Reading-ingestion API
- Rule-management API
- Alert-management API
- Initial setup and run instructions

**Exit criteria:** The service starts from the documented setup, persists data, and core endpoints correctly accept valid requests and reject invalid ones.

## Phase 3 — Rule Engine and Alert Reliability
**Goal:** Implement both kinds of rule evaluation and make alert handling predictable.

### Tasks
- Implement single-reading comparisons, including greater-than and less-than thresholds.
- Match readings to applicable enabled rules and create or update alerts when rules match.
- Implement time-window evaluation, starting with averages over a configurable duration.
- Define window boundaries and behavior for late, out-of-order, and duplicate readings.
- Avoid repeatedly scanning historical data when indexed queries or rolling aggregates are more appropriate.
- Define and enforce valid alert status transitions.
- Record acknowledgement and resolution timestamps.
- Implement practical idempotency and duplicate-alert handling.
- Handle processing retries, database failures, and partial batch failures without silently losing accepted events.
- Add unit and integration tests for thresholds, disabled rules, multiple matches, window boundaries, and retries.

### Deliverables
- Single-reading rule engine
- Time-window rule evaluation
- Alert lifecycle and deduplication behavior
- Automated tests for core rules and failure cases

**Exit criteria:** Known input sequences consistently produce expected alerts; disabled rules do not trigger alerts; retries do not create uncontrolled duplicates or silently lose accepted data.

## Phase 4 — Sensor Simulator and Performance Testing
**Goal:** Exercise the backend with realistic workloads and measure its performance.

### Tasks
- Build a configurable simulator for sensor IDs, metric types, values, timestamps, request rate, duration, and batch size.
- Support individual and batch submissions.
- Generate normal readings and deliberate threshold breaches; make runs reproducible where practical.
- Report attempted, accepted, rejected, and failed readings.
- Establish a baseline and test a burst targeting 10,000 readings in one second.
- Record throughput, request latency, error rate, persisted reading counts, alert-processing delay, and resource usage where available.
- Inspect bottlenecks such as database indexes, connection pools, write contention, consumer lag, and CPU/memory usage.
- Make measured changes one at a time and repeat comparable tests.
- Verify correctness under load, not just whether requests were accepted.

### Deliverables
- Sensor simulator and usage examples
- Load-test scripts or configuration
- Performance measurements and tuning notes

**Exit criteria:** The simulator covers normal and alert-triggering cases, and load-test results show whether the target was achieved in the tested environment.

## Phase 5 — Integration, Security, and Recovery Testing
**Goal:** Verify the complete system behaves correctly under normal and failure conditions.

### Tasks
- Run end-to-end tests from ingestion through rule evaluation to alert acknowledgement and resolution.
- Test malformed input, large batches, duplicate requests, database unavailability, worker restarts, and recovery behavior.
- Acknowledge stream messages only after successful processing and the relevant database changes have committed.
- Check retry and idempotency behavior to avoid silent data loss and uncontrolled duplicate alerts.
- Apply sensible request and batch-size limits.
- Ensure secrets are supplied through environment variables and are not committed to the repository.
- Add authentication and authorization if required by the project scope; document the controls actually implemented.
- Verify installation and execution from a clean environment.
- Record known limitations and remove unused code.

### Deliverables
- End-to-end test results
- Failure and recovery test results
- Example configuration without secrets
- Known limitations list

**Exit criteria:** The full workflow succeeds from a clean setup, and tested failure scenarios behave according to documented reliability guarantees.

## Phase 6 — Documentation and Final Demonstration
**Goal:** Package the project so an evaluator can run it, verify its behavior, and understand its design.

### Tasks
- Finalize the README with prerequisites, configuration, database setup, migrations, startup, and test commands.
- Document API endpoints with example requests and responses.
- Explain the architecture and important technical decisions.
- Document the simulator, example rules, and load-test procedure.
- Write a load-test report with the environment, workload, measured results, optimizations, and limitations.
- Prepare a demonstration covering:
  1. Start the service.
  2. Create and enable an alert rule.
  3. Submit normal readings.
  4. Submit a threshold-breaching reading.
  5. Inspect the generated alert.
  6. Acknowledge and resolve the alert.
  7. Demonstrate a time-window rule.
  8. Run the simulator or load test.

### Deliverables
- Complete source code
- Installation and execution instructions
- API documentation
- Load-test report
- End-to-end demonstration checklist

**Exit criteria:** An evaluator can set up, run, test, and understand the project using the submitted materials.

## Milestones
- **Milestone A:** Service starts and stores sensor readings.
- **Milestone B:** Organizers can manage rules and alerts.
- **Milestone C:** Single-reading and time-window rules produce expected alerts.
- **Milestone D:** Simulator exercises normal and alert-triggering workloads.
- **Milestone E:** Reliability and load tests produce documented results.
- **Milestone F:** Clean setup and final demonstration succeed.

## Final Checklist
- [ ] Runnable source code and clear setup instructions
- [ ] Example environment configuration without secrets
- [ ] Database schema and migrations
- [ ] Individual and batch ingestion
- [ ] Rule creation, editing, enabling, and disabling
- [ ] Single-reading threshold evaluation
- [ ] Time-window aggregation and evaluation
- [ ] Alert listing, acknowledgement, and resolution
- [ ] Duplicate handling, retries, and failure tests
- [ ] Sensor simulator
- [ ] Automated and end-to-end tests
- [ ] Load test targeting 10,000 readings in one second
- [ ] Report with measured performance and limitations
- [ ] Demonstration instructions

## Scope Control
Prioritize a complete, testable backend over optional features. Do not add dashboards, complex authentication, additional metric types, or distributed infrastructure unless required or justified by measurements. Introduce queues, caches, or stream-processing components because reliability and performance requirements justify them, not merely to increase architectural complexity.
