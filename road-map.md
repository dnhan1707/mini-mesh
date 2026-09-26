# Mini-Mesh Roadmap

## Project Goal

Build a small, production-quality distributed telemetry platform inspired by the
architecture and operational concerns of F5 Distributed Cloud.

The goal is not to reproduce all of F5XC. The goal is to learn how to keep a
distributed system running, observable, recoverable, and understandable during
failure.

## Target Architecture

```text
CE agents
	|
	| gRPC telemetry
	v
Regional Edge (RE)
	|
	| gRPC stream
	v
Global Controller (GC)
	|
	| Kafka events
	v
Kafka cluster
	|
	| consumer group
	v
Telemetry Indexer
	|
	v
Elasticsearch
	|
	v
Query API / dashboard
```

## Operating Principles

- [ ] Define the responsibility of every component before implementing it.
- [ ] Prefer a small working system over many partially connected technologies.
- [ ] Treat Kafka as the durable event backbone.
- [ ] Treat Elasticsearch as a searchable projection, not the source of truth.
- [ ] Make failure behavior explicit and test it deliberately.
- [ ] Instrument the platform itself, not only the edge nodes.
- [ ] Document every reliability decision and its tradeoff.

## Phase 0: Define the System

- [ ] Define the telemetry event schema.
- [ ] Define what it means for telemetry to be accepted.
- [ ] Define what happens when CE, RE, GC, Kafka, or Elasticsearch is unavailable.
- [ ] Define the acceptable data-loss and data-delay behavior.
- [ ] Define the initial service-level objectives:
  - [ ] 99% of telemetry searchable within 30 seconds.
  - [ ] No accepted events lost during an Elasticsearch restart.
  - [ ] CE reconnects within 60 seconds after recovery.
  - [ ] Kafka consumer lag has a defined limit.
- [ ] Write down the first failure scenarios to test.

## Phase 1: Build a Single-Node Vertical Slice

Start with the smallest complete system. Do not use Kafka or Elasticsearch yet.

```text
CE -> GC -> in-memory storage -> query endpoint
```

- [x] Create the repository structure from scratch.
- [x] Define the protobuf telemetry contract.
- [x] Implement CE metric collection.
- [x] Implement CE-to-GC gRPC streaming.
- [x] Implement reconnect and exponential backoff.
- [x] Add context cancellation and graceful shutdown.
- [x] Add a GC health endpoint.
- [x] Add a GC readiness endpoint.
- [x] Add a basic query endpoint for the latest node state.
- [x] Add structured service logs.
- [x] Add unit tests for collection, streaming, and reconnect behavior.

### Phase 1 Acceptance Test

- [ ] Start CE and GC.
- [ ] Query telemetry through the API.
- [ ] Stop GC.
- [ ] Confirm CE reports or detects the failure.
- [ ] Restart GC.
- [ ] Confirm CE reconnects and telemetry resumes.

## Phase 2: Add the Regional Edge

```text
CE -> RE -> GC
```

- [x] Implement RE as a forwarding service.
- [x] Add separate CE-to-RE and RE-to-GC connection handling.
- [ ] Propagate cancellation and deadlines across both hops. (Cancellation propagates cleanly via context chains through `reconnect.Run`; per-event *deadlines* don't fit naturally on a long-lived client-streaming RPC — deadlines apply to the whole RPC, not individual sends. Left unaddressed rather than overclaimed.)
- [x] Define behavior when GC is unavailable. (RE buffers up to 500 events in a bounded queue, dropping oldest on overflow, and keeps retrying GC with backoff.)
- [x] Define behavior when RE is unavailable. (CE tries an ordered list of RE addresses each reconnect cycle, failing back to the first/"closest" address every attempt, with exponential backoff once the whole list is exhausted.)
- [x] Add correlation IDs to telemetry flow logs. (`event_id` is logged by CE on send, RE on receive/enqueue, RE on forward, and GC on receive — usable as the correlation key across all three services.)
- [x] Add metrics for active streams and forwarded events. (RE's `/debug/queue` HTTP endpoint reports `queue_depth`, `dropped_total`, `forwarded_total`; real Prometheus metrics land in Phase 6.)
- [x] Add tests for graceful disconnects and broken streams.

### Phase 2 Acceptance Test

- [x] Stop RE.
- [x] Confirm CE retries.
- [x] Restart RE.
- [x] Confirm RE reconnects to GC.
- [x] Confirm telemetry eventually reaches GC.
- [x] Confirm delayed and missing telemetry can be distinguished.

## Phase 3: Deploy the Services on Kubernetes

- [x] Create Kubernetes Deployments for CE, RE, and GC.
- [x] Create Services for each network boundary. (`gc`, `re` ClusterIP Services; CE has no listener, so no Service.)
- [x] Move runtime configuration into ConfigMaps. (`mini-mesh-config`: `GC_ADDRS`, `RE_ADDRS` using in-cluster DNS names.)
- [ ] Store credentials in Secrets. (No credentials exist yet — everything is insecure/plaintext gRPC. Revisit in Phase 9 security hardening.)
- [x] Add readiness probes. (GC and RE `/readyz`; CE has no HTTP listener so no probe is possible yet.)
- [x] Add liveness probes. (GC and RE `/healthz`; same CE caveat.)
- [x] Add resource requests and limits.
- [x] Add rolling update configuration. (`maxUnavailable: 0, maxSurge: 1` on all three Deployments.)
- [x] Add PodDisruptionBudgets where appropriate. (GC and RE, `minAvailable: 1` — intentionally blocks voluntary eviction until replicated further.)
- [x] Add graceful termination periods. (`terminationGracePeriodSeconds: 15`, backed by the Phase 2 bounded GracefulStop fix.)
- [ ] Document local development and deployment commands.

### Phase 3 Acceptance Test

- [x] Kill a service pod. (`kubectl delete pod -l app=re`; recreated in ~13s.)
- [x] Confirm Kubernetes recreates it.
- [x] Confirm clients reconnect. (CE logged single `WARN EOF` then recovered within ~2s.)
- [x] Perform a rolling update. (`kubectl rollout restart deployment/re`; new pod ready before old terminated, zero dropped-connection window beyond the same single EOF/retry.)
- [x] Confirm telemetry remains available during the update.
- [x] Reschedule a pod and verify recovery. (Scaled RE to 2 replicas, cordoned + drained `desktop-worker2`; PDB `minAvailable: 1` correctly blocked eviction until the 2nd replica existed. Evicted pod recreated on `desktop-worker`. Scaled back to 1, uncordoned node.)

## Phase 4: Introduce Kafka

```text
GC -> Kafka
```

- [ ] Define the telemetry topic and retention policy.
- [ ] Use a replication factor of 3.
- [ ] Configure `min.insync.replicas=2`.
- [ ] Partition by `node_id` to preserve per-node ordering.
- [ ] Add stable event IDs.
- [ ] Add event creation timestamps.
- [ ] Configure producer acknowledgements.
- [ ] Add producer success and failure metrics.
- [ ] Define behavior when Kafka is unavailable.
- [ ] Document topic creation and inspection commands.
- [ ] Verify records using a Kafka consumer.

### Phase 4 Acceptance Test

- [ ] Stop the future downstream consumer.
- [ ] Confirm GC can continue publishing to Kafka.
- [ ] Stop one Kafka broker.
- [ ] Confirm the cluster remains available.
- [ ] Restart the broker.
- [ ] Confirm telemetry continues without silent loss.
- [ ] Inspect offsets and verify records are present.

## Phase 5: Add the Elasticsearch Indexer

```text
Kafka -> Telemetry Indexer -> Elasticsearch
```

- [ ] Create a separate indexer service.
- [ ] Consume with a named Kafka consumer group.
- [ ] Deserialize telemetry events.
- [ ] Convert events into structured JSON documents.
- [ ] Define Elasticsearch mappings.
- [ ] Use bulk indexing.
- [ ] Commit Kafka offsets only after successful indexing.
- [ ] Retry transient Elasticsearch failures.
- [ ] Create a dead-letter topic for invalid events.
- [ ] Make indexing idempotent.
- [ ] Add index naming and retention policy.
- [ ] Add indexing success, failure, and latency metrics.
- [ ] Add consumer lag metrics.

### Phase 5 Acceptance Test

- [ ] Stop Elasticsearch.
- [ ] Confirm Kafka continues receiving telemetry.
- [ ] Observe indexer lag increasing.
- [ ] Restart Elasticsearch.
- [ ] Confirm the indexer catches up.
- [ ] Verify accepted events are eventually searchable.
- [ ] Send an invalid event and verify dead-letter handling.

## Phase 6: Observe the Platform

- [ ] Add Prometheus metrics to CE, RE, GC, and the indexer.
- [ ] Add Grafana dashboards.
- [ ] Add structured JSON logs.
- [ ] Add correlation IDs across services.
- [ ] Add request and processing latency measurements.
- [ ] Add alerts for:
  - [ ] Service unavailable.
  - [ ] Kafka publish failures.
  - [ ] Consumer lag too high.
  - [ ] Elasticsearch indexing failures.
  - [ ] Stale node telemetry.
  - [ ] Reconnect rate too high.
- [ ] Create a dashboard for node health.
- [ ] Create a dashboard for pipeline health.
- [ ] Document the first troubleshooting runbook.

### Phase 6 Acceptance Test

- [ ] Use the dashboards to identify a stopped service.
- [ ] Use metrics to distinguish ingestion failure from indexing delay.
- [ ] Use logs and correlation IDs to trace one event across the system.
- [ ] Trigger an alert intentionally and resolve it.

## Phase 7: Add Control-Plane Behavior

Move from observation only to a small desired-state system.

- [ ] Define desired state and reported state.
- [ ] Add a configuration version.
- [ ] Add a small control API.
- [ ] Propagate configuration from GC to RE and CE.
- [ ] Add acknowledgements.
- [ ] Add safe rollout behavior.
- [ ] Add rollback behavior.
- [ ] Add audit events for configuration changes.

Example configuration values:

- [ ] Telemetry sampling interval.
- [ ] Enabled collectors.
- [ ] Node labels.
- [ ] Maintenance mode.

## Phase 8: Security and Hardening

- [ ] Enable TLS for gRPC.
- [ ] Add Kafka authentication and encryption.
- [ ] Add Elasticsearch authentication.
- [ ] Move all credentials to Kubernetes Secrets.
- [ ] Add Kubernetes service accounts and RBAC.
- [ ] Add network policies.
- [ ] Validate and limit incoming payloads.
- [ ] Add rate limiting.
- [ ] Bound all internal queues and buffers.
- [ ] Review resource limits under load.
- [ ] Test backup and restore.
- [ ] Test certificate rotation.

## Final Failure-Test Matrix

- [ ] CE process stops.
- [ ] RE process stops.
- [ ] GC process stops.
- [ ] Kafka broker stops.
- [ ] Elasticsearch stops.
- [ ] Indexer stops.
- [ ] Network latency increases.
- [ ] Network partition occurs.
- [ ] Disk becomes full.
- [ ] Invalid telemetry is sent.
- [ ] A rolling deployment occurs during traffic.
- [ ] A node is rescheduled to another Kubernetes worker.

For every failure, record:

- [ ] What the user observes.
- [ ] What data is delayed, lost, or replayed.
- [ ] How the system recovers.
- [ ] Which metric detects the problem.
- [ ] Which alert fires.
- [ ] What an operator does to resolve it.

## Definition of Done

- [ ] The system has a documented architecture.
- [ ] Each service has a single clear responsibility.
- [ ] Telemetry survives downstream Elasticsearch outages through Kafka.
- [ ] Consumer lag and indexing health are visible.
- [ ] Services recover from restarts without manual data repair.
- [ ] Alerts identify the major failure modes.
- [ ] Runbooks explain how to diagnose and recover the system.
- [ ] Every major reliability claim has an executable test.
