# Infrastructure migration for redis-stream-player

## Why this notice exists

The shared development stands managed by k8s-infra are being removed in one
sitting. This project's migration is delayed. When the k8s-infra wipe is carried
out (plan phase P3), tests that need those stands will fail until migrated.
No wipe date is set. Shared data is destroyed without backups or imports, and
the old service credentials, helper scripts and run-tag model are retired.
This repository does not consume those shared stands, so the wipe itself is
not expected to break its current Redis tests. Its local Compose setup still
needs the migration described here.

## What is affected here

The plan lists no shared-stand dependency for redis-stream-player. Inspection
of its source, tests, integration scripts and instructions found no dependency
on the retired k8s-infra registry, run tags or storage paths.

- `docker-compose.yaml` supplies Redis on `localhost:6389` and RedisInsight
  on port 5540; this is the local Compose environment to replace.
- `tests/test_integration.py` connects to `localhost:6389` and skips when Redis
  is absent. That skip is not evidence that the migrated live tests pass.
- `integration/integration_test.sh` starts and stops Compose, uses a fixed
  Docker container identity, and verifies recording and playback against Redis.
- `integration/fill_redis.py` defaults to port 6389 for its test data.
- `README.md` and `AGENTS.md` describe the Compose prerequisite and test flow.

## Platform available after the reset

k8s-infra retains ingress, the local-CA certificate, Docker Desktop's default
`hostpath` storage class and one exclusive GPU slot, without time-slicing.
The certificate covers single-label hosts directly under `k8s.docker.internal`
only; it does not cover deeper project subdomains. Wildcard name lookup is
available only if phase P4 proves it. Until then, use `kubectl port-forward`.
Request `nvidia.com/gpu: 1` only for a test that needs the GPU.
The platform no longer provisions project dependencies or manages Compose.

## Namespace lifetime

The sweeper deletes every namespace outside k8s-infra's protected list 48 hours
after creation, even while active, with no renewal. It also deletes namespaces
created before the last PC or Docker Desktop restart. It runs every 30 minutes.
A missing `ResourceQuota` is reported but neither exempts a namespace nor
causes deletion by itself. Deletion is based on age or restart.
The plan keeps deletion disabled until real restart proofs and arming are done.

## Rules for this migration

1. Deploy test infrastructure into this project's own namespaces using its own
   manifests or chart and its own teardown.
2. Use a distinct namespace prefix, such as `<project>-it-<id>`. Never reuse
   `clearml`, `mongodb`, `ingress-nginx`, `infra-janitor`, `default` or `kube-*`.
3. Give every created namespace a project-chosen `ResourceQuota`. Migration is
   incomplete without quotas, checked through the sweeper report. Add a
   `LimitRange` as well: CPU or memory quotas reject pods without requests.
4. Omit `storageClassName` or use the default storage class. Prefer `emptyDir`
   for disposable data. Remove references to `local-path` and
   `/mnt/wsl/data/k8s-volumes`.
5. Finish runs within the namespace lifetime and make tooling recreate missing
   namespaces, since they can disappear at 48 hours or after a restart.
6. Current workloads must not use the dedicated ClearML or FiftyOne services.
   Deploy this project's own ClearML and FiftyOne database wherever needed.
   Dedicated services require an owner-issued bundle file; none is issued at
   the start.
7. Request `nvidia.com/gpu: 1` only where a test needs it; only one slot exists.
8. Remove `INFRA_RUN_TAG`, `INFRA_PROJECT`, `K8S_INFRA_SKILL_DIR` and
   `INFRA_REGISTRY`, plus calls to `infra.py`, `clearml.py`, `mongodb.py`,
   `cvat.py`, `lakefs.py` and `postgres.py`, from code, skills and shell profiles.
9. After P4 proves wildcard lookup, use a single-label host directly under
   `k8s.docker.internal` on the project's own Ingress with the platform
   certificate. Until then, and if P4 fails, use `kubectl port-forward`.

## Required outcome and workflow

Run against a private Redis deployed in this project's own namespace with a
quota and owned teardown, replacing Compose Redis on `localhost:6389`.
The migrated project must have no Compose dependency.
The project decides how to build that deployment.

Carry this through the repository's own planning and verification workflow in
`AGENTS.md`; it specifies planning separately from implementation, pytest and
the `uv run prek run --all-files` completion gate for code changes. It names
neither Spec Kit nor OpenSpec. Preserve real Redis integration and CLI coverage
against the new environment rather than accepting absent-service skips.

## Done when

The required outcome passes this repository's verification, the project owner
has been told it has moved, and the sweeper report shows its namespaces carrying
a quota. Then delete this file.

Plan: `/mnt/wsl/data/nkt/k8s-infra/docs/plans/2026-11-09-managed-infrastructure-reset.md`.
