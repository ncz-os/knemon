# mnemos-knemon

KNEMON is the Mnemos **budget / cost plane** — it meters spend, enforces budgets,
and routes work by cost tier across the LLM and reasoning surfaces. It is a
separately installable `mnemos.*` namespace distribution (PEP 420) that overlays
onto `mnemos-core`.

## What's inside

- **Cost router** (`mnemos.domain.knemon.router`): tier-aware dispatch that picks
  a provider/model by price/latency/quality budget rather than a hardcoded
  default.
- **Budget engine** (`mnemos.domain.knemon.budget`): per-scope spend tracking and
  enforcement (caps, soft warnings, rollover windows).
- **Ledger + utilization API** (`mnemos.api.routes.ledger`,
  `…routes.knemon_utilization`): records spend events and exposes utilization
  against budgets.
- **Router + dashboard API** (`mnemos.api.routes.knemon_router`,
  `…routes.knemon_dashboard`): the routing decision endpoint and an operator
  dashboard surface.

## Install

```bash
pip install mnemos-core mnemos-knemon
```

KNEMON is bundled in the Mnemos umbrella image (`ghcr.io/ncz-os/mnemos-enterprise`) and the
`server`/`full` bundles, and is separable for minimal core installs. When the
distribution is present, `mnemos-core` mounts the KNEMON routes automatically;
when absent, core boots without them.

## Relationship to core

KNEMON depends on `mnemos-core` (one direction only). It pairs naturally with
`mnemos-pantheon` (the LLM facade KNEMON routes across) and `mnemos-graeae` (the
reasoning bus whose consultations KNEMON budgets), but does not require them —
each layer is independently installable.

## Atomic session-burn accounting

KNEMON routes reserve one request in the database before returning a decision.
The route response includes `reservation_id` and `reservation_expires_at` as
additive fields. Execution callers should pass `reservation_id` to the matching
`POST /v1/ledger` request. That write records actual usage and marks the
reservation reconciled in one transaction. Older callers that omit the token
remain compatible: the ledger endpoint claims the oldest pending reservation
for the same session. Abandoned reservations stop counting after
`MNEMOS_KNEMON_BURN_RESERVATION_TTL_SECONDS`, which defaults to the configured
session-burn window.

The wheel includes backend-specific `0052_knemon_burn_reservations.sql` files.
KNEMON applies the applicable idempotent migration before the first atomic route
or ledger write, including when upgrading an already-populated database.
Terminal and long-expired reservations are purged during routing after seven
days by default. Configure that audit window with
`MNEMOS_KNEMON_BURN_RESERVATION_RETENTION_SECONDS` and the cleanup cadence with
`MNEMOS_KNEMON_BURN_RESERVATION_CLEANUP_INTERVAL_SECONDS`. Development and
single-worker deployments can temporarily opt into the legacy non-atomic
behavior with `MNEMOS_KNEMON_ALLOW_NONATOMIC_BURN_DEVELOPMENT=true`; this
setting is not safe for production or multiple workers.


## Backend coverage

KNEMON's ledger, budget engine and reservation accounting run through core's
persistence ABC on every backend, with per-dialect SQL where a portable form
does not exist.

`GET /v1/knemon/recommendations` returned **HTTP 500 on PostgreSQL** while
working on the other five backends: the `model_registry` lookup in
`knemon_dashboard.py` hardcoded an integer-family predicate
(`WHERE available = 1 AND COALESCE(deprecated, 0) = 0`), and core declares those
columns `BOOLEAN` on PostgreSQL but `INTEGER` on SQLite, `NUMBER(1)` on Oracle
and `SMALLINT` on Db2. PostgreSQL has no implicit boolean-to-integer cast, so
the query raised `operator does not exist: boolean = integer`. Fixed by routing
the predicate through the existing `_dialect_rows()` per-dialect helper pattern;
only the PostgreSQL branch changed. A single portable form does not exist — a
bare `WHERE available` is valid on PostgreSQL but rejected by Oracle
`NUMBER(1)` and Db2 `SMALLINT` — so the dialect branch is required.

It survived review because the query had **zero test coverage on any backend**;
the only live-PostgreSQL test creates `usage_ledger` and never `model_registry`,
so CI's postgres service never reached it. The predicate shape is now asserted
for all five dialects, and the non-PostgreSQL branch is executed for real
against a SQLite `model_registry`.

## Build infrastructure & partners

Continuous integration and package distribution for this project are generously
supported by our open-source infrastructure partners:

- **[GitLab](https://gitlab.com/)** — canonical source hosting and CI pipelines
  (format / lint / test gates), via the
  [GitLab for Open Source](https://about.gitlab.com/solutions/open-source/) program.
- **[Buildkite](https://buildkite.com/)** — CI/CD orchestration with hosted macOS
  and Linux agents, and our APT package registry host
  (`packages.buildkite.com/ncz-os/ncz`), via the
  [Buildkite Open Source](https://buildkite.com/pricing) program.

Thank you to both for backing open-source software.


## Monetary attempt reservations

Python 3.13 or newer is required. Development dependency resolution uses the
real `../mnemos-core` checkout; it never substitutes a metadata-only core.

PANTHEON and GRAEAE use `mnemos.domain.knemon.attempts.Attempt` at the actual
upstream boundary. Each retry, fallback, and consultation participant has its
own reservation. Global and provider caps are enforced within the caller
subsystem (`pantheon` or `graeae`); these are separate accounting scopes, not an
account-wide sum across every MNEMOS component. Aliases in both configured caps
and recorded providers normalize to one key; duplicate alias caps use the
smaller amount. Provider/model prices must be known when a cap is configured.

Admission locks a durable subsystem row before reading committed plus pending
exposure and inserting a reservation. Ledger append and settlement share one
transaction. Unlinked settlement retains cost if ledger recording fails.
Repeated finalization is idempotent, changed attempt payloads are rejected, and
expired attempts remain counted. Recovery settles at the reserved estimate,
with provenance so authoritative late usage can correct it. Recovery runs at
most every 30 seconds per active backend during provider admission and handles
up to 500 expired attempts per subsystem per pass. Terminal history is retained
for at least the seven-day budget window. A known pre-dispatch abort can use
`release_cost`; ambiguous provider failures must be settled conservatively.

Migration `0056_knemon_cost_recovery.sql` adds recovery provenance and rolling
spend indexes for PostgreSQL, SQLite, MySQL/MariaDB, Oracle, and Db2. SQLite
compares parsed dates so ISO timestamps using either `T` or a space have the
same semantics. PostgreSQL and SQLite are exercised with live disposable
engines; the other dialects have SQL-shape/migration checks, not live qualification.

The aggregate table is a reporting snapshot. Hard admission uses indexed
seven-day SUM queries plus pending exposure, not a stale cached balance. Work
still grows with activity in that window and subsystem admissions serialize;
large installations should measure lock wait and query latency before choosing
partitioning or a bucketed accounting design. This release makes no O(1)
admission guarantee.
