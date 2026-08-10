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

KNEMON is bundled in the Mnemos umbrella image (`ghcr.io/ncz-os/mnemos`) and the
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

Apply `0040_knemon_burn_reservations.sql` for the active database backend before
deploying this release. Routing and ledger writes fail closed with an actionable
error when the tables are absent. Development and single-worker deployments can
temporarily opt into the legacy non-atomic behavior with
`MNEMOS_KNEMON_ALLOW_NONATOMIC_BURN_DEVELOPMENT=true`; this setting is not safe
for production or multiple workers.


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
