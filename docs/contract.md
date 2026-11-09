# HTTP contract

`edity-server` reads this service to learn how a Site's rollout is going
([RMYPG/platform#142](https://github.com/RMYPG/platform/issues/142), Edity's
ticket 33). The service stores nothing: every answer is worked out from the
cluster when the request arrives.

All responses are JSON and carry `Cache-Control: no-store`. There is no
authentication; a NetworkPolicy limits who can reach the port.

## `GET /v1/sites/{site}/environments/{environment}`

`site` is the Site's id and `environment` is the name configured for it, such as
`production`.

```json
{ "target": "v1.0.3", "running": "v1.0.2", "phase": "deploying", "reason": null }
```

| Field | Type | Meaning |
| --- | --- | --- |
| `target` | string or null | The tag the Site's `ImagePolicy` selected (`status.latestRef.tag`). |
| `running` | string or null | The tag of the newest of the Deployment's own ReplicaSets with available replicas. |
| `phase` | `live`, `deploying` or `failed` | See below. |
| `reason` | string or null | What failed, for the Developer. Can be set while `phase` is `live`. |

### Phases

- `live`: `running` equals `target`, and both are known.
- `failed`: not `live`, and a rollout signal holds.
- `deploying`: anything else.

| Object | Signal | Counts as failed |
| --- | --- | --- |
| Deployment | Rollout | `Progressing=False` with reason `ProgressDeadlineExceeded` |
| HelmRelease | Rollout | `Released=False` |
| The shared `ImageUpdateAutomation`, the Site's `Kustomization` | Rollout | Not Ready for longer than 10 minutes, as below |
| The Site's `ImageRepository` and `ImagePolicy` | Discovery | Not Ready for longer than 10 minutes, as below |

These four objects retry on their own, so what counts is a continuous stretch in
which `Ready` isn't `True`. Flux's `lastTransitionTime` can't measure that: it is
rewritten whenever `Ready`'s status, reason or message changes, and controllers
can pass through `Ready=Unknown` on each attempt. The service tracks the stretch
itself:

- It starts when the service first sees `Ready` not `True`, from the condition's
  `lastTransitionTime` if it has no record of the object being not Ready.
- `False` and `Unknown` both continue it, and `True` ends it.
- The signal counts once the stretch has lasted longer than 10 minutes (exactly
  10 minutes isn't enough) and `Ready=False` has been seen at least once during it.
- Once counting, it keeps counting while the current value is `Unknown`, so the
  phase doesn't flip between attempts.
- An object that has only ever been `Unknown` is not failing.

The stretch lives in memory, so **a restart restarts the wait**: after a restart
the service can only seed it from a `lastTransitionTime` that Flux has probably
just rewritten, so a Site that was already `failed` may report `deploying` again
for up to 10 minutes.

One accepted limit: the stretch is followed through events that can lag a moment
behind the service's cache. If `Ready` goes `False`, `True`, `False` inside that
lag, the earlier stretch carries on, so a waiting Site can report `failed` before
a fresh 10 minutes have passed. The object really is failing at that moment, so
the only effect is reporting it early.

`Released=False` and `ProgressDeadlineExceeded` have no grace period and are not
cleared by a new `target`. They stay until a rollout succeeds, so when a Site's
earlier rollout has failed, the next `target` reports `failed` right away, and
keeps doing so until a rollout succeeds.

A rollout signal counts only while `running` differs from `target`. A discovery
signal means `target` has stopped moving, so comparing the two can't show it: it
never changes `phase`, and it sets `reason` whatever `phase` is. A Site with a
failing image scan therefore answers `live` with a `reason`.

When several signals hold at once, `reason` lists them all, separated by `; `,
rollout signals first.

### A Site the service can't fully answer for

A configured Site whose objects don't exist yet, or whose `ImagePolicy` has not
resolved a tag, gets a normal `200` response. It is `live` only if both versions
are still known and equal:

```json
{ "target": null, "running": "v1.0.2", "phase": "deploying", "reason": "ImagePolicy flux-system/fisioindoor-production has no resolved tag" }
```

- `target` is `null` when the `ImagePolicy` is missing or has no resolved tag.
- `running` is `null` when the Deployment is missing, no ReplicaSet has available
  replicas, or the running image has no tag. `null` is never equal to anything,
  so a Site with an unknown version isn't `live`.
- `phase` is `deploying`, or `failed` if a rollout signal holds while the versions
  differ.
- `reason` names what is missing. A missing object that leaves both versions known
  and equal, such as a `HelmRelease`, goes in `reason` only and the Site stays
  `live`.
- `reason` stays `null` when `running` is only null because no ReplicaSet has
  available replicas, as during a pod's restart: that is a normal moment of a
  single-replica Site, not a failure.

**For a consumer comparing versions:** treat a `null` `target` or `running` as
older than every version. Ticket 33's rules then apply unchanged.

## `GET /v1/sites/{site}/environments/{environment}/rollouts`

```json
{
  "rollouts": [
    { "tag": "v1.0.3", "createdAt": "2026-11-09T12:00:00Z" },
    { "tag": "v1.0.2", "createdAt": "2026-10-07T09:30:00Z" }
  ]
}
```

The retained ReplicaSets, newest first, with one entry per run of the same image.
`createdAt` is the creation time of the oldest ReplicaSet in that run, which is
when that image first rolled out. `tag` is `null` if the image has no tag. The
list is empty if the Deployment doesn't exist or has no ReplicaSets.

These limits follow from reading ReplicaSets and storing nothing:

- An entry shows that a rollout started, not that it finished.
- A rollback reuses the old ReplicaSet and keeps its original time.
- History goes with the Deployment.

## Status codes

| Code | When | A consumer should |
| --- | --- | --- |
| `200` | The answer above. | Use it. |
| `404` | The Site or environment isn't configured, or the path doesn't exist. | Fix the configuration or the request. |
| `405` | A method other than `GET` or `HEAD`. | Use `GET`. |
| `503` | The watches have not finished their first sync, or a watch or the periodic check of the API server failed within the last 2 minutes, so an answer could be stale. | Treat it as no answer and ask again. |

A `404` or `503`, like a connection failure, says nothing about the rollout. It
must not make a Publish `failed`.

Error bodies are `{ "error": "message" }`.

## Probes

| Path | Answers |
| --- | --- |
| `GET /healthz` | Always `200 {"status":"ok"}`. |
| `GET /readyz` | `503 {"status":"syncing"}` until every watch has completed its first sync. After that, `503 {"status":"watch failing"}` while reads keep failing (the last failure was under 2 minutes ago), and otherwise `200 {"status":"ok"}`. |

A failure here is a watch error client-go reports, such as Forbidden, or a failure
of the service's own check: every 30 seconds it lists Deployments in one configured
namespace, with a limit of 1 and a 10-second timeout. The check is needed because
client-go hides many failures while opening a watch: it retries `connection refused`
and 429, and turns a timeout, connection reset or GOAWAY into an empty watch. A list
returns all of those as errors. `GET /healthz` is unaffected.
