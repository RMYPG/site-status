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

- `live`: `running` equals `target`, and every configured object exists.
- `failed`: `running` differs from `target` while a rollout signal holds.
- `deploying`: anything else.

| Object | Signal | Counts as failed |
| --- | --- | --- |
| Deployment | Rollout | `Progressing=False` with reason `ProgressDeadlineExceeded` |
| HelmRelease | Rollout | `Released=False` |
| The shared `ImageUpdateAutomation`, the Site's `Kustomization` | Rollout | `Ready=False` for longer than 10 minutes |
| The Site's `ImageRepository` and `ImagePolicy` | Discovery | `Ready=False` for longer than 10 minutes |

The 10 minutes are measured from the condition's `lastTransitionTime`. At exactly
10 minutes a condition does not count yet. `Ready=Unknown` is never a failure.

A rollout signal counts only while `running` differs from `target`. A discovery
signal means `target` has stopped moving, so comparing the two can't show it: it
never changes `phase`, and it sets `reason` whatever `phase` is. A Site with a
failing image scan therefore answers `live` with a `reason`.

When several signals hold at once, `reason` lists them all, separated by `; `,
rollout signals first.

### A Site the service can't fully answer for

A configured Site whose objects don't exist yet, or whose `ImagePolicy` has not
resolved a tag, gets a normal `200` response. It is never `live`:

```json
{ "target": null, "running": "v1.0.2", "phase": "deploying", "reason": "ImagePolicy flux-system/fisioindoor-production has no resolved tag" }
```

- `target` is `null` when the `ImagePolicy` is missing or has no resolved tag.
- `running` is `null` when the Deployment is missing, no ReplicaSet has available
  replicas, or the running image has no tag. `null` is never equal to anything,
  so a Site with an unknown version isn't `live`.
- `phase` is `deploying`, or `failed` if a rollout signal holds while the versions
  differ.
- `reason` names what is missing. It stays `null` when `running` is only null
  because no ReplicaSet has available replicas, as during a pod's restart: that is
  a normal moment of a single-replica Site, not a failure.

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
| `503` | The watches have not finished their first sync. | Treat it as no answer and ask again. |

A `404` or `503`, like a connection failure, says nothing about the rollout. It
must not make a Publish `failed`.

Error bodies are `{ "error": "message" }`.

## Probes

| Path | Answers |
| --- | --- |
| `GET /healthz` | Always `200 {"status":"ok"}`. |
| `GET /readyz` | `503 {"status":"syncing"}` until every watch has completed its first sync, then `200 {"status":"ok"}`. |
