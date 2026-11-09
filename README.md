# site-status

Read-only rollout status for RMYPG Sites.

`edity-server` asks this service whether production runs a Release, a newer one,
or has failed to roll one out. The service watches a few objects in the cluster
and answers from them. It stores nothing and writes nothing to the cluster, so a
restart or a missed watch event corrects itself.

[RMYPG/platform#142](https://github.com/RMYPG/platform/issues/142) owns the
requirements. The HTTP API is in [docs/contract.md](docs/contract.md).

## Configuration

The service reads one JSON file at startup. Give its path with `-config` or
`SITE_STATUS_CONFIG`; the flag wins. It listens on `:8080`, or on `-listen` or
`SITE_STATUS_LISTEN`. The configuration is not reloaded while it runs.

[`config.example.json`](config.example.json) lists production's three Sites:

```json
{
  "imageUpdateAutomation": "image-tags",
  "sites": [
    {
      "id": "fisioindoor",
      "environments": {
        "production": {
          "namespace": "fisioindoor",
          "deployment": "fisioindoor-sws",
          "helmRelease": "fisioindoor",
          "imagePolicy": "fisioindoor-production",
          "imageRepository": "fisioindoor",
          "kustomization": "fisioindoor"
        }
      }
    }
  ]
}
```

| Key | Meaning |
| --- | --- |
| `imageUpdateAutomation` | The `ImageUpdateAutomation` all Sites share, in `flux-system`. |
| `sites[].id` | The Site's id in the URL. |
| `sites[].environments` | Keyed by the environment's name in the URL. |
| `namespace` | Where the Deployment and HelmRelease live. |
| `deployment`, `helmRelease` | Names in `namespace`. |
| `imagePolicy`, `imageRepository`, `kustomization` | Names in `flux-system`. |

Every key is required. The service refuses to start on an unknown key, a
duplicate key, a duplicate Site or environment, an environment that repeats
another's Deployment, HelmRelease or ImagePolicy, or an invalid name. An
`ImageRepository` or `Kustomization` may be shared between environments.

Only the `production` environment exists today, but it is a name in the
configuration and the URL, not something the code treats differently.

## Kubernetes access

The service authenticates only with its pod's ServiceAccount token, and uses only
`get`, `list` and `watch`. It watches each configured namespace separately and
never lists or watches across the cluster, because its permissions are granted
per namespace. Anything less than the table below keeps it unready: the first
list fails and is retried, and `/readyz` answers `503` until it succeeds.

After the first sync, the service also has to notice when its cache stops
following the cluster. It reads two things: the errors client-go reports from
watches, such as Forbidden, and a check of its own, a `list` of Deployments in one
configured namespace every 30 seconds, with a limit of 1 and a 10-second timeout
(no extra RBAC). The check is needed because client-go hides many failures while
opening a watch: it retries `connection refused` and 429, and turns a timeout,
connection reset or GOAWAY into an empty watch, so a network block would otherwise
go unseen. Any failure of either is logged, and `/readyz` and the API answer `503`
until 2 minutes after the last one.

| Resources | Where |
| --- | --- |
| `deployments`, `replicasets` (`apps`); `helmreleases` (`helm.toolkit.fluxcd.io`) | Each namespace the service reports on: one ClusterRole, granted by a RoleBinding per namespace |
| `imagerepositories`, `imagepolicies`, `imageupdateautomations` (`image.toolkit.fluxcd.io`); `kustomizations` (`kustomize.toolkit.fluxcd.io`) | `flux-system`, through its own Role |

It needs no access to Secrets, ConfigMaps or Pods, no write verb and no `leases`.

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: ClusterRole
metadata:
  name: site-status-namespace
rules:
  - apiGroups: [apps]
    resources: [deployments, replicasets]
    verbs: [get, list, watch]
  - apiGroups: [helm.toolkit.fluxcd.io]
    resources: [helmreleases]
    verbs: [get, list, watch]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
  name: site-status
  namespace: flux-system
rules:
  - apiGroups: [image.toolkit.fluxcd.io]
    resources: [imagerepositories, imagepolicies, imageupdateautomations]
    verbs: [get, list, watch]
  - apiGroups: [kustomize.toolkit.fluxcd.io]
    resources: [kustomizations]
    verbs: [get, list, watch]
```

The API versions it reads are `apps/v1`, `helm.toolkit.fluxcd.io/v2`,
`image.toolkit.fluxcd.io/v1` and `kustomize.toolkit.fluxcd.io/v1`.

The service runs only inside the cluster: it has no kubeconfig path.

## Container image

`ghcr.io/rmypg/site-status:vX.Y.Z`, built on distroless static and pinned to its
base images by digest. It runs as UID 65532 and writes nothing to its filesystem,
so it works with a read-only root filesystem under Pod Security `restricted`. Logs
are JSON on stdout. `SIGTERM` shuts it down after in-flight requests finish.

Pushing a `vX.Y.Z` tag publishes the image. Deploying it is a reviewed commit in
`RMYPG/platform` that pins the new digest.

## Development

[mise](https://mise.jdx.dev/) installs the pinned tools.

```sh
mise run test   # go test -race
mise run lint   # go mod tidy -diff, golangci-lint, jactionlint
mise run ci     # both, as CI does
```

Docker builds happen in CI: the `image` job builds and starts the image on every
pull request, without pushing it.
