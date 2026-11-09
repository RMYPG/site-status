# A static Go binary on distroless static, so the image holds no shell, package
# manager or credentials. It runs as the image's non-root user 65532, needs no
# capabilities and writes nothing to its root filesystem, so Pod Security
# "restricted" with readOnlyRootFilesystem admits it. Both bases are pinned by
# digest; bump them in reviewed commits. Keep the Go version in step with
# mise.toml.
FROM golang:1.27.2-trixie@sha256:47667934874913a449b96faa7ef4777fa285d9ec29126ee80ed61c45b7cccf0e AS build
WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY cmd ./cmd
COPY internal ./internal
ARG VERSION=dev
RUN CGO_ENABLED=0 GOFLAGS=-mod=readonly GOTOOLCHAIN=local \
    go build -trimpath -ldflags "-s -w -X main.version=${VERSION}" -o /out/site-status ./cmd/site-status

# The static image has no libc, so only a statically linked binary runs on it.
FROM gcr.io/distroless/static-debian13:nonroot@sha256:25ac8f0c3b2334a9eb05452266bd4697864a8c8ef0ef71e5e6480d7f92868329

COPY --from=build /out/site-status /site-status

USER 65532:65532
EXPOSE 8080
ENTRYPOINT ["/site-status"]
