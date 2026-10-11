ARG DAR_IMAGE
FROM ${DAR_IMAGE} AS cli

# Go 1.26.8, pinned to its multi-platform manifest.
FROM --platform=$BUILDPLATFORM docker.io/library/golang@sha256:dc9ad6c05acc7a88e5b71bde60a5fe3bd4b9f0db209011711b464107438a8107 AS build
WORKDIR /src
ENV CGO_ENABLED=0
COPY go.mod go.sum ./
RUN go mod download
COPY *.go ./
RUN go test -count=1 -timeout 60s ./...
ARG TARGETARCH
RUN GOOS=linux GOARCH=$TARGETARCH go build -trimpath -o /out/go-cli .

# Debian 12 slim, compatible with the CLI's glibc 2.36 baseline.
FROM docker.io/library/debian@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
RUN apt-get update \
    && apt-get install -y --no-install-recommends libc6 libgcc-s1 ca-certificates \
    && rm -rf /var/lib/apt/lists/*
COPY --from=cli /usr/local/bin/dar /usr/local/bin/dar
COPY --from=cli /usr/share/doc/dar/ /usr/share/doc/dar/
COPY --from=build /out/go-cli /usr/local/bin/go-cli
WORKDIR /work
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/go-cli"]
