# Debian 12 slim, pinned to its multi-platform manifest.
ARG BASE_IMAGE=docker.io/library/debian@sha256:7c7b2c966bc9ee8cedfeef67e0e279108992c77681fa595db4a9d65c06ccc587
FROM ${BASE_IMAGE}

ARG BASE_IMAGE
ARG CLI_VERSION
ARG CLI_SOURCE_COMMIT
LABEL org.opencontainers.image.source="https://github.com/mag1cfrog/delta-arrow-reader" \
      org.opencontainers.image.revision="${CLI_SOURCE_COMMIT}" \
      org.opencontainers.image.version="${CLI_VERSION}" \
      org.opencontainers.image.licenses="Apache-2.0" \
      org.opencontainers.image.base.name="${BASE_IMAGE}"

COPY runtime-packages.txt /tmp/dar-runtime-packages.txt
RUN apt-get update \
    && xargs apt-get install -y --no-install-recommends < /tmp/dar-runtime-packages.txt \
    && rm -rf /var/lib/apt/lists/* /tmp/dar-runtime-packages.txt

COPY --chmod=0755 dar /usr/local/bin/dar
COPY --chmod=0644 LICENSE NOTICE /usr/share/doc/dar/

WORKDIR /work
USER 65532:65532
ENTRYPOINT ["/usr/local/bin/dar"]
CMD ["--help"]
