ARG PRODUCTION_RUNTIME_REF
FROM ${PRODUCTION_RUNTIME_REF}

ARG DEBUG_TOOLS_BUNDLE_SHA256

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

LABEL org.opencontainers.image.title="IGV Snapshot Workflow runtime-debug" \
      org.opencontainers.image.description="Separate noVNC diagnostic runtime; never review or publication eligible" \
      org.opencontainers.image.runtime.role="runtime-debug" \
      org.opencontainers.image.artifact.class="DEBUG_ONLY" \
      org.opencontainers.image.review.eligible="false" \
      org.opencontainers.image.publication.eligible="false"

USER 0

COPY containers/runtime-debug-materials.lock.json /opt/igv-container-locks/runtime-debug-materials.lock.json
COPY containers/runtime-debug-policy.json /opt/igv-container-locks/runtime-debug-policy.json

# This file is deliberately absent while runtime-debug-materials.lock.json is in
# MISSING_MATERIALS state. scripts/build-runtime-debug.sh rejects the build
# before Docker is called. A release engineer must supply the offline bundle and
# its real SHA-256; the Dockerfile never downloads an unlocked debug dependency.
COPY containers/debug-runtime-tools.tar.gz /tmp/debug-runtime-tools.tar.gz
RUN [[ "${DEBUG_TOOLS_BUNDLE_SHA256}" =~ ^[a-f0-9]{64}$ ]] \
    && printf '%s  %s\n' "${DEBUG_TOOLS_BUNDLE_SHA256}" /tmp/debug-runtime-tools.tar.gz | sha256sum --check --strict - \
    && mkdir -p /opt/debug-runtime \
    && tar --extract --gzip --file /tmp/debug-runtime-tools.tar.gz \
        --directory /opt/debug-runtime --no-same-owner --no-same-permissions \
    && rm -f /tmp/debug-runtime-tools.tar.gz \
    && test -x /opt/debug-runtime/bin/novnc_proxy \
    && test -x /opt/debug-runtime/bin/websockify \
    && test -x /opt/debug-runtime/bin/x11vnc \
    && chmod -R a-w /opt/debug-runtime

COPY --chmod=0755 containers/bin/runtime-debug-entrypoint /usr/local/bin/runtime-debug-entrypoint
COPY --chmod=0755 containers/bin/debug-screenshot /usr/local/bin/debug-screenshot

ENV PATH="/opt/debug-runtime/bin:${PATH}" \
    IGV_SNAPSHOT_IMAGE_ROLE=runtime-debug \
    IGV_SNAPSHOT_ARTIFACT_CLASS=DEBUG_ONLY \
    DEBUG_OUTPUT_ROOT=/run/debug-only \
    DISPLAY=:99

USER 65532:65532
WORKDIR /run/debug-only

ENTRYPOINT ["runtime-debug-entrypoint"]
