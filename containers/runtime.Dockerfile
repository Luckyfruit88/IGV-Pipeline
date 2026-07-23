FROM docker.io/library/almalinux@sha256:f043b7ac550015e1ed0b5a55a420c61d178bff4357ab9663fe0fbdcf1e6e2d86

ARG TARGETPLATFORM
ARG BUILDPLATFORM
ARG SOURCE_COMMIT
ARG SOURCE_TREE

LABEL org.opencontainers.image.title="IGV Pipeline" \
      org.opencontainers.image.version="3.0.0" \
      org.opencontainers.image.source="https://github.com/Luckyfruit88/IGV-Pipeline" \
      org.opencontainers.image.description="Pinned linux/amd64 Nextflow, IGV Desktop, Xvfb, QC, and composition runtime" \
      org.opencontainers.image.base.digest="sha256:f043b7ac550015e1ed0b5a55a420c61d178bff4357ab9663fe0fbdcf1e6e2d86"

SHELL ["/bin/bash", "-o", "pipefail", "-c"]

COPY containers/runtime-system-packages.lock /opt/igv-container-locks/runtime-system-packages.lock
COPY containers/fonts-local.conf /etc/fonts/local.conf
RUN [[ "${TARGETPLATFORM}" == "linux/amd64" ]] \
    && mapfile -t runtime_packages < <(grep -Ev '^[[:space:]]*(#|$)' /opt/igv-container-locks/runtime-system-packages.lock) \
    && dnf --setopt=install_weak_deps=False --setopt=keepcache=False install -y "${runtime_packages[@]}" \
    && dnf clean all \
    && rm -rf /var/cache/dnf \
    && fc-cache -f \
    && rpm -qa --qf '%{NAME}-%{EPOCHNUM}:%{VERSION}-%{RELEASE}.%{ARCH}\n' | LC_ALL=C sort > /opt/igv-container-locks/rpm-inventory.txt

ADD --chmod=0755 --checksum=sha256:e9683b483df06dbd3fdd8a37f1b6826d7e5caf4e85bf15a0af4fbad3d4ad1a58 \
    https://github.com/mamba-org/micromamba-releases/releases/download/2.6.2-1/micromamba-linux-64 \
    /usr/local/bin/micromamba
COPY containers/helper-linux-64.lock /opt/igv-container-locks/helper-linux-64.lock
COPY containers/samtools-linux-64.lock /opt/igv-container-locks/samtools-linux-64.lock
COPY containers/materials.lock.json /opt/igv-container-locks/helper-materials.lock.json
COPY containers/runtime-materials.lock.json /opt/igv-container-locks/runtime-materials.lock.json

RUN export MAMBA_ROOT_PREFIX=/opt/micromamba-root \
    && micromamba create --yes --prefix /opt/igv-helper --file /opt/igv-container-locks/helper-linux-64.lock \
    && micromamba create --yes --prefix /opt/igv-samtools --file /opt/igv-container-locks/samtools-linux-64.lock \
    && micromamba clean --all --yes \
    && rm -rf /opt/micromamba-root /root/.cache

ADD --checksum=sha256:0b7a9351598a27140ebd995f07e478b668c9e512d8f17964be1ce64e556527e7 \
    https://data.broadinstitute.org/igv/projects/downloads/2.16/IGV_Linux_2.16.2_WithJava.zip \
    /tmp/igv.zip
RUN unzip -q /tmp/igv.zip -d /tmp/igv-unpack \
    && mkdir -p /opt/igv \
    && cp -a /tmp/igv-unpack/IGV_Linux_2.16.2/. /opt/igv/ \
    && rm -rf /tmp/igv.zip /tmp/igv-unpack

ADD --checksum=sha256:968c283e104059dae86ea1d670672a80170f27a39529d815843ec9c1f0fa2a03 \
    https://github.com/adoptium/temurin21-binaries/releases/download/jdk-21.0.8%2B9/OpenJDK21U-jre_x64_linux_hotspot_21.0.8_9.tar.gz \
    /tmp/java-21.tar.gz
RUN mkdir -p /opt/java-21 \
    && tar -xzf /tmp/java-21.tar.gz --strip-components=1 -C /opt/java-21 \
    && rm -f /tmp/java-21.tar.gz

ADD --chmod=0444 --checksum=sha256:231a3c0fc7bae88add57ab420e9c6306ee35ff87b6af356cd2d8aa56347472f3 \
    https://www.nextflow.io/releases/v25.04.7/nextflow-25.04.7-one.jar \
    /opt/nextflow/nextflow-25.04.7-one.jar
ADD --chmod=0555 --checksum=sha256:a57f804243c6fa3b1e3194ab05a054f7799b5d4423049b62bbb171530dba9fe2 \
    https://www.nextflow.io/releases/v25.04.7/nextflow \
    /opt/nextflow/nextflow-25.04.7-launcher
RUN chmod 0555 /opt/nextflow \
    && chmod 0444 /opt/nextflow/nextflow-25.04.7-one.jar \
    && chmod 0555 /opt/nextflow/nextflow-25.04.7-launcher

WORKDIR /opt/igv-pipeline/pipeline
COPY LICENSE MANIFEST.in README.md README.zh-CN.md ./
COPY main.nf nextflow.config nextflow_schema.json pyproject.toml ./
COPY requirements-build.lock requirements.lock uv.lock ./
COPY bin ./bin
COPY conf ./conf
COPY config ./config
COPY containers ./containers
COPY docs ./docs
COPY modules ./modules
COPY schema ./schema
COPY scripts ./scripts
COPY src ./src
COPY workflows ./workflows
RUN /opt/igv-helper/bin/python -m pip install \
        --no-deps \
        --no-build-isolation \
        --no-cache-dir \
        /opt/igv-pipeline/pipeline
RUN [[ "${SOURCE_COMMIT}" =~ ^[a-f0-9]{40}$ ]] \
    && [[ "${SOURCE_TREE}" =~ ^[a-f0-9]{40}$ ]] \
    && /opt/igv-helper/bin/python bin/create_runtime_manifest.py \
        --source-commit "${SOURCE_COMMIT}" \
        --source-tree "${SOURCE_TREE}" \
        --materials-lock containers/runtime-materials.lock.json \
        --runtime-config src/ssqtl_igv/resources/v3-runtime.yaml \
        --output /opt/igv-pipeline/runtime-manifest.json \
    && chmod 0444 /opt/igv-pipeline/runtime-manifest.json

COPY --chmod=0755 containers/bin/igv /opt/igv/bin/igv
COPY --chmod=0755 containers/bin/nextflow /usr/local/bin/nextflow
COPY --chmod=0755 containers/bin/runtime-entrypoint /usr/local/bin/runtime-entrypoint
COPY --chmod=0755 containers/runtime-self-test.sh /usr/local/bin/runtime-self-test

RUN groupadd --gid 65532 igvsnapshot \
    && useradd --uid 65532 --gid 65532 --home-dir /run/home --shell /sbin/nologin igvsnapshot \
    && mkdir -p /run/home /output /project /work \
    && chown -R 65532:65532 /run /output /work \
    && chmod -R a-w /opt /usr/local/bin/nextflow /usr/local/bin/runtime-entrypoint /usr/local/bin/runtime-self-test

ENV PATH="/opt/igv-helper/bin:/opt/igv-samtools/bin:/opt/igv/bin:/usr/local/bin:${PATH}" \
    PYTHONNOUSERSITE=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    LC_ALL=C.UTF-8 \
    LANG=C.UTF-8 \
    TESSDATA_PREFIX=/opt/igv-helper/share/tessdata \
    IGV_JAVA_HOME=/opt/igv/jdk-11 \
    NXF_JAVA_HOME=/opt/java-21 \
    NXF_VER=25.04.7 \
    NXF_OFFLINE=true \
    NXF_HOME=/run/home/.nextflow \
    IGV_SNAPSHOT_PIPELINE_DIR=/opt/igv-pipeline/pipeline \
    IGV_RUNTIME_MANIFEST=/opt/igv-pipeline/runtime-manifest.json \
    HOME=/run/home

USER 65532:65532
WORKDIR /output

RUN NXF_HOME=/work/.nextflow runtime-self-test \
    && rm -rf /work/.nextflow

ENTRYPOINT ["runtime-entrypoint"]
CMD ["--help"]
