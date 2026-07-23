# Portable runtime 3.0

The production artifact is one public `linux/amd64` OCI image:

```text
ghcr.io/luckyfruit88/igv-pipeline:3.0.0
```

Docker runs that image directly. Apptainer and Singularity convert the same
image from `docker://` into a local SIF; the project does not maintain a second
prebuilt SIF artifact.

## Embedded manifest and cache identity

The image contains an unsigned manifest at:

```text
/opt/igv-pipeline/runtime-manifest.json
```

It freezes the pipeline version and source commit/tree, platform, material-lock
hash, render-policy hash, IGV and its bundled JRE, Nextflow and controller Java,
Python/R, samtools, Poppler, ImageMagick, fontconfig/fonts, Tesseract, and Xvfb.
The controller validates the manifest automatically and computes
`runtime_fingerprint_sha256`. The manifest and validation bundle are real
Nextflow inputs, so a runtime or render-policy change changes the task cache
identity.

The manifest does not contain a signature, certification envelope, named
approver, approval date, expiry state, host path, scheduler ID, or timestamp.
Users do not supply a public key, identity sidecar, OCI/SIF mapping, or JRE
approval record.

When an OCI digest or SIF checksum can be observed automatically, it is written
as provenance. Observed provenance is not a prerequisite and is not part of the
portable fingerprint.

## Two isolated JVMs

IGV Desktop 2.16.2 always launches with its bundled JRE 11 through
`/opt/igv/bin/igv`. Nextflow 25.04.7 always launches with the isolated Java 21
runtime. The startup self-test checks both. JRE 11 remains visible in the
manifest, SBOM, and vulnerability report, but has no manual approval workflow.

## Pull-and-run execution

The outer container runs Nextflow with the local executor. It does not mount a
Docker socket and does not launch nested containers. The public command accepts
the project, output, work, resume, concurrency, and shard-size controls only.
At startup the entrypoint creates HOME and Nextflow state below the writable
output mount so Docker `--user`, Apptainer, and Singularity can all run as the
invoking non-root UID.

The automatic concurrency rule is:

```text
min(8, available CPUs, floor(available memory / 8 GiB))
```

If memory cannot be observed reliably, concurrency is one. The resolved value
is frozen in the run contract and configures Nextflow queue size and render
`maxForks`.

## Runtime self-test

The image build and every run check the embedded manifest, tool versions,
screen/runtime contract, Xvfb, fonts, OCR, both JVMs, IGV wrapper, Nextflow, and
writable runtime state. Network access is not needed after image/SIF retrieval.
A self-test or input-integrity failure exits `1` before rendering.

## SCC boundary

The default BU SCC pattern requests one compute-node allocation, then runs the
same SIF with Nextflow's local executor and at most eight concurrent cases. Raw
qacct is optional accounting for that outer job and does not block screenshot
availability. The existing host-controller/Singularity/SGE mode remains an
advanced architecture, not part of the pull-only promise.

The maintainer 100-case pilot checks exact task coverage, artifacts, and QC with
the same SIF. It does not issue a runtime certificate or add a user parameter.

## Release artifacts

The `v3.0.0` workflow publishes `:3.0.0`, `:3.0`, and `:latest` from the same
build result. BuildKit provenance and SBOM attestations, a digest checksum, and
a non-blocking vulnerability report are maintenance evidence. None are files a
user must download to run the pipeline.
