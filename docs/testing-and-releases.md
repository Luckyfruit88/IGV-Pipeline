# Testing and releases

## Ordinary use

Run the project directly with `igv-snapshot run`. No campaign, fixed pilot size,
review, or publication receipt is required. Required input validation, runtime
checks, per-case QC, exact task coverage, and `rerun-failed` remain enabled.

On a new installation, an optional real-render test is available:

```bash
# Inside the built OCI/SIF runtime; use a new, writable output directory.
igv-snapshot smoke-test --output /output/smoke-test
```

It creates two synthetic loci with a tiny real indexed BAM and local reference,
then executes the normal Nextflow DAG and IGV renderer with one worker. It checks
the expected task IDs, image checksums and PNG contents. The directory must not
already exist; failed evidence is retained instead of overwritten. Use another
new directory to repeat it. Nothing is downloaded, no human approval is created,
and a successful test is not a requirement for a later project run. Two cases
are example coverage, not a scientific validation threshold or a resource test.

`-stub-run` remains useful to developers for testing DAG wiring. It does not
render IGV images. Synthetic test doubles are not accepted as production output.
The existing CI still tests Nextflow resume and the ssQTL adapter separately.

The published v3.0.0 image predates this refactor. Build the branch's Dockerfile
with its SOURCE_COMMIT and SOURCE_TREE arguments to test the new command; do not
expect the already published image to change when this branch changes.

## Maintainer releases

A stable `vMAJOR.MINOR.PATCH` tag must match `pyproject.toml`. Source-level version
constants and the runtime self-test must also be updated together on a version
bump; the release workflow itself does not contain a version to edit.

The workflow runs regression tests, builds one image, tests that image using the
real smoke example and embedded source identity, and only then pushes the same
image. The published digest is pulled back and checked against the tested image
ID. Existing version tags are not overwritten. Only the exact version is pushed;
rolling `latest` or major/minor aliases are deliberately not moved automatically.
Build identifiers, the runtime manifest, checksums and a vulnerability report are
retained. This workflow does not require a particular compute site, the current
main HEAD, a pilot candidate, or any fixed number of research tasks.

No release is created by a branch push. This refactor does not alter v3.0.0,
its existing container tags, historical QA results, or any user's output.

## Historical campaigns

The original experiment and candidate workflow are archived under
`benchmarks/legacy/`. Operators of existing campaigns can use
`python -m ssqtl_igv.benchmark_cli campaign ...`. This source/module interface is
separate from `igv-snapshot` and the default container entrypoint. The frozen
8,973/100-case policy remains only to preserve the meaning of existing records.
The batch-request API and scientific review remain available for those records,
but normal runs do not import the campaign or review implementation.

## Refactor boundary

This change does not redesign output generations, reduce the safety of input
paths, retire old result formats, change the renderer, or change resource retry
semantics. Input-column simplification, shared-reference mounts, file-level QC
caching and image-size optimization require separate regression-tested changes.
