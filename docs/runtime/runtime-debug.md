# runtime-debug / noVNC contract

## Status and control boundary

`runtime-debug` is a separate diagnostic image, not a production image target.
Its controlled object is one interactive IGV session and its output directory.
The desired state is local-only observation without creating an artifact that
can be mistaken for production evidence. Observable state consists of the
debug image digest, the `DEBUG_ONLY.json` session marker, visibly watermarked
screenshots, and per-screenshot sidecars. The control actions are admission
rejection, loopback-only port publication, isolated mounts, and generation
discard.

The committed implementation is currently a **fail-closed scaffold**:

- `containers/runtime.Dockerfile` remains the only production Dockerfile and
  contains no noVNC, websockify, or x11vnc software;
- `containers/runtime-debug.Dockerfile` can derive only from an explicitly
  digest-pinned production runtime;
- `containers/runtime-debug-materials.lock.json` is intentionally
  `MISSING_MATERIALS`, because no checksum-locked, scanned offline noVNC bundle
  is available in this checkout;
- `scripts/build-runtime-debug.sh` rejects that state before invoking Docker.

The root `.dockerignore` excludes the future debug bundle from production
build context. Only `containers/runtime-debug.Dockerfile.dockerignore` admits
it for the separate debug Dockerfile, preventing locked noVNC bytes from being
captured incidentally by the production image's source-tree copy.

This scaffold is not evidence that a debug image was built, scanned, or tested.
Do not replace the missing checksum with a guessed value. A future
debug-material resolution must record the actual bundle bytes and SHA-256,
change `lock_state` to `LOCKED`, pass the separate self-test, and publish to the
separate `igv-pipeline-debug` repository.

## Exposure and isolation

Once a locked debug digest exists, the only admitted launcher is:

```bash
bash scripts/run-runtime-debug.sh \
  --image 'ghcr.io/luckyfruit88/igv-pipeline-debug@sha256:<64hex>' \
  --input /absolute/input \
  --reference /absolute/reference \
  --debug-output /absolute/empty-debug-directory \
  --port 6080
```

The browser endpoint is `http://127.0.0.1:6080/vnc.html`. The host bind address
is fixed to `127.0.0.1`; it is not a CLI parameter. Inside the isolated
container network, noVNC listens on `0.0.0.0:6080` solely so Docker can forward
that port, while x11vnc listens on container loopback. The launcher creates an
internal Docker bridge, drops every capability, enables no-new-privileges,
uses a read-only root filesystem, and mounts input/reference read-only. It does
not mount host HOME, `/`, a production `/run`, a publication destination, or
the Docker socket.

After Xvfb is ready, the debug entrypoint starts `/opt/igv/bin/igv` itself with
`DISPLAY=:99`, the production wrapper's explicit bundled JRE 11, and a writable
`--igvDirectory /run/home/igv-debug`. The launcher supplies `/run/home` as an
ephemeral tmpfs. The private IGV control port is not published and the
entrypoint accepts no production batch argument; the noVNC window therefore
shows a real interactive IGV Desktop rather than an empty X display without
creating a production render path. IGV is included in self-test and PID
cleanup.

The localhost bind is a diagnostic convenience, not an authorization system.
Do not expose it through a reverse proxy, SSH remote forwarding, a public
interface, or a shared workstation session.

## DEBUG_ONLY artifact invariant

The debug image has OCI labels and environment state declaring:

```text
image_role=runtime-debug
artifact_class=DEBUG_ONLY
review_eligible=false
publication_eligible=false
```

It can write only to the dedicated `/run/debug-only` mount. Startup creates an
immutable `DEBUG_ONLY.json` session marker. The supported screenshot command,
`debug-screenshot SAFE_BASENAME`, writes
`SAFE_BASENAME.DEBUG_ONLY.png`, burns a visible `DEBUG_ONLY` watermark into the
pixels, adds a PNG comment, and creates a checksum sidecar whose review and
publication eligibility are both false.

Every image observed or captured through the noVNC session is diagnostic,
including screenshots taken by the browser or operating system that lack the
wrapper sidecar. Debug outputs must never be copied into a run generation,
review package, publication staging tree, parity result, or release evidence
bundle. A production artifact admission check must reject a `DEBUG_ONLY`
filename, marker, sidecar, metadata value, or runtime-debug identity.

The fail-closed tree gate is available for review/package and publication
launchers:

```bash
python3 scripts/verify-production-artifact-tree.py \
  --tree /absolute/candidate-production-tree
```

It rejects symlinks, `DEBUG_ONLY` names, JSON markers, runtime-debug identity,
and the supported debug PNG metadata. Calling this gate is mandatory before a
candidate tree is admitted to review or publication; a PASS does not make an
otherwise untrusted tree eligible.

Debugging can identify a corrective action, but it cannot repair an existing
generation in place. Apply the correction to the production runtime or render
policy, create a new production generation, and rerun through accounting, QC,
human review, and atomic publication. This feedback boundary prevents a useful
diagnostic observation from becoming silent production state.
