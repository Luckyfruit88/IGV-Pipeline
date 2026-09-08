"""One-shot, branch-only source transformation; removed after verification."""
from pathlib import Path
import ast
import hashlib
import re
import textwrap

ROOT = Path(__file__).resolve().parents[1]

def read(path):
    return (ROOT / path).read_text()

def write(path, content):
    target = ROOT / path
    target.parent.mkdir(parents=True, exist_ok=True)
    target.write_text(content)

def replace(content, old, new):
    assert old in content, old[:100]
    return content.replace(old, new)

def function(content, name):
    node = next(n for n in ast.parse(content).body if isinstance(n, ast.FunctionDef) and n.name == name)
    return ''.join(content.splitlines(keepends=True)[node.lineno - 1:node.end_lineno]) + '\n'

src = read('src/ssqtl_igv/v3_cli.py')
raw = src.encode()
assert hashlib.sha1(b'blob ' + str(len(raw)).encode() + b'\0' + raw).hexdigest() == '9c04255734de7c57a67adbca650e4c07728de3b9'
pstart = src.index('    campaign = subparsers.add_parser(')
pend = src.index('    return parser', pstart)
parser_block = src[pstart:pend]
bstart = src.index('        elif args.command == "campaign":')
bend = src.index('        else:  # pragma: no cover - argparse owns completeness\n            raise AssertionError(args.command)', bstart)
body = textwrap.dedent(src[bstart:bend].split('\n', 1)[1])
bench = '''"""Optional, historical research campaign commands; not a product prerequisite."""
from __future__ import annotations

import argparse
import os
import shutil
import subprocess
import sys
from pathlib import Path
from typing import Any

from .campaign_v3 import create_next_batch, prepare_campaign, reduce_campaign_status
from .orchestrator_v3 import resolve_max_parallel, run_portable_ssqtl_normalization
from .project_launcher import run_project_workflow
from .project_v3 import load_project_config
from .utils import reject_symlink_path_components
from .v3_cli import _add_resource_options, _embedded_runtime_manifest, _emit, _resume_identity


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        prog="python -m ssqtl_igv.benchmark_cli",
        description="Historical SCC research campaigns (not required for normal runs)",
    )
    subparsers = parser.add_subparsers(dest="command", required=True)
''' + parser_block + '    return parser\n\n\n'
bench += function(src, '_prepare_campaign_master') + '\n\n' + function(src, '_run_campaign_batch') + '\n\n'
bench += '''def main(argv: list[str] | None = None) -> int:
    args = _parser().parse_args(argv)
    try:
''' + textwrap.indent(body, '        ') + '''    except (OSError, TypeError, ValueError, RuntimeError, subprocess.SubprocessError) as exc:
        _emit({"status": "INFRASTRUCTURE_FATAL", "error_type": type(exc).__name__, "message": str(exc)}, stream=sys.stderr)
        return 1
    _emit(result)
    return code


if __name__ == "__main__":
    raise SystemExit(main())
'''
bench = replace(bench, '    try:\n        if args.campaign_command == "prepare":', '''    try:
        if args.campaign_command == "run-batch":
            output = reject_symlink_path_components(args.output, label="batch output").resolve(strict=False)
            for name in ("home", "nextflow"):
                directory = reject_symlink_path_components(output / ".runtime" / name, label="batch runtime")
                directory.mkdir(parents=True, exist_ok=True)
            os.environ["HOME"] = str(output / ".runtime/home")
            os.environ["NXF_HOME"] = str(output / ".runtime/nextflow")
            self_test = Path("/usr/local/bin/runtime-self-test")
            if self_test.is_file():
                subprocess.run([str(self_test)], check=True, stdout=sys.stderr)
        if args.campaign_command == "prepare":''')
write('src/ssqtl_igv/benchmark_cli.py', bench)
core = src[:bstart] + src[bend:]
core = core[:pstart] + core[pend:]
for name in ('_prepare_campaign_master', '_run_campaign_batch'):
    core = replace(core, function(src, name).rstrip() + '\n\n\n', '')
for block in ('from .campaign_v3 import (\n    create_next_batch,\n    prepare_campaign,\n    reduce_campaign_status,\n)\n', 'from .orchestrator_v3 import (\n    resolve_max_parallel,\n    run_portable_ssqtl_normalization,\n)\n', 'import shutil\n', 'from .migration_v3 import import_v2_read_only\n', 'from .publication import build_publication_promotion_receipt, promote_publication\n', 'from .publication_v3 import build_publication_staging\n', 'from .review_server import finalize_review, serve_review\n'):
    core = replace(core, block, '')
core = replace(core, 'def _publish(args: argparse.Namespace) -> dict[str, Any]:\n', 'def _publish(args: argparse.Namespace) -> dict[str, Any]:\n    from .publication import build_publication_promotion_receipt, promote_publication\n    from .publication_v3 import build_publication_staging\n\n')
core = replace(core, '        elif args.command == "review":\n', '        elif args.command == "review":\n            from .review_server import finalize_review, serve_review\n\n')
core = replace(core, '        elif args.command == "import-v2":\n', '        elif args.command == "import-v2":\n            from .migration_v3 import import_v2_read_only\n\n')
core = replace(core, '    doctor = subparsers.add_parser(', '''    smoke = subparsers.add_parser("smoke-test", help="optionally run a tiny real IGV example; no pilot required")
    smoke.add_argument("--output", default="/output/smoke-test")

    doctor = subparsers.add_parser(''')
core = replace(core, '        elif args.command == "doctor":\n', '        elif args.command == "smoke-test":\n            from .smoke_test import run_smoke_test\n\n            result = run_smoke_test(args.output)\n            code = int(result["exit_code"])\n        elif args.command == "doctor":\n')
core = core.replace('"--max-cases-per-shard", type=int, default=256)', '"--max-cases-per-shard", type=int, default=256, help=argparse.SUPPRESS)')
write('src/ssqtl_igv/v3_cli.py', core)
orch = read('src/ssqtl_igv/orchestrator_v3.py')
for module, name in (('campaign_v3', 'load_and_validate_batch_request'), ('review_package_v3', 'build_review_package_v3')):
    orch = replace(orch, f'from .{module} import {name}\n', '')
    wrapper = f'''\ndef {name}(*args, **kwargs):
    """Load the optional research layer only when explicitly requested."""
    from .{module} import {name} as implementation

    return implementation(*args, **kwargs)

'''
    pos = orch.index('\n_SGE_SITE_TOKEN =')
    orch = orch[:pos] + wrapper + orch[pos:]
write('src/ssqtl_igv/orchestrator_v3.py', orch)
admission = replace(read('src/ssqtl_igv/project_admission_v3.py'), 'from .campaign_v3 import materialize_batch_tasks\n', '')
admission = replace(admission, '            tasks, binding_value = materialize_batch_tasks(batch_request)', '            from .campaign_v3 import materialize_batch_tasks\n\n            tasks, binding_value = materialize_batch_tasks(batch_request)')
write('src/ssqtl_igv/project_admission_v3.py', admission)
entry = replace(read('containers/bin/runtime-entrypoint'), 'doctor|run|rerun-failed|review|publish|campaign)', 'doctor|run|rerun-failed|review|publish|smoke-test)')
start = entry.index('        if [[ "$1" == "run" ||')
end = entry.index('            /usr/local/bin/runtime-self-test', start)
entry = entry[:start] + '        if [[ "$1" == "run" || "$1" == "rerun-failed" || "$1" == "smoke-test" ]]; then\n' + entry[end:]
write('containers/bin/runtime-entrypoint', entry)
for path in ('tests/unit/test_campaign_execution_cli_v3.py', 'tests/unit/test_campaign_v3.py'):
    test = replace(read(path), 'from ssqtl_igv import v3_cli', 'from ssqtl_igv import benchmark_cli as v3_cli')
    if 'execution_cli' in path:
        test = replace(test, 'def test_campaign_execution_options_do_not_change_public_run_options() -> None:\n    parser = v3_cli._parser()', 'def test_campaign_execution_options_do_not_change_public_run_options() -> None:\n    from ssqtl_igv import v3_cli as product_cli\n\n    parser = product_cli._parser()')
    write(path, test)
old = ROOT / '.github/workflows/pilot-candidate.yml'
write('benchmarks/legacy/pilot-candidate.yml', old.read_text())
old.unlink()
readme = read('README.md')
start = readme.index('#### Maintainer 100-case QA')
end = readme.index('### Developer Architecture', start)
legacy = readme[start:end]
readme = readme[:start] + '''#### Optional installation check

Run `igv-snapshot smoke-test --output /output/smoke-test` inside the runtime to
render two small synthetic BAM examples with the real IGV/Nextflow path. This
is optional and does not authorize or gate later runs. `-stub-run` checks DAG
wiring only, not actual rendering. See [testing and releases](docs/testing-and-releases.md).

''' + readme[end:]
readme = re.sub(r'> Release status:.*?use a locally built image tag\.\n', '''> The published v3.0.0 remains available. Source changes on this refactor branch
> are not in that image: build a local image to exercise them. No 100-case pilot
> is required for installation, ordinary runs, or subsequent releases.
''', readme, flags=re.S)
readme = readme.replace('igv-snapshot campaign ...', 'igv-snapshot smoke-test').replace('### SCC Pilot Qualification', '### Optional BU SCC deployment')
start = readme.index('#### 维护者 100-case QA')
end = readme.index('### 开发者架构', start)
legacy_zh = readme[start:end]
readme = readme[:start] + '''#### 可选的小型真实出图测试

在运行环境中执行 `igv-snapshot smoke-test --output /output/smoke-test`，
使用微型合成 BAM 走真实 IGV/Nextflow 出图流程。此项测试不阻塞正式运行，
也不会生成“授权通过”凭证；不再要求固定 100-case pilot。
`-stub-run` 只验证流程连接，不能证明 IGV 已经正确出图。
详见[测试与发布](docs/testing-and-releases.md)。

''' + readme[end:]
readme = readme.replace('### SCC Pilot 验证', '### 可选的 BU SCC 部署').replace('igv-snapshot campaign reconcile', 'python -m ssqtl_igv.benchmark_cli campaign reconcile')
readme = readme.replace('Campaign commands are an advanced scientific batching layer. They authorize\nselections and later batches but never copy Nextflow task state.', 'Historical campaign commands are isolated in `python -m ssqtl_igv.benchmark_cli`.\nThey are not required for ordinary runs; see [the archived protocol](benchmarks/legacy/README.md).')
write('README.md', readme)
write('benchmarks/legacy/README.md', ('''# Historical SCC pilot

This directory preserves the original v3.0.0 maintainer experiment. It is not
part of installation, ordinary execution, or the release gate. The archived
workflow is documentation, not a registered GitHub Actions workflow.

Existing campaign data remains readable. To operate a historical campaign use
`python -m ssqtl_igv.benchmark_cli campaign ...` from a source checkout or an
explicit container Python entrypoint. Do not run this workflow before ordinary
`igv-snapshot run`. The frozen 8,973/100-case policy is deliberately not presented
as a general-purpose batching interface. Existing release assets are unchanged.

## Original protocol (historical, not current requirements)

''' + legacy + '\n## 原始协议（历史记录，不是当前要求）\n\n' + legacy_zh).rstrip() + '\n')
for path in (ROOT / 'docs').rglob('*.md'):
    content = path.read_text()
    updated = content.replace('igv-snapshot campaign ', 'python -m ssqtl_igv.benchmark_cli campaign ')
    if updated != content:
        path.write_text(updated)
test = read('tests/test_portable_workflow.py').replace('SCC Pilot Qualification', 'Optional BU SCC deployment').replace('SCC Pilot 验证', '可选的 BU SCC 部署')
write('tests/test_portable_workflow.py', test)
helper = read('scripts/submit-bu-scc-pull-run.sh')
helper = replace(helper, 'if [[ -n "${batch_request_arg}" ]]; then\n    container_command+=(', 'if [[ -n "${batch_request_arg}" ]]; then\n    container_command[1]=exec\n    container_command+=(')
helper = replace(helper, '        "${sif_path}"\n        campaign\n', '        "${sif_path}"\n        /opt/igv-helper/bin/python\n        -m ssqtl_igv.benchmark_cli\n        campaign\n')
write('scripts/submit-bu-scc-pull-run.sh', helper)
test = read('tests/unit/test_scc_pull_run_launcher.py').replace('f" {sif} campaign run-batch"', 'f" {sif} /opt/igv-helper/bin/python -m ssqtl_igv.benchmark_cli campaign run-batch"')
write('tests/unit/test_scc_pull_run_launcher.py', test)
s = read('tests/unit/test_runtime_v3_contract.py')
s = s.replace('doctor|run|rerun-failed|review|publish|campaign)', 'doctor|run|rerun-failed|review|publish|smoke-test)')
s = s.replace('assert \'"${2:-}" == "prepare-master"\' in entrypoint', 'assert \'"$1" == "smoke-test"\' in entrypoint')
s = s.replace('assert \'"${2:-}" == "run-batch"\' in entrypoint', 'assert \'"$1" == "campaign"\' not in entrypoint')
s = s.replace('assert \'"${2:-}" == "prepare-master"\' in condition', 'assert \'"$1" == "smoke-test"\' in condition')
s = s.replace('assert \'"${2:-}" == "run-batch"\' in condition', 'assert \'"$1" == "campaign"\' not in condition')
s = s.replace('candidate = _text(".github/workflows/pilot-candidate.yml")', 'candidate = _text("benchmarks/legacy/pilot-candidate.yml")', 1)
replacements = {
    'test_release_workflow_promotes_candidate_supply_chain_evidence': '''def test_release_workflow_tests_a_tagged_image_without_site_gates() -> None:
    release = _text(".github/workflows/release.yml")
    assert 'tags: ["v*"]' in release
    assert 'scripts/release-version.py --tag "$GITHUB_REF_NAME"' in release
    assert "python -m pytest -q" in release
    assert release.count("docker/build-push-action@") == 1
    assert "load: true" in release and "push: false" in release
    assert "smoke-test --output /output/smoke" in release
    assert release.index("Test the candidate with real IGV") < release.index("docker push")
    assert "--network none" in release
    assert "--cap-drop ALL" in release
    assert "--security-opt no-new-privileges" in release
    assert 'exit-code: "0"' in release
    for gate in ("pilot-candidate", "SCC", "8,973", "100-case", "refs/remotes/origin/main"):
        assert gate not in release
''',
    'test_release_tags_share_one_build_digest_and_maintainer_artifact_set': '''def test_release_pushes_the_tested_image_and_retains_evidence() -> None:
    release = _text(".github/workflows/release.yml")
    assert release.count("docker/build-push-action@") == 1
    assert 'docker tag igv-pipeline:release "$target"' in release
    assert 'docker push "$target"' in release
    assert 'docker pull "${IMAGE}@${digest}"' in release
    assert 'test "$(docker image inspect' in release
    assert "Refusing to overwrite an existing version tag" in release
    for artifact in ("SOURCE_COMMIT.txt", "SOURCE_TREE.txt", "RUNTIME_MANIFEST.json",
                     "TESTED_IMAGE_ID.txt", "OCI_DIGEST.txt", "SHA256SUMS", "trivy-report.json"):
        assert artifact in release
    assert 'gh release create "$GITHUB_REF_NAME" --verify-tag' in release
    assert "latest" not in release
    assert "igv-pipeline-${{ steps.source.outputs.version }}-release-evidence" in release
''',
}
for name, new in replacements.items():
    node = next(n for n in ast.parse(s).body if isinstance(n, ast.FunctionDef) and n.name == name)
    lines = s.splitlines(keepends=True)
    s = ''.join(lines[:node.lineno - 1]) + new + ''.join(lines[node.end_lineno:])
write('tests/unit/test_runtime_v3_contract.py', s)
# The connector commits workflow edits separately; tests use the final intended tree.
ci = replace(read('.github/workflows/ci.yml'), 'branches: [main]', 'branches: [main, "refactor/lightweight-*"]')
ci = replace(ci, '      - name: Run raw ssQTL through the native project DAG\n', '''      - name: Run optional real IGV smoke example
        shell: bash
        run: |
          set -euo pipefail
          mkdir -p "${RUNNER_TEMP}/real-smoke-output"
          docker run --rm --platform linux/amd64 \\
            --user "$(id -u):$(id -g)" --network none \\
            --mount "type=bind,src=${RUNNER_TEMP}/real-smoke-output,dst=/output" \\
            igv-pipeline:ci smoke-test --output /output/smoke

      - name: Run raw ssQTL through the native project DAG
''')
write('.github/workflows/ci.yml', ci)
(ROOT / '.github/workflows/refactor-source.yml').unlink(missing_ok=True)
print('Lightweight source transformation complete.')
