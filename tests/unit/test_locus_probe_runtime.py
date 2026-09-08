from __future__ import annotations

import base64
from pathlib import Path
from types import SimpleNamespace

import pytest
from PIL import Image

from ssqtl_igv import desktop
from ssqtl_igv.utils import sha256_file


def test_precompiled_probe_uses_explicit_java_without_compiling(tmp_path, monkeypatch):
    probe = tmp_path / "probe"
    probe.mkdir()
    clazz = probe / "LocusClipboardProbe.class"
    clazz.write_bytes(b"fixture class")
    (probe / "SHA256SUMS").write_text(sha256_file(clazz) + "  LocusClipboardProbe.class\n")
    java = tmp_path / "jre/bin/java"
    java.parent.mkdir(parents=True)
    java.write_text("fixture executable")
    monkeypatch.setattr(desktop, "_LOCUS_PROBE_ROOT", probe)
    monkeypatch.setattr(desktop.shutil, "which", lambda *a, **k: pytest.fail("PATH lookup is not the runtime contract"))
    calls = []
    def run(command, **kwargs):
        calls.append(command)
        return SimpleNamespace(returncode=0, stdout=base64.b64encode(b"chr11:100-200").decode(), stderr="")
    monkeypatch.setattr(desktop, "_run", run)
    result = desktop._probe_locus_control_text(evidence_dir=tmp_path, screen_point=(30, 40),
                                               env={"IGV_JAVA_HOME": str(java.parent.parent), "PATH": ""})
    assert result["observed_normalized"] == "chr11:100-200"
    assert result["probe_sha256"] == sha256_file(clazz)
    assert len(calls) == 1 and calls[0][0] == str(java)
    assert not list(tmp_path.rglob("*.java"))
    clazz.write_bytes(b"corrupt class")
    assert desktop._probe_locus_control_text(evidence_dir=tmp_path, screen_point=(30, 40),
                                            env={"IGV_JAVA_HOME": str(java.parent.parent)})["status"] == "ERROR"
    assert len(calls) == 1


@pytest.mark.parametrize("native_text,expected_pass", [("chr11:100-200", True), ("chr1:100-200", False), ("chr11:100-201", False)])
def test_native_fallback_keeps_exact_chromosome_and_coordinate_check(tmp_path, monkeypatch, native_text, expected_pass):
    frame = tmp_path / "frame.png"
    Image.new("RGB", (20, 20), "white").save(frame)
    monkeypatch.setattr(desktop, "_run", lambda *a, **k: SimpleNamespace(returncode=0, stdout="chr:100-200", stderr=""))
    monkeypatch.setattr(desktop, "_probe_locus_control_text", lambda **k: {
        "status": "OBSERVED", "observed_text": native_text, "observed_normalized": native_text})
    config = SimpleNamespace(get=lambda key, default=None: default)
    result = desktop.verify_expected_locus_text(config, frame=frame,
        roi={"x": 0, "y": 0, "width": 20, "height": 20}, expected_locus="chr11:100-200",
        evidence_path=tmp_path / "roi.png", control_screen_point=(10, 10))
    assert result["matched"] is expected_pass
    assert result["native_control_fallback"]["matched"] is expected_pass
