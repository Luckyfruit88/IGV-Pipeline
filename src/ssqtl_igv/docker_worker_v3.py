from __future__ import annotations

import os
import re
from typing import Any


DOCKER_WORKER_HOME = "/run/home"
DOCKER_WORKER_TMP_TMPFS = "/tmp:rw,noexec,nosuid,nodev,size=256m"
MAX_POSIX_ID = 2_147_483_647
_CANONICAL_POSITIVE_INTEGER = re.compile(r"^[1-9][0-9]{0,9}$")


def _positive_posix_id(value: object, *, label: str) -> int:
    if value is None or isinstance(value, bool):
        raise ValueError(f"Docker worker {label} is missing or invalid")
    text = str(value)
    if not _CANONICAL_POSITIVE_INTEGER.fullmatch(text):
        raise ValueError(
            f"Docker worker {label} must be a canonical positive integer; root (0) is prohibited"
        )
    parsed = int(text)
    if parsed > MAX_POSIX_ID:
        raise ValueError(
            f"Docker worker {label} exceeds the supported POSIX ID limit {MAX_POSIX_ID}"
        )
    return parsed


def docker_worker_identity(
    profile: str,
    host_uid: object | None,
    host_gid: object | None,
    *,
    discover: bool = False,
) -> dict[str, Any] | None:
    """Return the immutable host-worker mapping for the advanced Docker profile.

    The public CLI may discover both values together on POSIX hosts. Direct
    Nextflow use and internal orchestration must provide both explicitly so a
    missing value can never fall through to Docker's default user (root).
    """

    normalized = str(profile).strip().lower()
    supplied = host_uid is not None or host_gid is not None
    if normalized != "docker":
        if supplied:
            raise ValueError("Docker worker UID/GID are valid only with the docker profile")
        return None

    if discover and host_uid is None and host_gid is None:
        getuid = getattr(os, "getuid", None)
        getgid = getattr(os, "getgid", None)
        if callable(getuid) and callable(getgid):
            host_uid = getuid()
            host_gid = getgid()
    elif (host_uid is None) != (host_gid is None):
        raise ValueError("Docker worker UID and GID must be supplied together")

    uid = _positive_posix_id(host_uid, label="UID")
    gid = _positive_posix_id(host_gid, label="GID")
    return {
        "schema_version": "3.0-docker-worker-identity",
        "uid": uid,
        "gid": gid,
        "home": DOCKER_WORKER_HOME,
        "home_tmpfs": (
            f"/run/home:rw,noexec,nosuid,nodev,size=64m,uid={uid},gid={gid},mode=0700"
        ),
        "tmp_tmpfs": DOCKER_WORKER_TMP_TMPFS,
        "root_user_prohibited": True,
    }
