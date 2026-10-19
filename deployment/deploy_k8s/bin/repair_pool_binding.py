#!/usr/bin/env python3
"""Repair supervisor inputs from compute_ctl's single committed binding bundle."""
import json
import os
import stat
import sys
from pathlib import Path


def repair(root):
    root = Path(root)
    committed = root / "pool-binding-committed.json"
    if not committed.exists():
        return
    document = json.loads(committed.read_text())
    config, environment = document["config"], document["environment"]
    if not isinstance(config, dict) or not isinstance(environment, str):
        raise ValueError("invalid committed binding bundle")
    for path, text in ((root / "configs/config.json", json.dumps(config)),
                       (root / "pool-rebind.env", environment)):
        try:
            previous = path.stat()
            mode, uid, gid = stat.S_IMODE(previous.st_mode), previous.st_uid, previous.st_gid
        except FileNotFoundError:
            mode, uid, gid = 0o600, 1000, 1000
        temporary = path.with_name(path.name + ".supervisor-repair")
        fd = os.open(str(temporary), os.O_WRONLY | os.O_CREAT | os.O_TRUNC, mode)
        with os.fdopen(fd, "w") as output:
            os.fchmod(output.fileno(), mode)
            if os.geteuid() == 0:
                os.fchown(output.fileno(), uid, gid)
            output.write(text)
            output.flush()
            os.fsync(output.fileno())
        os.replace(str(temporary), str(path))
        directory = os.open(str(path.parent), os.O_RDONLY | os.O_DIRECTORY)
        try:
            os.fsync(directory)
        finally:
            os.close(directory)


if __name__ == "__main__":
    repair(sys.argv[1] if len(sys.argv) > 1 else "/var/db/gaussdb")
