#!/usr/bin/env python3
"""Minimal coordinator launcher; legacy/local commands keep the full CLI parser."""
import os
import sys
from coordinator_client import request


def option_value(argv, name, default=None):
    value = default
    for index, word in enumerate(argv):
        if word == name:
            if index + 1 == len(argv) or argv[index + 1].startswith("--"):
                raise ValueError("missing value for " + name)
            value = argv[index + 1]
        elif word.startswith(name + "="):
            value = word.split("=", 1)[1]
    return value


def fast_config(argv, root, environ):
    is_pitr = bool(argv and argv[0] == "pitr")
    is_branch = len(argv) >= 2 and argv[0] == "branch" and argv[1] in ("create", "delete")
    if not (is_pitr or is_branch):
        return None
    excluded = ("-h", "--help", "--") if is_pitr else ("-h", "--help", "--dry-run", "--")
    if any(word in excluded for word in argv):
        return None
    # Let argparse interpret abbreviated control flags. Appending a default
    # namespace after an unrecognized abbreviation could otherwise override it.
    controls = ("--namespace", "--timeout", "--dry-run", "--help")
    if is_pitr:
        controls += ("--time",)
    for word in argv:
        option = word.split("=", 1)[0]
        if option.startswith("--") and option not in controls and any(flag.startswith(option) for flag in controls):
            return None
    try:
        if (is_pitr or argv[1] == "create") and not option_value(argv, "--from-pool"):
            return None
        namespace = option_value(argv, "--namespace")
        timeout = int(option_value(argv, "--timeout", "300"))
    except ValueError:
        return None  # Preserve the original parser's usage/error reporting.
    values = {}
    try:
        with open(os.path.join(root, "quick-start-branching.env"), encoding="utf-8") as file:
            for raw_line in file:
                line = raw_line.strip()
                if line and not line.startswith("#") and "=" in line:
                    key, value = line.split("=", 1)
                    values[key.strip()] = value
    except FileNotFoundError:
        pass

    def configured(key, environment, default=""):
        return values.get(key, environ.get(environment, default))

    url = configured("COORDINATOR_URL", "K8S_LOCAL_COORDINATOR_URL").rstrip("/")
    if not url:
        return None
    if namespace is None:
        namespace = configured("NAMESPACE", "K8S_LOCAL_NAMESPACE", "bw")
    token_file = os.path.expanduser(configured("COORDINATOR_TOKEN_FILE", "K8S_LOCAL_COORDINATOR_TOKEN_FILE"))
    # Relative credential paths are anchored to deploy_k8s, not the caller cwd.
    if token_file and not os.path.isabs(token_file):
        token_file = os.path.join(os.path.abspath(root), token_file)
    return {"url": url,
            "token_file": token_file,
            "argv": list(argv) + ["--namespace", namespace],
            "timeout": timeout}


def main():
    argv = sys.argv[1:]
    bin_dir = os.path.dirname(os.path.abspath(__file__))
    config = fast_config(argv, os.path.dirname(bin_dir), os.environ)
    if config is None:
        legacy = os.path.join(bin_dir, "k8s_local.py")
        os.execv(sys.executable, [sys.executable, legacy] + argv)
    try:
        result = request(config)
    except Exception as error:
        print("k8s_local: " + str(error), file=sys.stderr)
        return 1
    import json
    print(json.dumps(result, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    sys.exit(main())
