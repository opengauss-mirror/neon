#!/usr/bin/env python3
import argparse
import fnmatch
import json
import os
import shutil
import socket
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request

try:
    import tomllib
except ModuleNotFoundError:
    import tomli as tomllib


SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
COMPOSE_DIR = os.path.dirname(SCRIPT_DIR)
REPO_DIR = os.path.dirname(COMPOSE_DIR)
CP_URL = os.environ.get("DOCKER_CONTROL_PLANE_URL", "http://127.0.0.1:8080")
DEFAULT_STORAGE_CONTROLLER_URL = "http://127.0.0.1:1234"
DEFAULT_STORAGE_IMAGE = os.environ.get("NEON_IMAGE", "og_storage:latest")
DEFAULT_COMPUTE_IMAGE = os.environ.get("COMPUTE_IMAGE", "og_compute:latest")
DEFAULT_OG_VERSION = os.environ.get("OG_VERSION", "V702")
DEFAULT_COMPOSE_PROJECT = os.environ.get("COMPOSE_PROJECT_NAME", "neon_poc")
DEFAULT_NUM_PAGE_SERVERS = 1
DEFAULT_NUM_SAFEKEEPERS = 1
EXTRA_COMPOSE_FILES = [
    item
    for item in os.environ.get("DOCKER_LOCAL_EXTRA_COMPOSE_FILES", "").split(os.pathsep)
    if item
]

CORE_SERVICES = [
    "data_permissions",
    "docker_control_plane",
    "storage_controller_db",
    "storage_broker",
    "storage_controller",
    "pageserver",
    "safekeeper",
    "endpoint_storage",
]

STOP_SERVICES = [
    "endpoint_storage",
    "pageserver",
    "safekeeper",
    "storage_controller",
    "storage_broker",
    "storage_controller_db",
]

SERVICE_ALIASES = {
    "control-plane": "docker_control_plane",
    "docker-control-plane": "docker_control_plane",
    "docker_control_plane": "docker_control_plane",
    "endpoint-storage": "endpoint_storage",
    "endpoint_storage": "endpoint_storage",
    "storage-controller": "storage_controller",
    "storage_controller": "storage_controller",
    "storage-controller-db": "storage_controller_db",
    "storage_controller_db": "storage_controller_db",
    "storage-broker": "storage_broker",
    "storage_broker": "storage_broker",
    "broker": "storage_broker",
    "pageserver": "pageserver",
    "safekeeper": "safekeeper",
    "safekeeper1": "safekeeper",
    "safekeeper2": "safekeeper2",
    "safekeeper3": "safekeeper3",
    "endpoint-storage": "endpoint_storage",
}


def api(method, path, body=None, timeout=120):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    req = urllib.request.Request(CP_URL + path, data=data, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode("utf-8", "replace")
        raise SystemExit(f"{method} {path} failed: HTTP {exc.code}: {payload}") from exc
    if not payload:
        return {}
    return json.loads(payload)


def storage_controller_api(method, path, body=None, timeout=120, base_url=None):
    data = None
    headers = {}
    if body is not None:
        data = json.dumps(body).encode("utf-8")
        headers["Content-Type"] = "application/json"
    storage_controller_url = base_url or os.environ.get("STORAGE_CONTROLLER_HOST_HTTP", DEFAULT_STORAGE_CONTROLLER_URL)
    req = urllib.request.Request(
        storage_controller_url + path,
        data=data,
        headers=headers,
        method=method,
    )
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode("utf-8", "replace")
        raise SystemExit(f"{method} {path} failed: HTTP {exc.code}: {payload}") from exc
    if not payload:
        return {}
    return json.loads(payload)


def http_upload_file(method, url, path, timeout=900):
    with open(path, "rb") as f:
        data = f.read()
    req = urllib.request.Request(url, data=data, method=method)
    try:
        with urllib.request.urlopen(req, timeout=timeout) as resp:
            payload = resp.read().decode("utf-8")
    except urllib.error.HTTPError as exc:
        payload = exc.read().decode("utf-8", "replace")
        raise SystemExit(f"{method} {url} failed: HTTP {exc.code}: {payload}") from exc
    if not payload:
        return {}
    return json.loads(payload)


def run(cmd, cwd=COMPOSE_DIR, check=True, capture=False, env=None):
    if capture:
        result = subprocess.run(
            cmd,
            cwd=cwd,
            check=check,
            text=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            env=env,
        )
        return result.stdout.strip()
    subprocess.run(cmd, cwd=cwd, check=check, env=env)
    return ""


def compose_cmd(*args, capture=False, check=True, compose_files=None, env=None):
    cmd = ["docker", "compose"]
    files = list(compose_files or ["docker-compose.yml"])
    files.extend(EXTRA_COMPOSE_FILES)
    for compose_file in files:
        cmd += ["-f", compose_file]
    cmd += list(args)
    return run(cmd, check=check, capture=capture, env=env)


def compose_base(*args, capture=False, check=True, env=None):
    return compose_cmd(*args, capture=capture, check=check, env=env)


def normalize_service(name):
    if name in SERVICE_ALIASES:
        return SERVICE_ALIASES[name]
    if name.startswith("storage-controller") and name[len("storage-controller"):].isdigit():
        return "storage_controller" + name[len("storage-controller"):]
    return name


def endpoint_override(endpoint_id):
    return os.path.join(".neon", "control_plane", "overrides", "endpoints", f"{endpoint_id}.yml")


def endpoint_compose_files(endpoint_id):
    override = endpoint_override(endpoint_id)
    files = ["docker-compose.yml"]
    files.extend(existing_override_files(os.path.join("safekeepers", "*.yml")))
    if os.path.exists(os.path.join(COMPOSE_DIR, override)):
        files.append(override)
    return files


def compose_endpoint(endpoint_id, *args, capture=False, check=True, env=None):
    return compose_cmd(
        *args,
        compose_files=endpoint_compose_files(endpoint_id),
        check=check,
        capture=capture,
        env=env,
    )


def existing_override_files(pattern):
    root = os.path.join(COMPOSE_DIR, ".neon", "control_plane", "overrides")
    matches = []
    for dirpath, _dirnames, filenames in os.walk(root):
        for filename in filenames:
            full_path = os.path.join(dirpath, filename)
            rel = os.path.relpath(full_path, root)
            if fnmatch.fnmatch(rel, pattern):
                matches.append(os.path.join(".neon", "control_plane", "overrides", rel))
    return sorted(matches)


def pageserver_ref(value):
    try:
        raw = int(value)
    except (TypeError, ValueError):
        raise SystemExit(f"pageserver ref must be an ordinal or node id: {value}") from None
    if raw >= 1000:
        node_id = raw
        ordinal = raw - 1000
    else:
        ordinal = raw
        node_id = 1000 + raw
    if ordinal < 1:
        raise SystemExit(f"pageserver ordinal must be positive: {value}")
    service_name = "pageserver" if ordinal == 1 else f"pageserver{ordinal}"
    return ordinal, node_id, service_name


def pageserver_arg_ref(args):
    ref = getattr(args, "ref", None)
    pageserver_id = getattr(args, "pageserver_id", None)
    if ref is not None and pageserver_id is not None:
        _, node_id, _ = pageserver_ref(ref)
        if node_id != int(pageserver_id):
            raise SystemExit(f"pageserver ref {ref} does not match --id {pageserver_id}")
    return ref if ref is not None else pageserver_id if pageserver_id is not None else 1


def pageserver_override(service_name):
    return os.path.join(".neon", "control_plane", "overrides", "pageservers", f"{service_name}.yml")


def pageserver_compose_files(service_name):
    files = ["docker-compose.yml"]
    override = pageserver_override(service_name)
    if os.path.exists(os.path.join(COMPOSE_DIR, override)):
        files.append(override)
    return files


def compose_pageserver(service_name, *args, capture=False, check=True, env=None):
    return compose_cmd(
        *args,
        compose_files=pageserver_compose_files(service_name),
        capture=capture,
        check=check,
        env=env,
    )


def safekeeper_service(sk_id):
    sk_id = int(sk_id)
    return "safekeeper" if sk_id == 1 else f"safekeeper{sk_id}"


def safekeeper_connstring_arg(value):
    value = value.strip()
    if value.isdigit():
        return f"{safekeeper_service(value)}:5454"
    return value


def safekeeper_override(sk_id):
    return os.path.join(".neon", "control_plane", "overrides", "safekeepers", f"safekeeper{int(sk_id)}.yml")


def safekeeper_compose_files(sk_id):
    files = ["docker-compose.yml"]
    if int(sk_id) == 1:
        return files
    override = safekeeper_override(sk_id)
    if os.path.exists(os.path.join(COMPOSE_DIR, override)):
        files.append(override)
    return files


def compose_safekeeper(sk_id, *args, capture=False, check=True, env=None):
    return compose_cmd(
        *args,
        compose_files=safekeeper_compose_files(sk_id),
        capture=capture,
        check=check,
        env=env,
    )


def storage_controller_service(instance_id):
    instance_id = int(instance_id)
    if instance_id < 1:
        raise SystemExit(f"storage-controller --instance-id must be positive: {instance_id}")
    return "storage_controller" if instance_id == 1 else f"storage_controller{instance_id}"


def storage_controller_override(service_name):
    return os.path.join(".neon", "control_plane", "overrides", "storage_controllers", f"{service_name}.yml")


def storage_controller_compose_files(instance_id):
    files = ["docker-compose.yml"]
    if int(instance_id) != 1:
        override = storage_controller_override(storage_controller_service(instance_id))
        if os.path.exists(os.path.join(COMPOSE_DIR, override)):
            files.append(override)
    return files


def compose_storage_controller(instance_id, *args, capture=False, check=True, env=None):
    return compose_cmd(
        *args,
        compose_files=storage_controller_compose_files(instance_id),
        capture=capture,
        check=check,
        env=env,
    )


def local_config_path():
    return os.path.join(COMPOSE_DIR, ".neon", "control_plane", "docker_local.json")


def read_local_config():
    path = local_config_path()
    if not os.path.exists(path):
        return {}
    with open(path, encoding="utf-8") as f:
        return json.load(f)


def write_local_config(config):
    path = local_config_path()
    os.makedirs(os.path.dirname(path), exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(config, f, indent=2, sort_keys=True)
        f.write("\n")
    os.replace(tmp, path)


def local_compose_project():
    return read_local_config().get("compose_project") or DEFAULT_COMPOSE_PROJECT


def parse_json_lines(output):
    if not output:
        return []
    rows = []
    for line in output.splitlines():
        line = line.strip()
        if line:
            rows.append(json.loads(line))
    return rows


def host_port_owner(port):
    try:
        output = run(
            ["docker", "ps", "--format", "{{.Names}}\t{{.Ports}}"],
            capture=True,
            check=False,
        )
    except Exception:
        return None
    needle = f":{int(port)}->"
    for line in output.splitlines():
        if "\t" not in line:
            continue
        name, ports = line.split("\t", 1)
        if needle in ports:
            return name
    return None


def host_port_available(port, owning_container=None):
    if not port:
        return True
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as sock:
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        try:
            sock.bind(("0.0.0.0", int(port)))
        except OSError:
            if owning_container and host_port_owner(port) == owning_container:
                return True
            return False
    return True


def next_available_host_port(port, owning_container=None):
    port = int(port)
    while not host_port_available(port, owning_container=owning_container):
        port += 1
    return port


def resolve_endpoint_host_ports(endpoint):
    changed = False
    resolved = dict(endpoint)
    owning_container = resolved.get("container_name")
    for field, label in [("host_pg_port", "Postgres"), ("host_http_port", "HTTP")]:
        port = resolved.get(field)
        if not port:
            continue
        next_port = next_available_host_port(port, owning_container=owning_container)
        if next_port != port:
            print(
                f"endpoint {resolved['endpoint_id']} {label} host port {port} is busy; using {next_port}",
                file=sys.stderr,
            )
            resolved[field] = next_port
            changed = True
    return resolved, changed


def resolve_pageserver_host_ports(service_name, http_port, pg_port):
    container_name = f"{local_compose_project()}-{service_name}-1"
    resolved_http = next_available_host_port(http_port, owning_container=container_name)
    resolved_pg = next_available_host_port(pg_port, owning_container=container_name)
    if resolved_http != http_port:
        print(
            f"{service_name} HTTP host port {http_port} is busy; using {resolved_http}",
            file=sys.stderr,
        )
    if resolved_pg != pg_port:
        print(
            f"{service_name} Postgres host port {pg_port} is busy; using {resolved_pg}",
            file=sys.stderr,
        )
    return resolved_http, resolved_pg


def resolve_storage_controller_host_port(service_name, http_port):
    container_name = f"{local_compose_project()}-{service_name}-1"
    resolved_http = next_available_host_port(http_port, owning_container=container_name)
    if resolved_http != http_port:
        print(
            f"{service_name} HTTP host port {http_port} is busy; using {resolved_http}",
            file=sys.stderr,
        )
    return resolved_http


def storage_controller_host_http_url(instance_id):
    service_name = storage_controller_service(instance_id)
    container_name = f"{local_compose_project()}-{service_name}-1"
    mapped = run(["docker", "port", container_name, "1234/tcp"], capture=True, check=False)
    if mapped:
        first = mapped.splitlines()[0]
        if ":" in first:
            return f"http://127.0.0.1:{first.rsplit(':', 1)[1]}"
    return f"http://127.0.0.1:{1234 + int(instance_id) - 1}"


def rewrite_endpoint_with_ports(endpoint):
    return api("POST", "/v1/endpoint", {
        "endpoint_id": endpoint["endpoint_id"],
        "tenant_id": endpoint.get("tenant_id"),
        "timeline_id": endpoint.get("timeline_id"),
        "branch_name": endpoint.get("branch_name"),
        "host_pg_port": endpoint.get("host_pg_port"),
        "host_http_port": endpoint.get("host_http_port"),
        "internal_http_port": endpoint.get("internal_http_port"),
        "endpoint_pageserver_id": endpoint.get("endpoint_pageserver_id"),
        "service_name": endpoint.get("service_name"),
        "static_lsn": endpoint.get("static_lsn"),
        "hot_standby": endpoint.get("hot_standby"),
        "autoprewarm": endpoint.get("autoprewarm"),
        "offload_lfc_interval_seconds": endpoint.get("offload_lfc_interval_seconds"),
        "pg_version": endpoint.get("pg_version"),
        "grpc": endpoint.get("grpc"),
        "config_only": endpoint.get("config_only"),
        "enable_oggit": endpoint.get("enable_oggit"),
        "oggit_database": endpoint.get("oggit_database"),
        "update_catalog": not endpoint.get("skip_pg_catalog_updates", True),
        "create_test_user": endpoint.get("create_test_user"),
        "remote_ext_base_url": endpoint.get("remote_ext_base_url"),
        "privileged_role_name": endpoint.get("privileged_role_name"),
        "safekeepers_generation": endpoint.get("safekeepers_generation"),
        "safekeeper_connstrings": endpoint.get("safekeeper_connstrings"),
        "extra_config": endpoint.get("extra_config"),
    })


def endpoint_is_primary(endpoint):
    return not endpoint.get("hot_standby") and not endpoint.get("static_lsn")


def ensure_no_running_primary_on_same_timeline(endpoint):
    if not endpoint_is_primary(endpoint):
        return
    all_endpoints = api("GET", "/v1/endpoint").get("endpoints", [])
    for other in all_endpoints:
        if other.get("endpoint_id") == endpoint.get("endpoint_id"):
            continue
        if other.get("status") != "Running":
            continue
        if not endpoint_is_primary(other):
            continue
        if (
            other.get("tenant_id") == endpoint.get("tenant_id")
            and other.get("timeline_id") == endpoint.get("timeline_id")
        ):
            raise SystemExit(
                f"refusing to start primary endpoint {endpoint['endpoint_id']} on timeline "
                f"{endpoint['timeline_id']}: endpoint {other['endpoint_id']} is already Running. "
                "Create a branch timeline first, or stop the existing endpoint."
            )


def disable_restart_policy(path):
    if not os.path.exists(path):
        return
    with open(path, encoding="utf-8") as f:
        content = f.read()
    updated = content.replace("restart: always", 'restart: "no"')
    if updated != content:
        with open(path, "w", encoding="utf-8") as f:
            f.write(updated)


def disable_endpoint_restart_policy(endpoint_id):
    disable_restart_policy(os.path.join(COMPOSE_DIR, endpoint_override(endpoint_id)))


def ensure_dir(path):
    os.makedirs(path, exist_ok=True)


def chmod_tree(path, mode=0o777):
    try:
        for root, dirs, files in os.walk(path):
            os.chmod(root, mode)
            for name in dirs:
                os.chmod(os.path.join(root, name), mode)
            for name in files:
                try:
                    os.chmod(os.path.join(root, name), mode)
                except FileNotFoundError:
                    pass
        os.chmod(path, mode)
        return
    except PermissionError:
        pass
    abs_path = os.path.abspath(path)
    run([
        "docker", "run", "--rm", "--user", "0:0",
        "-v", f"{abs_path}:/fix",
        "--entrypoint", "/bin/sh",
        DEFAULT_STORAGE_IMAGE,
        "-ec",
        "chmod -R 0777 /fix",
    ], cwd=COMPOSE_DIR)


def chmod_dir(path, mode=0o777):
    try:
        os.chmod(path, mode)
        return
    except PermissionError:
        pass
    abs_path = os.path.abspath(path)
    run([
        "docker", "run", "--rm", "--user", "0:0",
        "-v", f"{abs_path}:/fix",
        "--entrypoint", "/bin/sh",
        DEFAULT_STORAGE_IMAGE,
        "-ec",
        "chmod 0777 /fix",
    ], cwd=COMPOSE_DIR)


def safe_rmtree(path):
    if not os.path.exists(path):
        return
    chmod_tree(path)
    shutil.rmtree(path)


def prepare_endpoint_data_dir(endpoint):
    data_dir = endpoint.get("data_dir") or os.path.join(".neon", endpoint["service_name"])
    host_dir = data_dir if os.path.isabs(data_dir) else os.path.join(COMPOSE_DIR, data_dir)
    try:
        os.makedirs(host_dir, exist_ok=True)
    except PermissionError:
        pass
    run([
        "docker", "run", "--rm", "--user", "0:0",
        "-v", f"{host_dir}:/var/db/gaussdb",
        "--entrypoint", "/bin/sh",
        DEFAULT_COMPUTE_IMAGE,
        "-ec",
        "mkdir -p /var/db/gaussdb/compute && touch /var/db/gaussdb/postgresql_extend.conf && chmod -R 0777 /var/db/gaussdb",
    ], cwd=COMPOSE_DIR)


def print_json(value):
    print(json.dumps(value, indent=2, sort_keys=True))


def parse_key_value(values):
    result = {}
    for item in values or []:
        if "=" not in item:
            raise SystemExit(f"expected key=value: {item}")
        key, value = item.split("=", 1)
        if not key:
            raise SystemExit(f"empty key in {item}")
        result[key] = value
    return result


def parse_json_scalar(value):
    if value in {"true", "false", "null"}:
        return json.loads(value)
    try:
        return int(value)
    except ValueError:
        pass
    try:
        return json.loads(value)
    except json.JSONDecodeError:
        return value


def parse_colon_config(values):
    result = {}
    for item in values or []:
        if ":" not in item:
            raise SystemExit(f"expected key:value: {item}")
        key, value = item.split(":", 1)
        if not key:
            raise SystemExit(f"empty key in {item}")
        result[key] = parse_json_scalar(value)
    return result


def parse_extra_config(values):
    result = {}
    for item in values or []:
        if "=" not in item:
            raise SystemExit(f"expected key=value: {item}")
        key, value = item.split("=", 1)
        if not key:
            raise SystemExit(f"empty key in {item}")
        result[key] = parse_json_scalar(value)
    return result


def parse_placement_policy(value):
    if value is None:
        return None
    try:
        return json.loads(value)
    except json.JSONDecodeError as exc:
        raise argparse.ArgumentTypeError(f"placement policy must be JSON: {value}") from exc


def parse_storcon_placement(value):
    if value is None:
        return None
    value = str(value).strip()
    if value in {"detached", "secondary"}:
        return value
    if value.startswith("attached:"):
        try:
            count = int(value.split(":", 1)[1])
        except ValueError:
            raise argparse.ArgumentTypeError(f"placement must be detached, secondary, or attached:<n>: {value}") from None
        if count < 0:
            raise argparse.ArgumentTypeError(f"attached count must be non-negative: {value}")
        return value
    raise argparse.ArgumentTypeError(f"placement must be detached, secondary, or attached:<n>: {value}")


def parse_shard_scheduling(value):
    value = str(value).strip()
    if value not in {"active", "essential", "pause", "stop"}:
        raise argparse.ArgumentTypeError("scheduling must be one of: active, essential, pause, stop")
    return value


def parse_safekeeper_scheduling(value):
    value = str(value).strip()
    if value not in {"active", "activating", "pause", "decomissioned"}:
        raise argparse.ArgumentTypeError("safekeeper scheduling must be one of: active, activating, pause, decomissioned")
    return value


def parse_pg_version(value):
    raw = str(value)
    normalized = raw.strip().lower()
    if normalized == "v702":
        return 14
    for prefix in ("pg", "v"):
        if normalized.startswith(prefix):
            normalized = normalized[len(prefix):]
            break
    try:
        version = int(normalized)
    except ValueError:
        raise argparse.ArgumentTypeError(f"unsupported pg version: {raw}") from None
    if version not in {14, 15, 16, 17}:
        raise argparse.ArgumentTypeError(f"unsupported pg version: {raw}")
    return version


def parse_bool(value):
    normalized = str(value).strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    raise argparse.ArgumentTypeError(f"expected boolean value, got {value!r}")


def init_counts_from_config(path):
    with open(path, "rb") as f:
        config = tomllib.load(f)
    return {
        "num_pageservers": len(config.get("pageservers") or []) or None,
        "num_safekeepers": len(config.get("safekeepers") or []) or None,
    }
