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
    control_plane_token = os.environ.get("DOCKER_CONTROL_PLANE_TOKEN") or os.environ.get(
        "CONTROL_PLANE_JWT_TOKEN"
    )
    if control_plane_token:
        headers["Authorization"] = f"Bearer {control_plane_token}"
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


def wait_http(path, timeout=120):
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            return api("GET", path, timeout=5)
        except Exception:
            time.sleep(1)
    raise SystemExit(f"timed out waiting for {path}")


def wait_storage_controller(timeout=120, base_url=None):
    deadline = time.time() + timeout
    last_error = None
    while time.time() < deadline:
        try:
            return storage_controller_api("GET", "/control/v1/node", timeout=5, base_url=base_url)
        except Exception as exc:
            last_error = exc
            time.sleep(1)
    raise SystemExit(f"timed out waiting for storage_controller: {last_error}")


def wait_endpoint(endpoint_id, timeout=180):
    deadline = time.time() + timeout
    while time.time() < deadline:
        endpoint = api("GET", f"/v1/endpoint/{endpoint_id}")
        if endpoint.get("status") == "Running":
            return endpoint
        time.sleep(1)
    raise SystemExit(f"timed out waiting for endpoint {endpoint_id} to become Running")


def compose_endpoint_json(endpoint_id, *args):
    output = compose_endpoint(endpoint_id, *args, "--format", "json", capture=True, check=False)
    if not output:
        return []
    return [json.loads(line) for line in output.splitlines() if line.strip()]


def endpoint_tail_logs(endpoint_id, service, lines=80):
    return compose_endpoint(
        endpoint_id,
        "logs",
        f"--tail={lines}",
        service,
        capture=True,
        check=False,
    )


def wait_endpoint_started(endpoint_id, service, timeout=180):
    deadline = time.time() + timeout
    last_status = None
    while time.time() < deadline:
        endpoint = api("GET", f"/v1/endpoint/{endpoint_id}")
        last_status = endpoint.get("status")
        if last_status == "Running":
            return endpoint
        if last_status in {"Failed", "Exited"}:
            logs = endpoint_tail_logs(endpoint_id, service)
            raise SystemExit(
                f"endpoint {endpoint_id} entered {last_status}; last logs:\n{logs}"
            )

        rows = compose_endpoint_json(endpoint_id, "ps", service)
        if rows:
            state = rows[0].get("State") or rows[0].get("Status")
            exit_code = rows[0].get("ExitCode")
            if state in {"exited", "dead"} or exit_code not in {None, 0, "0"}:
                logs = endpoint_tail_logs(endpoint_id, service)
                raise SystemExit(
                    f"endpoint {endpoint_id} container is {state} exit_code={exit_code}; "
                    f"control-plane status={last_status}; last logs:\n{logs}"
                )

        logs = endpoint_tail_logs(endpoint_id, service, lines=30)
        if "could not start the compute node" in logs or "compute node is now in Failed state" in logs:
            raise SystemExit(
                f"endpoint {endpoint_id} compute failed while control-plane status is {last_status}; "
                f"last logs:\n{logs}"
            )
        time.sleep(1)
    logs = endpoint_tail_logs(endpoint_id, service)
    raise SystemExit(
        f"timed out waiting for endpoint {endpoint_id} to become Running "
        f"(last status={last_status}); last logs:\n{logs}"
    )


def endpoint_connstr(endpoint_id, user="branch_merge", password="Branch_merge%40123"):
    endpoint = api("GET", f"/v1/endpoint/{endpoint_id}")
    port = endpoint.get("host_pg_port")
    if not port:
        raise SystemExit(f"endpoint {endpoint_id} has no host_pg_port")
    return f"postgresql://{user}:{password}@127.0.0.1:{port}/postgres?sslmode=disable"


def pageserver_nodes():
    return storage_controller_api("GET", "/control/v1/node")


def pageserver_node(node_id):
    for node in pageserver_nodes():
        if node.get("id") == node_id:
            return node
    return None


def wait_pageserver_registered(node_id, service_name, timeout=120):
    container_name = f"{local_compose_project()}-{service_name}-1"
    deadline = time.time() + timeout
    while time.time() < deadline:
        node = pageserver_node(node_id)
        if node and node.get("availability") == "Active":
            return node
        state = run(
            ["docker", "inspect", "-f", "{{.State.Status}} {{.State.ExitCode}}", container_name],
            capture=True,
            check=False,
        )
        if state.startswith(("exited", "dead", "restarting")):
            logs = run(["docker", "logs", "--tail=120", container_name], capture=True, check=False)
            raise SystemExit(
                f"{service_name} failed before registering in storage_controller; last logs:\n{logs}"
            )
        time.sleep(1)
    logs = run(["docker", "logs", "--tail=120", container_name], capture=True, check=False)
    raise SystemExit(
        f"timed out waiting for {service_name} node_id={node_id} to register in storage_controller; "
        f"last logs:\n{logs}"
    )


def tenant_shards(tenant_id=None):
    path = "/v1/pageserver/shards"
    if tenant_id:
        path += "?" + urllib.parse.urlencode({"tenant_id": tenant_id})
    return api("GET", path)


def resolve_tenant_id(explicit=None):
    if explicit:
        return explicit
    env = api("GET", "/v1/env")
    tenant_id = env.get("default_tenant_id")
    if tenant_id:
        return tenant_id
    mappings = api("GET", "/v1/mappings")
    main = mappings.get("main")
    if main and main.get("tenant_id"):
        return main["tenant_id"]
    raise SystemExit("tenant_id is required when no default tenant or main mapping exists")


def migrate_tenant_shard(tenant_shard_id, node_id, origin_node_id=None, prewarm=None, override_scheduler=False, timeout=None):
    body = {"node_id": node_id}
    if origin_node_id is not None:
        body["origin_node_id"] = origin_node_id
    if prewarm is not None:
        body["prewarm"] = prewarm
    if override_scheduler:
        body["override_scheduler"] = override_scheduler
    if timeout is not None:
        body["timeout_seconds"] = timeout
    return api(
        "POST",
        f"/v1/pageserver/{tenant_shard_id}/migrate",
        body,
        timeout=(timeout + 5) if timeout is not None else None,
    )


def pageserver_host_http_url(node_id):
    ordinal, _node_id, _service_name = pageserver_ref(node_id)
    service_name = "pageserver" if ordinal == 1 else f"pageserver{ordinal}"
    container_name = f"{local_compose_project()}-{service_name}-1"
    mapped = run(["docker", "port", container_name, "9898/tcp"], capture=True, check=False)
    if mapped:
        first = mapped.splitlines()[0]
        if ":" in first:
            return f"http://127.0.0.1:{first.rsplit(':', 1)[1]}"
    return f"http://127.0.0.1:{9897 + ordinal}"


def safekeeper_ids_from_overrides():
    root = os.path.join(COMPOSE_DIR, ".neon", "control_plane", "overrides", "safekeepers")
    ids = set()
    if not os.path.isdir(root):
        return ids
    for name in os.listdir(root):
        if not name.startswith("safekeeper") or not name.endswith(".yml"):
            continue
        raw = name[len("safekeeper"):-len(".yml")]
        if raw.isdigit():
            ids.add(int(raw))
    return ids


def configured_safekeeper_ids():
    config = read_local_config()
    count = int(config.get("num_safekeepers") or DEFAULT_NUM_SAFEKEEPERS)
    ids = set(range(1, count + 1))
    ids.update(safekeeper_ids_from_overrides())
    return sorted(ids)


def persist_num_safekeepers_at_least(sk_id):
    config = read_local_config()
    current = int(config.get("num_safekeepers") or DEFAULT_NUM_SAFEKEEPERS)
    if int(sk_id) <= current:
        return
    config["num_safekeepers"] = int(sk_id)
    write_local_config(config)


def persist_num_pageservers_at_least(ordinal):
    config = read_local_config()
    current = int(config.get("num_pageservers") or DEFAULT_NUM_PAGE_SERVERS)
    if int(ordinal) <= current:
        return
    config["num_pageservers"] = int(ordinal)
    write_local_config(config)


def ensure_safekeeper_overrides_for_count(count, image=DEFAULT_STORAGE_IMAGE, og_version=DEFAULT_OG_VERSION):
    for sk_id in range(2, int(count) + 1):
        override = os.path.join(COMPOSE_DIR, safekeeper_override(sk_id))
        if os.path.exists(override):
            continue
        write_safekeeper_override(
            sk_id,
            safekeeper_http_host_port(sk_id),
            image=image,
            og_version=og_version,
            broker_endpoint=os.environ.get("BROKER_ENDPOINT", "http://storage_broker:50051"),
        )


def safekeeper_http_host_port(sk_id):
    return 7675 + int(sk_id)


def parse_id_list(value, label):
    ids = []
    for item in str(value).split(","):
        item = item.strip()
        if not item:
            continue
        try:
            ids.append(int(item))
        except ValueError:
            raise SystemExit(f"{label} must be a comma-separated integer list: {value}") from None
    if not ids:
        raise SystemExit(f"{label} must not be empty")
    return ids


def storage_controller_safekeepers(base_url=None):
    return storage_controller_api("GET", "/control/v1/safekeeper", base_url=base_url)


def storage_controller_safekeeper(sk_id, base_url=None):
    sk_id = int(sk_id)
    try:
        for row in storage_controller_safekeepers(base_url=base_url):
            if int(row.get("id")) == sk_id:
                return row
    except Exception:
        return None
    return None


def register_safekeeper(sk_id, timeout=120, base_url=None):
    sk_id = int(sk_id)
    service = safekeeper_service(sk_id)
    body = {
        "id": sk_id,
        "region_id": "local",
        "version": 1,
        "host": service,
        "port": 5454,
        "http_port": 7676,
        "https_port": None,
        "availability_zone_id": f"local-{sk_id}",
    }
    storage_controller_api(
        "POST",
        f"/control/v1/safekeeper/{sk_id}",
        body,
        timeout=timeout,
        base_url=base_url,
    )
    storage_controller_api(
        "POST",
        f"/control/v1/safekeeper/{sk_id}/scheduling_policy",
        {"scheduling_policy": "Active"},
        timeout=timeout,
        base_url=base_url,
    )
    return storage_controller_safekeeper(sk_id, base_url=base_url)


def register_configured_safekeepers(timeout=120, base_url=None):
    wait_storage_controller(timeout=timeout, base_url=base_url)
    rows = []
    for sk_id in configured_safekeeper_ids():
        rows.append(register_safekeeper(sk_id, timeout=timeout, base_url=base_url))
    return rows


def maybe_register_safekeeper(sk_id, timeout=120):
    try:
        wait_storage_controller(timeout=min(timeout, 30))
        return register_safekeeper(sk_id, timeout=timeout)
    except SystemExit as exc:
        print(f"warning: could not register {safekeeper_service(sk_id)} in storage_controller: {exc}", file=sys.stderr)
    except Exception as exc:
        print(f"warning: could not register {safekeeper_service(sk_id)} in storage_controller: {exc}", file=sys.stderr)
    return None


def set_safekeeper_scheduling_policy(sk_id, policy, timeout=120):
    return storage_controller_api(
        "POST",
        f"/control/v1/safekeeper/{int(sk_id)}/scheduling_policy",
        {"scheduling_policy": policy},
        timeout=timeout,
    )


def write_safekeeper_override(sk_id, host_http_port, image=DEFAULT_STORAGE_IMAGE, og_version=DEFAULT_OG_VERSION, broker_endpoint="http://storage_broker:50051"):
    sk_id = int(sk_id)
    if sk_id == 1:
        raise SystemExit("safekeeper1 is the built-in safekeeper service named 'safekeeper'")
    service = safekeeper_service(sk_id)
    override = os.path.join(COMPOSE_DIR, safekeeper_override(sk_id))
    os.makedirs(os.path.dirname(override), exist_ok=True)
    ensure_dir(os.path.join(COMPOSE_DIR, ".neon", service))
    chmod_dir(os.path.join(COMPOSE_DIR, ".neon", service))
    content = f"""services:
  {service}:
    restart: "no"
    image: ${{NEON_IMAGE:-{image}}}
    pull_policy: never
    environment:
      - OG_VERSION=${{OG_VERSION:-{og_version}}}
      - PATH=/usr/local/${{OG_VERSION:-{og_version}}}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
      - SAFEKEEPER_ADVERTISE_URL={service}:5454
      - SAFEKEEPER_ID={sk_id}
      - BROKER_ENDPOINT={broker_endpoint}
      - SAFEKEEPER_EXTRA_OPT=${{SAFEKEEPER_EXTRA_OPT:-}}
    ports:
      - {host_http_port}:7676
    volumes:
      - ./.neon/{service}:/data/.neon/{service}
    entrypoint: ["/bin/sh", "-ec"]
    command:
      - |
        exec safekeeper \\
          --listen-pg=$$SAFEKEEPER_ADVERTISE_URL \\
          --listen-http=0.0.0.0:7676 \\
          --id=$$SAFEKEEPER_ID \\
          --broker-endpoint=$$BROKER_ENDPOINT \\
          $$SAFEKEEPER_EXTRA_OPT \\
          -D /data/.neon/{service}
    depends_on:
      data_permissions:
        condition: service_completed_successfully
      storage_broker:
        condition: service_started
"""
    with open(override, "w", encoding="utf-8") as f:
        f.write(content)
    return os.path.relpath(override, COMPOSE_DIR)


def write_storage_controller_override(instance_id, host_http_port, image=DEFAULT_STORAGE_IMAGE, og_version=DEFAULT_OG_VERSION):
    instance_id = int(instance_id)
    if instance_id == 1:
        return None
    service = storage_controller_service(instance_id)
    override = os.path.join(COMPOSE_DIR, storage_controller_override(service))
    os.makedirs(os.path.dirname(override), exist_ok=True)
    ensure_dir(os.path.join(COMPOSE_DIR, ".neon", f"storage_controller_{instance_id}"))
    content = f"""services:
  {service}:
    restart: "no"
    image: ${{NEON_IMAGE:-{image}}}
    pull_policy: never
    environment:
      - OG_VERSION=${{OG_VERSION:-{og_version}}}
      - PATH=/usr/local/${{OG_VERSION:-{og_version}}}/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
      - STORAGE_CONTROLLER_EXTRA_ARGS=${{STORAGE_CONTROLLER_EXTRA_ARGS:-}}
    ports:
      - {host_http_port}:1234
    volumes:
      - ./.neon:/data/.neon
    command: >
      /bin/sh -ec 'mkdir -p /data/.neon/storage_controller_{instance_id}
      && exec storage_controller
      --listen=0.0.0.0:1234
      --address-for-peers=http://{service}:1234
      --database-url=postgresql://omm@storage_controller_db:5432/storage_controller
      --db-connect-timeout=60s
      --dev
      --timelines-onto-safekeepers
      --control-plane-url=http://docker_control_plane:8080
      $$STORAGE_CONTROLLER_EXTRA_ARGS'
    depends_on:
      - docker_control_plane
      - storage_broker
      - storage_controller_db
"""
    with open(override, "w", encoding="utf-8") as f:
        f.write(content)
    return os.path.relpath(override, COMPOSE_DIR)


def cmd_init(args):
    config_counts = {}
    if args.config:
        if args.num_pageservers is not None:
            raise SystemExit("Cannot specify both --num-pageservers and --config")
        config_counts = init_counts_from_config(args.config)
    num_pageservers = args.num_pageservers or config_counts.get("num_pageservers") or DEFAULT_NUM_PAGE_SERVERS
    num_safekeepers = args.num_safekeepers or config_counts.get("num_safekeepers") or DEFAULT_NUM_SAFEKEEPERS
    if args.force == "remove-all-contents":
        compose_base("down", "--remove-orphans", check=False)
        safe_rmtree(os.path.join(COMPOSE_DIR, ".neon"))
    elif args.force == "must-not-exist" and os.path.exists(os.path.join(COMPOSE_DIR, ".neon")):
        raise SystemExit(".neon already exists; pass --force=remove-all-contents to reinitialize")
    state_root = os.path.join(COMPOSE_DIR, ".neon")
    control_plane_dir = os.path.join(state_root, "control_plane")
    os.makedirs(control_plane_dir, exist_ok=True)
    chmod_dir(state_root)
    chmod_tree(control_plane_dir)
    compose_base("up", "-d", "docker_control_plane")
    wait_http("/ready", timeout=args.timeout)
    result = api("POST", "/v1/init", {
        "compose_project": args.compose_project,
        "og_version": args.og_version,
        "storage_image": args.storage_image,
        "compute_image": args.compute_image,
        "num_pageservers": num_pageservers,
        "num_safekeepers": num_safekeepers,
    })
    write_local_config({
        "compose_project": args.compose_project,
        "og_version": args.og_version,
        "storage_image": args.storage_image,
        "compute_image": args.compute_image,
        "num_pageservers": num_pageservers,
        "num_safekeepers": num_safekeepers,
        "config": args.config,
    })
    print_json(result)


def ensure_requested_dynamic_services(timeout=120):
    config = read_local_config()
    page_count = int(config.get("num_pageservers") or DEFAULT_NUM_PAGE_SERVERS)
    sk_count = int(config.get("num_safekeepers") or DEFAULT_NUM_SAFEKEEPERS)
    storage_image = config.get("storage_image") or DEFAULT_STORAGE_IMAGE
    og_version = config.get("og_version") or DEFAULT_OG_VERSION

    for ordinal in range(2, page_count + 1):
        node_id = 1000 + ordinal
        service_name = "pageserver" if ordinal == 1 else f"pageserver{ordinal}"
        if pageserver_node(node_id):
            continue
        args = argparse.Namespace(
            pageserver_cmd="add",
            ordinal=str(ordinal),
            node_id=node_id,
            http_port=None,
            pg_port=None,
            image=storage_image,
            og_version=og_version,
            storage_controller_http=os.environ.get("STORAGE_CONTROLLER_HTTP", "http://storage_controller:1234"),
            broker_endpoint=os.environ.get("BROKER_ENDPOINT", "http://storage_broker:50051"),
            timeout=timeout,
        )
        print(f"Ensuring {service_name} node_id={node_id}...", file=sys.stderr)
        cmd_pageserver(args)

    ensure_safekeeper_overrides_for_count(sk_count, image=storage_image, og_version=og_version)
    for sk_id in range(2, sk_count + 1):
        args = argparse.Namespace(
            safekeeper_cmd="add",
            id=sk_id,
            http_port=None,
            image=storage_image,
            og_version=og_version,
            broker_endpoint=os.environ.get("BROKER_ENDPOINT", "http://storage_broker:50051"),
            timeout=timeout,
        )
        print(f"Ensuring safekeeper{sk_id}...", file=sys.stderr)
        cmd_safekeeper(args)


def cmd_start(args):
    if args.service:
        services = [normalize_service(s) for s in args.service]
    else:
        config = read_local_config()
        sk_count = int(config.get("num_safekeepers") or DEFAULT_NUM_SAFEKEEPERS)
        ensure_safekeeper_overrides_for_count(
            sk_count,
            image=config.get("storage_image") or DEFAULT_STORAGE_IMAGE,
            og_version=config.get("og_version") or DEFAULT_OG_VERSION,
        )
        services = [
            "data_permissions",
            "docker_control_plane",
            "storage_controller_db",
            "storage_broker",
            "storage_controller",
            "pageserver",
            "safekeeper",
            "endpoint_storage",
        ]
    compose_base("up", "-d", "--force-recreate", *services)
    if "docker_control_plane" in services:
        wait_http("/ready", timeout=args.timeout)
    if not args.service:
        wait_storage_controller(timeout=args.timeout)
        ensure_requested_dynamic_services(timeout=args.timeout)
        register_configured_safekeepers(timeout=args.timeout)
        print_json(api("POST", "/v1/start", {}))


def cmd_stop(args):
    mode = getattr(args, "mode", "fast")
    if args.service:
        services = [normalize_service(s) for s in args.service]
    else:
        config = read_local_config()
        page_count = int(config.get("num_pageservers") or DEFAULT_NUM_PAGE_SERVERS)
        sk_count = int(config.get("num_safekeepers") or DEFAULT_NUM_SAFEKEEPERS)
        services = [
            "endpoint_storage",
            "pageserver",
            "safekeeper",
            "storage_controller",
            "storage_broker",
            "storage_controller_db",
        ]
        for ordinal in range(2, page_count + 1):
            service = f"pageserver{ordinal}"
            if mode == "immediate":
                compose_pageserver(service, "kill", service, check=False)
            else:
                compose_pageserver(service, "stop", "--timeout", str(args.timeout), service, check=False)
        for sk_id in range(2, sk_count + 1):
            service = safekeeper_service(sk_id)
            if mode == "immediate":
                compose_safekeeper(sk_id, "kill", service, check=False)
            else:
                compose_safekeeper(sk_id, "stop", "--timeout", str(args.timeout), service, check=False)
    if not args.service:
        try:
            status = api("GET", "/v1/status")
            for endpoint in status.get("endpoints", []):
                if endpoint.get("status") == "Running":
                    stop_endpoint(endpoint["endpoint_id"], quiet=True, mode=mode)
        except Exception as exc:
            print(f"warning: failed to stop running endpoints via control-plane: {exc}", file=sys.stderr)
    if args.include_control_plane and "docker_control_plane" not in services:
        services.append("docker_control_plane")
    if mode == "immediate":
        compose_base("kill", *services)
    else:
        compose_base("stop", "--timeout", str(args.timeout), *services)


def cmd_status(_args):
    print_json(api("GET", "/v1/status"))


def cmd_ps(args):
    cmd = ["ps"]
    if args.all:
        cmd.append("--all")
    if args.format:
        cmd += ["--format", args.format]
    if args.service:
        cmd += [normalize_service(s) for s in args.service]
    compose_base(*cmd)


def cmd_logs(args):
    cmd = ["logs", f"--tail={args.tail}"]
    if args.follow:
        cmd.append("--follow")
    if args.service:
        cmd += [normalize_service(s) for s in args.service]
    compose_base(*cmd)


def cmd_service(args):
    storage_controller_instance = None
    if args.service_name == "storage_controller":
        storage_controller_instance = int(getattr(args, "instance_id", 1))
        service = storage_controller_service(storage_controller_instance)
    else:
        service = normalize_service(args.service_name)

    def compose_service(*compose_args, capture=False, check=True, env=None):
        if storage_controller_instance is None:
            return compose_base(*compose_args, capture=capture, check=check, env=env)
        return compose_storage_controller(
            storage_controller_instance,
            *compose_args,
            capture=capture,
            check=check,
            env=env,
        )

    if args.service_cmd == "status":
        compose_service("ps", service)
    elif args.service_cmd == "logs":
        compose_service("logs", f"--tail={args.tail}", service)
    elif args.service_cmd == "start":
        env = os.environ.copy()
        up_args = ["up", "-d"]
        storage_controller_base_url = None
        if storage_controller_instance is not None:
            config = read_local_config()
            extra_args = []
            if getattr(args, "handle_ps_local_disk_loss", None) is True:
                extra_args.append("--handle-ps-local-disk-loss")
            if extra_args:
                env["STORAGE_CONTROLLER_EXTRA_ARGS"] = " ".join(extra_args)
                up_args.append("--force-recreate")
            requested_port = getattr(args, "base_port", None)
            if requested_port is None:
                requested_port = 1234 + storage_controller_instance - 1
            host_port = resolve_storage_controller_host_port(service, requested_port)
            storage_controller_base_url = f"http://127.0.0.1:{host_port}"
            if storage_controller_instance == 1:
                env["STORAGE_CONTROLLER_HOST_PORT"] = str(host_port)
                os.environ["STORAGE_CONTROLLER_HOST_HTTP"] = storage_controller_base_url
                up_args.append("--force-recreate")
            else:
                write_storage_controller_override(
                    storage_controller_instance,
                    host_port,
                    image=config.get("storage_image") or DEFAULT_STORAGE_IMAGE,
                    og_version=config.get("og_version") or DEFAULT_OG_VERSION,
                )
                if host_port != requested_port or extra_args:
                    up_args.append("--force-recreate")
        compose_service(*up_args, service, env=env)
        if service == "docker_control_plane":
            wait_http("/ready", timeout=args.timeout)
        if storage_controller_instance is not None:
            wait_storage_controller(timeout=args.timeout, base_url=storage_controller_base_url)
            register_configured_safekeepers(
                timeout=args.timeout,
                base_url=storage_controller_base_url,
            )
    elif args.service_cmd == "stop":
        compose_service("stop", "--timeout", str(args.timeout), service)
    elif args.service_cmd == "restart":
        if storage_controller_instance is not None:
            compose_service("up", "-d", "--force-recreate", service)
        else:
            compose_service("restart", "--timeout", str(args.timeout), service)
        if service == "docker_control_plane":
            wait_http("/ready", timeout=args.timeout)
        if storage_controller_instance is not None:
            base_url = storage_controller_host_http_url(storage_controller_instance)
            wait_storage_controller(timeout=args.timeout, base_url=base_url)
            register_configured_safekeepers(timeout=args.timeout, base_url=base_url)


def cmd_tenant(args):
    if args.tenant_cmd == "list":
        print_json(api("GET", "/v1/tenant"))
    elif args.tenant_cmd == "describe":
        tenant_id = args.tenant_id or args.tenant_id_pos
        if not tenant_id:
            tenant_id = resolve_tenant_id(None)
        print_json(api("GET", f"/v1/tenant/{tenant_id}"))
    elif args.tenant_cmd == "create":
        config = parse_colon_config(args.config)
        print_json(api("POST", "/v1/tenant", {
            "tenant_id": args.tenant_id,
            "timeline_id": args.timeline_id,
            "branch_name": args.branch_name,
            "set_default": args.set_default,
            "pg_version": args.pg_version,
            "shard_count": args.shard_count,
            "shard_stripe_size": args.shard_stripe_size,
            "placement_policy": args.placement_policy,
            "config": config,
        }))
    elif args.tenant_cmd == "locate":
        print_json(api("GET", f"/v1/tenant/{args.tenant_id}/locate"))
    elif args.tenant_cmd == "delete":
        tenant_id = args.tenant_id or args.tenant_id_pos
        if not tenant_id:
            tenant_id = resolve_tenant_id(None)
        print_json(api("DELETE", f"/v1/tenant/{tenant_id}"))
    elif args.tenant_cmd == "policy":
        tenant_id = args.tenant_id or args.tenant_id_pos
        if not tenant_id:
            tenant_id = resolve_tenant_id(None)
        if args.placement is None and args.scheduling is None:
            raise SystemExit("tenant policy requires --placement and/or --scheduling")
        print_json(api("PUT", f"/v1/tenant/{tenant_id}/policy", {
            "placement": args.placement,
            "scheduling": args.scheduling,
        }))
    elif args.tenant_cmd == "shard-split":
        tenant_id = args.tenant_id or args.tenant_id_pos
        if not tenant_id:
            tenant_id = resolve_tenant_id(None)
        print_json(api("POST", f"/v1/tenant/{tenant_id}/shard-split", {
            "shard_count": args.shard_count,
            "stripe_size": args.stripe_size,
        }))
    elif args.tenant_cmd == "set-preferred-az":
        tenant_id = args.tenant_id or args.tenant_id_pos
        if not tenant_id:
            tenant_id = resolve_tenant_id(None)
        print_json(api("PUT", f"/v1/tenant/{tenant_id}/preferred-az", {
            "preferred_az": args.preferred_az,
        }))
    elif args.tenant_cmd == "set-default":
        tenant_id = args.tenant_id or args.tenant_id_pos
        if not tenant_id:
            raise SystemExit("tenant set-default requires TENANT_ID or --tenant-id")
        print_json(api("POST", f"/v1/tenant/{tenant_id}/set-default", {}))
    elif args.tenant_cmd == "config":
        tenant_id = args.tenant_id or args.tenant_id_pos
        if not tenant_id:
            raise SystemExit("tenant config requires TENANT_ID or --tenant-id")
        print_json(api("PUT", f"/v1/tenant/{tenant_id}/config", {
            "config": parse_colon_config(args.config),
        }))
    elif args.tenant_cmd == "import":
        tenant_id = args.tenant_id or args.tenant_id_pos
        if not tenant_id:
            raise SystemExit("tenant import requires TENANT_ID or --tenant-id")
        print_json(api("POST", f"/v1/tenant/{tenant_id}/import", {}))


def cmd_timeline(args):
    branch_name = getattr(args, "branch_name", None)
    branch_name_pos = getattr(args, "branch_name_pos", None)
    if branch_name and branch_name_pos and branch_name != branch_name_pos:
        raise SystemExit(
            f"branch name mismatch: positional {branch_name_pos!r} != --branch-name {branch_name!r}"
        )
    branch_name = branch_name or branch_name_pos

    if args.timeline_cmd == "list":
        path = "/v1/timeline"
        if getattr(args, "tenant_id", None):
            path += "?" + urllib.parse.urlencode({"tenant_id": args.tenant_id})
        print_json(api("GET", path))
    elif args.timeline_cmd == "branch":
        if not branch_name:
            raise SystemExit("timeline branch requires BRANCH_NAME or --branch-name")
        ancestor_timeline_id = args.ancestor_timeline_id
        if args.ancestor_branch_name:
            mappings = api("GET", "/v1/mappings")
            mapping = mappings.get(args.ancestor_branch_name)
            if not mapping:
                raise SystemExit(f"Found no timeline id for branch name {args.ancestor_branch_name!r}")
            mapped_timeline_id = mapping.get("timeline_id")
            if ancestor_timeline_id and ancestor_timeline_id != mapped_timeline_id:
                raise SystemExit(
                    f"ancestor mismatch: --ancestor-timeline-id {ancestor_timeline_id!r} "
                    f"!= timeline for --ancestor-branch-name {args.ancestor_branch_name!r} ({mapped_timeline_id!r})"
                )
            ancestor_timeline_id = mapped_timeline_id
        print_json(api("POST", "/v1/timeline/branch", {
            "tenant_id": args.tenant_id,
            "branch_name": branch_name,
            "ancestor_timeline_id": ancestor_timeline_id,
            "ancestor_start_lsn": args.ancestor_start_lsn,
            "timeline_id": args.timeline_id,
        }))
    elif args.timeline_cmd == "create":
        if not branch_name:
            raise SystemExit("timeline create requires BRANCH_NAME or --branch-name")
        print_json(api("POST", "/v1/timeline", {
            "tenant_id": args.tenant_id,
            "branch_name": branch_name,
            "timeline_id": args.timeline_id,
            "pg_version": args.pg_version,
        }))
    elif args.timeline_cmd == "delete":
        tenant_id = resolve_tenant_id(args.tenant_id)
        print_json(api("DELETE", f"/v1/timeline/{tenant_id}/{args.timeline_id}", timeout=args.timeout))
    elif args.timeline_cmd == "import":
        if not branch_name:
            raise SystemExit("timeline import requires BRANCH_NAME or --branch-name")
        tenant_id = resolve_tenant_id(args.tenant_id)
        if bool(args.wal_tarfile) != bool(args.end_lsn):
            raise SystemExit("timeline import requires both --wal-tarfile and --end-lsn, or neither")
        if not os.path.exists(args.base_tarfile):
            raise SystemExit(f"base tarfile does not exist: {args.base_tarfile}")
        if args.wal_tarfile and not os.path.exists(args.wal_tarfile):
            raise SystemExit(f"WAL tarfile does not exist: {args.wal_tarfile}")
        end_lsn = args.end_lsn or args.base_lsn
        pageserver_id = args.pageserver_id
        if pageserver_id is None:
            located = api("GET", f"/v1/tenant/{tenant_id}/locate")
            shard = (located.get("shards") or [{}])[0]
            pageserver_id = shard.get("node_id") or shard.get("node_attached") or 1001
        base_url = pageserver_host_http_url(pageserver_id).rstrip("/")
        query = urllib.parse.urlencode({
            "base_lsn": args.base_lsn,
            "end_lsn": end_lsn,
            "pg_version": args.pg_version,
        })
        print(f"Importing basebackup into pageserver node {pageserver_id}...", file=sys.stderr)
        http_upload_file(
            "PUT",
            f"{base_url}/v1/tenant/{tenant_id}/timeline/{args.timeline_id}/import_basebackup?{query}",
            args.base_tarfile,
            timeout=args.timeout,
        )
        if args.wal_tarfile:
            query = urllib.parse.urlencode({
                "start_lsn": args.base_lsn,
                "end_lsn": args.end_lsn,
            })
            print(f"Importing WAL into pageserver node {pageserver_id}...", file=sys.stderr)
            http_upload_file(
                "PUT",
                f"{base_url}/v1/tenant/{tenant_id}/timeline/{args.timeline_id}/import_wal?{query}",
                args.wal_tarfile,
                timeout=args.timeout,
            )
        print_json(api("POST", "/v1/timeline/import", {
            "branch_name": branch_name,
            "tenant_id": tenant_id,
            "timeline_id": args.timeline_id,
            "end_lsn": end_lsn,
            "pg_version": args.pg_version,
        }))


def cmd_mappings(args):
    if args.mappings_cmd == "list":
        print_json(api("GET", "/v1/mappings"))
    elif args.mappings_cmd == "map":
        print_json(api("POST", "/v1/mappings/map", {
            "branch_name": args.branch_name,
            "tenant_id": args.tenant_id,
            "timeline_id": args.timeline_id,
        }))


def cmd_endpoint(args):
    if args.endpoint_cmd == "list":
        value = api("GET", "/v1/endpoint")
        if getattr(args, "tenant_id", None):
            value["endpoints"] = [
                endpoint
                for endpoint in value.get("endpoints", [])
                if endpoint.get("tenant_id") == args.tenant_id
            ]
        print_json(value)
    elif args.endpoint_cmd == "status":
        print_json(api("GET", f"/v1/endpoint/{args.endpoint_id}"))
    elif args.endpoint_cmd == "create":
        if args.oggit_database is not None and not args.enable_oggit:
            raise SystemExit("--oggit-database requires --enable-oggit")
        extra_config = parse_extra_config(args.config)
        static_lsn = args.static_lsn or args.lsn
        host_http_port = (
            args.http_port
            if args.http_port is not None
            else args.external_http_port
            if args.external_http_port is not None
            else 3081
        )
        endpoint_id = args.endpoint_id or f"ep-{args.branch_name or 'main'}"
        endpoint = {
            "endpoint_id": endpoint_id,
            "tenant_id": args.tenant_id,
            "timeline_id": args.timeline_id,
            "branch_name": args.branch_name,
            "host_pg_port": args.pg_port,
            "host_http_port": host_http_port,
            "internal_http_port": args.internal_http_port,
            "endpoint_pageserver_id": args.pageserver_id,
            "service_name": args.service_name,
            "static_lsn": static_lsn,
            "hot_standby": args.hot_standby,
            "autoprewarm": args.autoprewarm,
            "offload_lfc_interval_seconds": args.offload_lfc_interval_seconds,
            "pg_version": args.pg_version,
            "grpc": args.grpc,
            "config_only": args.config_only,
            "enable_oggit": args.enable_oggit,
            "oggit_database": args.oggit_database or "postgres",
            "update_catalog": args.update_catalog,
            "create_test_user": False,
            "privileged_role_name": args.privileged_role_name,
            "extra_config": extra_config,
        }
        endpoint, _changed = resolve_endpoint_host_ports(endpoint)
        print_json(api("POST", "/v1/endpoint", endpoint))
    elif args.endpoint_cmd == "start":
        if args.oggit_database is not None and not args.enable_oggit:
            raise SystemExit("--oggit-database requires --enable-oggit")
        endpoint = api("GET", f"/v1/endpoint/{args.endpoint_id}")
        updates = {}
        if args.safekeepers_generation is not None:
            updates["safekeepers_generation"] = args.safekeepers_generation
        if args.pageserver_id is not None:
            updates["endpoint_pageserver_id"] = args.pageserver_id
        if args.safekeepers:
            updates["safekeeper_connstrings"] = [
                safekeeper_connstring_arg(item)
                for item in args.safekeepers.split(",")
                if item.strip()
            ]
        if args.remote_ext_base_url:
            updates["remote_ext_base_url"] = args.remote_ext_base_url
        if args.create_test_user:
            updates["create_test_user"] = True
            updates["skip_pg_catalog_updates"] = False
        if args.autoprewarm:
            updates["autoprewarm"] = True
        if args.offload_lfc_interval_seconds is not None:
            updates["offload_lfc_interval_seconds"] = args.offload_lfc_interval_seconds
        if args.enable_oggit:
            updates["enable_oggit"] = True
            updates["skip_pg_catalog_updates"] = False
            if args.oggit_database is not None:
                updates["oggit_database"] = args.oggit_database
        if updates:
            endpoint = rewrite_endpoint_with_ports({**endpoint, **updates})
        endpoint, changed = resolve_endpoint_host_ports(endpoint)
        if changed:
            endpoint = rewrite_endpoint_with_ports(endpoint)
        if not args.allow_multiple:
            ensure_no_running_primary_on_same_timeline(endpoint)
        plan = api("POST", f"/v1/endpoint/{args.endpoint_id}/start", {
            "enable_oggit": bool(endpoint.get("enable_oggit")),
        })
        prepare_endpoint_data_dir(api("GET", f"/v1/endpoint/{args.endpoint_id}"))
        service = plan["services"][0]
        disable_endpoint_restart_policy(args.endpoint_id)
        try:
            compose_endpoint(args.endpoint_id, "up", "-d", service)
            wait_endpoint_started(args.endpoint_id, service, timeout=args.timeout)
        except Exception:
            compose_endpoint(args.endpoint_id, "stop", service, check=False)
            api("POST", f"/v1/endpoint/{args.endpoint_id}/stop", {"last_lsn": None})
            raise
        print_json(api("GET", f"/v1/endpoint/{args.endpoint_id}"))
    elif args.endpoint_cmd == "stop":
        stopped = stop_endpoint(args.endpoint_id, mode=args.mode)
        if getattr(args, "destroy", False):
            endpoint = api("GET", f"/v1/endpoint/{args.endpoint_id}")
            data_dir = endpoint.get("data_dir") or os.path.join(".neon", endpoint["service_name"])
            safe_rmtree(data_dir if os.path.isabs(data_dir) else os.path.join(COMPOSE_DIR, data_dir))
        print_json(stopped)
    elif args.endpoint_cmd == "restart":
        stop_endpoint(args.endpoint_id, quiet=True)
        restart_args = argparse.Namespace(
            endpoint_cmd="start",
            endpoint_id=args.endpoint_id,
            timeout=args.timeout,
            allow_multiple=args.allow_multiple,
            safekeepers_generation=args.safekeepers_generation,
            safekeepers=args.safekeepers,
            remote_ext_base_url=args.remote_ext_base_url,
            create_test_user=args.create_test_user,
            enable_oggit=args.enable_oggit,
            oggit_database=args.oggit_database,
            autoprewarm=args.autoprewarm,
            offload_lfc_interval_seconds=args.offload_lfc_interval_seconds,
            pageserver_id=args.pageserver_id,
            dev=args.dev,
        )
        cmd_endpoint(restart_args)
    elif args.endpoint_cmd in {"reconfigure", "refresh-configuration", "update-pageservers"}:
        if args.endpoint_cmd in {"reconfigure", "update-pageservers"}:
            endpoint = api("GET", f"/v1/endpoint/{args.endpoint_id}")
            updates = {}
            if getattr(args, "pageserver_id", None) is not None:
                updates["endpoint_pageserver_id"] = args.pageserver_id
            if getattr(args, "safekeepers", None):
                updates["safekeeper_connstrings"] = [
                    safekeeper_connstring_arg(item)
                    for item in args.safekeepers.split(",")
                    if item.strip()
                ]
            if updates:
                rewrite_endpoint_with_ports({**endpoint, **updates})
        print_json(api("POST", f"/v1/endpoint/{args.endpoint_id}/{args.endpoint_cmd}", {}))
    elif args.endpoint_cmd == "generate-jwt":
        path = f"/v1/endpoint/{args.endpoint_id}/generate-jwt"
        if args.scope:
            path += "?" + urllib.parse.urlencode({"scope": args.scope})
        print_json(api("POST", path, {}))
    elif args.endpoint_cmd == "destroy":
        endpoint = api("GET", f"/v1/endpoint/{args.endpoint_id}")
        compose_endpoint(args.endpoint_id, "stop", endpoint["service_name"], check=False)
        compose_endpoint(args.endpoint_id, "rm", "-f", endpoint["service_name"], check=False)
        if args.remove_data:
            data_dir = endpoint.get("data_dir") or os.path.join(".neon", endpoint["service_name"])
            safe_rmtree(data_dir if os.path.isabs(data_dir) else os.path.join(COMPOSE_DIR, data_dir))
        print_json(api("DELETE", f"/v1/endpoint/{args.endpoint_id}"))


def stop_endpoint(endpoint_id, quiet=False, mode="fast"):
    endpoint = api("GET", f"/v1/endpoint/{endpoint_id}")
    service = endpoint["service_name"]
    last_lsn = None
    try:
        last_lsn = compose_endpoint(endpoint_id, "exec", "-T", service, "gsql",
                                    "-d", "postgres", "-U", "cloud_admin", "-p", "55433",
                                    "-h", "127.0.0.1", "-tAc",
                                    "SELECT pg_current_xlog_location()", capture=True, check=False)
    except Exception:
        last_lsn = None
    if mode == "immediate":
        compose_endpoint(endpoint_id, "kill", service, check=False)
    else:
        compose_endpoint(endpoint_id, "stop", service, check=False)
    result = api("POST", f"/v1/endpoint/{endpoint_id}/stop", {"last_lsn": last_lsn})
    if not quiet:
        print(f"last_lsn={last_lsn or 'unknown'}", file=sys.stderr)
    return result


def cmd_pageserver(args):
    if args.pageserver_cmd == "list":
        print_json(api("GET", "/v1/pageserver"))
    elif args.pageserver_cmd == "status":
        _ordinal, node_id, service_name = pageserver_ref(pageserver_arg_ref(args))
        print_json({
            "node": pageserver_node(node_id),
            "compose": parse_json_lines(compose_pageserver(
                service_name,
                "ps",
                service_name,
                "--format",
                "json",
                capture=True,
                check=False,
            )),
        })
    elif args.pageserver_cmd == "add":
        ordinal, node_id, service_name = pageserver_ref(args.ordinal)
        if ordinal == 1:
            raise SystemExit("pageserver ordinal 1 is the static default pageserver")
        node_id = args.node_id or node_id
        host_http_port = args.http_port if args.http_port is not None else 9897 + ordinal
        host_pg_port = args.pg_port if args.pg_port is not None else 6399 + ordinal
        host_http_port, host_pg_port = resolve_pageserver_host_ports(service_name, host_http_port, host_pg_port)
        ensure_dir(os.path.join(COMPOSE_DIR, ".neon"))
        chmod_dir(os.path.join(COMPOSE_DIR, ".neon"))
        try:
            storage_controller_api("DELETE", f"/debug/v1/tombstone/{node_id}", timeout=10)
        except SystemExit:
            pass
        plan = api("POST", "/v1/pageserver", {
            "service_name": service_name,
            "node_id": node_id,
            "host_http_port": host_http_port,
            "host_pg_port": host_pg_port,
            "og_version": args.og_version,
            "storage_image": args.image,
            "storage_controller_http": args.storage_controller_http,
            "broker_endpoint": args.broker_endpoint,
        })
        env = os.environ.copy()
        env["OG_VERSION"] = args.og_version
        env["NEON_IMAGE"] = args.image
        compose_pageserver(service_name, "up", "-d", service_name, env=env)
        wait_pageserver_registered(node_id, service_name, timeout=args.timeout)
        persist_num_pageservers_at_least(ordinal)
        print(f"Started {service_name}")
        print(f"node_id={node_id}")
        print(f"host_http=http://127.0.0.1:{host_http_port}")
        print(f"host_pg=127.0.0.1:{host_pg_port}")
        print(f"override={plan['override_file']}")
        print("check_nodes=curl -s http://127.0.0.1:1234/control/v1/node | jq")
    elif args.pageserver_cmd == "start":
        _ordinal, node_id, service_name = pageserver_ref(pageserver_arg_ref(args))
        compose_pageserver(service_name, "up", "-d", service_name)
        wait_pageserver_registered(node_id, service_name, timeout=args.timeout)
        print_json(pageserver_node(node_id))
    elif args.pageserver_cmd == "stop":
        _ordinal, _node_id, service_name = pageserver_ref(pageserver_arg_ref(args))
        if args.stop_mode == "immediate":
            compose_pageserver(service_name, "kill", service_name, check=False)
        else:
            compose_pageserver(service_name, "stop", "--timeout", str(args.timeout), service_name, check=False)
    elif args.pageserver_cmd == "restart":
        _ordinal, node_id, service_name = pageserver_ref(pageserver_arg_ref(args))
        if args.stop_mode == "immediate":
            compose_pageserver(service_name, "kill", service_name, check=False)
            compose_pageserver(service_name, "up", "-d", service_name)
        else:
            compose_pageserver(service_name, "restart", service_name, check=False)
        wait_pageserver_registered(node_id, service_name, timeout=args.timeout)
        print_json(pageserver_node(node_id))
    elif args.pageserver_cmd == "remove":
        ordinal, node_id, service_name = pageserver_ref(pageserver_arg_ref(args))
        if node_id == 1001 and not args.allow_remove_primary:
            raise SystemExit("refusing to remove the default pageserver node 1001; pass --allow-remove-primary to override")
        nodes = pageserver_nodes()
        registered = any(node.get("id") == node_id for node in nodes)
        moved_shards = []
        if registered:
            dest_node_id = args.dest_node_id
            if dest_node_id is None:
                candidates = [
                    node.get("id")
                    for node in nodes
                    if node.get("id") != node_id
                    and node.get("availability") == "Active"
                    and node.get("scheduling") == "Active"
                ]
                dest_node_id = candidates[0] if candidates else None
            if dest_node_id is None:
                raise SystemExit(f"no active destination node found for moving shards off {node_id}")
            print(f"Moving attached tenant shards from node {node_id} to node {dest_node_id}...")
            for shard in tenant_shards():
                if shard.get("node_attached") == node_id:
                    tenant_shard_id = shard["tenant_shard_id"]
                    print(f"Migrating {tenant_shard_id} -> {dest_node_id}")
                    moved_shards.append(tenant_shard_id)
                    print_json(migrate_tenant_shard(
                        tenant_shard_id,
                        dest_node_id,
                        origin_node_id=node_id,
                        override_scheduler=True,
                    ))
            print(f"Waiting for node {node_id} to have no attached tenant shards...")
            deadline = time.time() + args.timeout
            while time.time() < deadline:
                if not any(shard.get("node_attached") == node_id for shard in tenant_shards()):
                    break
                time.sleep(1)
            if any(shard.get("node_attached") == node_id for shard in tenant_shards()):
                raise SystemExit(f"timed out waiting for node {node_id} to drain attached shards")
            print(f"Deleting node {node_id} from storage_controller...")
            print_json(api("POST", f"/v1/pageserver/{node_id}/delete", {"force": args.force}))
            deadline = time.time() + args.timeout
            while time.time() < deadline:
                if pageserver_node(node_id) is None:
                    break
                time.sleep(1)
            if pageserver_node(node_id) is not None:
                raise SystemExit(f"timed out waiting for storage_controller to delete node {node_id}")
        else:
            print(f"Node {node_id} is not registered in storage_controller; stopping container only.", file=sys.stderr)
        compose_pageserver(service_name, "stop", service_name, check=False)
        compose_pageserver(service_name, "rm", "-f", service_name, check=False)
        if args.remove_data:
            safe_rmtree(os.path.join(COMPOSE_DIR, ".neon", service_name))
            safe_rmtree(os.path.join(COMPOSE_DIR, ".neon", f"{service_name}_config"))
            override = os.path.join(COMPOSE_DIR, pageserver_override(service_name))
            if os.path.exists(override):
                os.remove(override)
        print(f"Removed {service_name} node_id={node_id}")
        if moved_shards:
            print("Tenant shards were moved. Running endpoints are reconfigured by notify-attach when storage_controller emits it.")
    elif args.pageserver_cmd == "migrate":
        print_json(migrate_tenant_shard(
            args.tenant_shard_id,
            args.node_id,
            origin_node_id=args.origin_node_id,
            prewarm=args.prewarm,
            override_scheduler=args.override_scheduler,
            timeout=args.timeout,
        ))
    elif args.pageserver_cmd == "migrate-secondary":
        print_json(api("POST", f"/v1/pageserver/{args.tenant_shard_id}/migrate-secondary", {
            "node_id": args.node_id,
            "origin_node_id": args.origin_node_id,
            "prewarm": args.prewarm,
            "override_scheduler": args.override_scheduler,
            "timeout_seconds": args.timeout,
        }, timeout=args.timeout + 5))
    elif args.pageserver_cmd == "cancel-reconcile":
        print_json(api("POST", f"/v1/pageserver/{args.tenant_shard_id}/cancel-reconcile", {}))
    elif args.pageserver_cmd == "bulk-migrate":
        print_json(api("POST", "/v1/pageserver/bulk-migrate", {
            "nodes": parse_id_list(args.nodes, "--nodes"),
            "concurrency": args.concurrency,
            "max_shards": args.max_shards,
            "dry_run": args.dry_run,
            "override_scheduler": args.override_scheduler,
            "timeout_seconds": args.timeout,
        }, timeout=args.timeout + 5))
    elif args.pageserver_cmd == "fill":
        print_json(api("POST", f"/v1/pageserver/{args.node_id}/fill", {}))
    elif args.pageserver_cmd == "drain":
        print_json(api("POST", f"/v1/pageserver/{args.node_id}/drain", {}))
    elif args.pageserver_cmd == "cancel-drain":
        print_json(api("POST", f"/v1/pageserver/{args.node_id}/cancel-drain", {
            "timeout_seconds": args.timeout,
        }, timeout=args.timeout + 5))
    elif args.pageserver_cmd == "cancel-fill":
        print_json(api("POST", f"/v1/pageserver/{args.node_id}/cancel-fill", {
            "timeout_seconds": args.timeout,
        }, timeout=args.timeout + 5))
    elif args.pageserver_cmd == "shards":
        print_json(tenant_shards(args.tenant_id))


def cmd_safekeeper(args):
    if args.safekeeper_cmd == "list":
        registered = {}
        try:
            registered = {int(row.get("id")): row for row in storage_controller_safekeepers()}
        except Exception as exc:
            print(f"warning: could not query storage_controller safekeepers: {exc}", file=sys.stderr)
        rows = []
        for sk_id in sorted(set(configured_safekeeper_ids()) | set(registered)):
            service = safekeeper_service(sk_id)
            rows.append({
                "id": sk_id,
                "service": service,
                "storage_controller": registered.get(sk_id),
                "compose": parse_json_lines(compose_safekeeper(sk_id, "ps", service, "--format", "json", capture=True, check=False)),
            })
        print_json(rows)
    elif args.safekeeper_cmd == "status":
        service = safekeeper_service(args.id)
        print_json({
            "id": args.id,
            "service": service,
            "storage_controller": storage_controller_safekeeper(args.id),
            "compose": parse_json_lines(compose_safekeeper(args.id, "ps", service, "--format", "json", capture=True, check=False)),
        })
    elif args.safekeeper_cmd in {"start", "stop", "restart"}:
        service = safekeeper_service(args.id)
        env = None
        extra_opts = getattr(args, "safekeeper_extra_opt", None) or []
        if extra_opts:
            env = os.environ.copy()
            env["SAFEKEEPER_EXTRA_OPT"] = " ".join(extra_opts)
        if args.safekeeper_cmd == "start":
            if extra_opts:
                compose_safekeeper(args.id, "up", "-d", "--force-recreate", service, env=env)
            else:
                compose_safekeeper(args.id, "up", "-d", service)
            maybe_register_safekeeper(args.id, timeout=args.timeout)
        elif args.safekeeper_cmd == "stop":
            if args.stop_mode == "immediate":
                compose_safekeeper(args.id, "kill", service, check=False)
            else:
                compose_safekeeper(args.id, "stop", "--timeout", str(args.timeout), service)
        else:
            if args.stop_mode == "immediate":
                compose_safekeeper(args.id, "kill", service, check=False)
                if extra_opts:
                    compose_safekeeper(args.id, "up", "-d", "--force-recreate", service, env=env)
                else:
                    compose_safekeeper(args.id, "up", "-d", service)
            elif extra_opts:
                compose_safekeeper(args.id, "up", "-d", "--force-recreate", service, env=env)
            else:
                compose_safekeeper(args.id, "restart", service)
            maybe_register_safekeeper(args.id, timeout=args.timeout)
    elif args.safekeeper_cmd == "add":
        if args.id == 1:
            service = safekeeper_service(args.id)
            persist_num_safekeepers_at_least(args.id)
            compose_safekeeper(args.id, "up", "-d", service)
            row = maybe_register_safekeeper(args.id, timeout=args.timeout)
            print(f"{service} is the built-in safekeeper; ensured it is running")
            if row:
                print(f"storage_controller_scheduling={row.get('scheduling_policy')}")
            return
        service = safekeeper_service(args.id)
        host_http_port = args.http_port if args.http_port is not None else safekeeper_http_host_port(args.id)
        host_http_port = next_available_host_port(
            host_http_port,
            owning_container=f"{local_compose_project()}-{service}-1",
        )
        override = write_safekeeper_override(
            args.id,
            host_http_port,
            image=args.image,
            og_version=args.og_version,
            broker_endpoint=args.broker_endpoint,
        )
        persist_num_safekeepers_at_least(args.id)
        compose_safekeeper(args.id, "up", "-d", service)
        row = maybe_register_safekeeper(args.id, timeout=args.timeout)
        print(f"Started {service}")
        print(f"id={args.id}")
        print(f"host_http=http://127.0.0.1:{host_http_port}")
        print(f"compute_connstring={service}:5454")
        if row:
            print(f"storage_controller_scheduling={row.get('scheduling_policy')}")
        print(f"override={override}")
        print("new timelines choose safekeeper membership through storage-controller")
        print(f"migrate_existing=bin/docker_local safekeeper migrate --tenant-id <tenant_id> --timeline-id <timeline_id> --new-sk-set {','.join(str(i) for i in configured_safekeeper_ids())}")
    elif args.safekeeper_cmd == "remove":
        service = safekeeper_service(args.id)
        refs = []
        try:
            for endpoint in api("GET", "/v1/endpoint").get("endpoints", []):
                for connstr in endpoint.get("safekeeper_connstrings") or []:
                    if connstr.startswith(f"{service}:"):
                        refs.append(endpoint.get("endpoint_id"))
                        break
        except Exception as exc:
            print(f"warning: could not inspect endpoint safekeeper refs: {exc}", file=sys.stderr)
        if refs and not args.force:
            raise SystemExit(
                f"refusing to remove {service}: endpoints still reference it: {', '.join(refs)}. "
                "Restart/reconfigure them with --safekeepers first, or pass --force."
            )
        try:
            set_safekeeper_scheduling_policy(args.id, "Decomissioned")
        except Exception as exc:
            print(f"warning: could not mark {service} decomissioned in storage_controller: {exc}", file=sys.stderr)
        compose_safekeeper(args.id, "stop", service, check=False)
        compose_safekeeper(args.id, "rm", "-f", service, check=False)
        if args.remove_data:
            safe_rmtree(os.path.join(COMPOSE_DIR, ".neon", service))
            override = os.path.join(COMPOSE_DIR, safekeeper_override(args.id))
            if os.path.exists(override):
                os.remove(override)
        print(f"Removed {service}")
    elif args.safekeeper_cmd == "scheduling":
        print_json(api("PUT", f"/v1/safekeeper/{args.id}/scheduling", {
            "scheduling_policy": args.scheduling_policy,
        }))
    elif args.safekeeper_cmd == "migrate":
        tenant_id = resolve_tenant_id(args.tenant_id)
        new_sk_set = parse_id_list(args.new_sk_set, "--new-sk-set")
        print_json(api(
            "POST",
            "/v1/safekeeper/timeline-migrate",
            {
                "tenant_id": tenant_id,
                "timeline_id": args.timeline_id,
                "new_sk_set": new_sk_set,
            },
            timeout=args.timeout,
        ))


def cmd_branch(args):
    if args.branch_cmd in {"diff", "merge"}:
        body = {
            "source_endpoint": args.source_endpoint,
            "target_endpoint": args.target_endpoint,
            "source_schema": args.source_schema,
            "target_schema": args.target_schema,
            "database": args.database,
            "incremental_oggit": getattr(args, "incremental_oggit", False),
            "keep_fdw": getattr(args, "keep_fdw", False),
        }
        if args.branch_cmd == "merge":
            body["strategy"] = args.strategy
        print_json(api("POST", f"/v1/branch/{args.branch_cmd}", body, timeout=900))
    else:
        body = {
            "tenant_id": getattr(args, "tenant_id", None),
            "target_branch": getattr(args, "target_branch", None),
            "target_endpoint": getattr(args, "target_endpoint", None),
            "target_connstr": getattr(args, "target_connstr", None),
            "database": args.database,
            "user": getattr(args, "user", "branch_merge"),
        }
        if args.branch_cmd == "resolve":
            body["conflict_id"] = args.conflict_id
            if args.custom_sql:
                body["custom_sql"] = args.custom_sql
            else:
                body["resolution"] = args.resolution
        if args.branch_cmd in {"merge-status", "conflicts"}:
            query = {
                key: value
                for key, value in body.items()
                if value is not None
            }
            path = (
                f"/v1/branch/merge/{args.merge_id}/{args.branch_cmd.replace('merge-', '')}?"
                + urllib.parse.urlencode(query)
            )
            print_json(api("GET", path, timeout=900))
        else:
            print_json(api("POST", f"/v1/branch/merge/{args.merge_id}/{args.branch_cmd}", body, timeout=900))


def cmd_oggit(args):
    if args.oggit_cmd == "gc":
        print_json(api("POST", "/v1/oggit/gc", {
            "retention_lsn_distance": args.retention_lsn_distance,
        }, timeout=args.timeout))


def add_service_parser(sub, command_name, service_name, aliases=None):
    svc = sub.add_parser(command_name, aliases=aliases or [])
    svc_sub = svc.add_subparsers(dest="service_cmd", required=True)
    for action in ["start", "stop", "restart", "status"]:
        p = svc_sub.add_parser(action)
        p.add_argument("-t", "--timeout", type=int, default=120)
        if action in {"stop", "restart"}:
            p.add_argument("-m", "--mode", choices=["fast", "immediate"], default="fast")
        if command_name == "storage-controller":
            p.add_argument("--instance-id", type=int, default=1)
        if command_name == "storage-controller" and action == "start":
            p.add_argument("--base-port", type=int)
            p.add_argument("--handle-ps-local-disk-loss", type=parse_bool)
        p.set_defaults(func=cmd_service, service_name=service_name)
    p = svc_sub.add_parser("logs")
    p.add_argument("--tail", default="120")
    if command_name == "storage-controller":
        p.add_argument("--instance-id", type=int, default=1)
    p.set_defaults(func=cmd_service, service_name=service_name)


def build_parser():
    parser = argparse.ArgumentParser(prog="docker_local")
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("init")
    p.add_argument("--compose-project", default=DEFAULT_COMPOSE_PROJECT)
    p.add_argument("--og-version", default=DEFAULT_OG_VERSION)
    p.add_argument("--storage-image", default=DEFAULT_STORAGE_IMAGE)
    p.add_argument("--compute-image", default=DEFAULT_COMPUTE_IMAGE)
    p.add_argument("--num-pageservers", type=int)
    p.add_argument("--num-safekeepers", type=int)
    p.add_argument("--config")
    p.add_argument(
        "--force",
        nargs="?",
        const="remove-all-contents",
        default="must-not-exist",
        choices=["must-not-exist", "empty-dir-ok", "remove-all-contents"],
    )
    p.add_argument("-t", "--timeout", type=int, default=120)
    p.set_defaults(func=cmd_init)

    p = sub.add_parser("start")
    p.add_argument("--service", action="append", default=[])
    p.add_argument("-t", "--timeout", type=int, default=120)
    p.set_defaults(func=cmd_start)
    p = sub.add_parser("stop")
    p.add_argument("--include-control-plane", action="store_true")
    p.add_argument("--service", action="append", default=[])
    p.add_argument("-m", "--mode", choices=["fast", "immediate"], default="fast")
    p.add_argument("--timeout", type=int, default=120)
    p.set_defaults(func=cmd_stop)
    p = sub.add_parser("status")
    p.set_defaults(func=cmd_status)
    p = sub.add_parser("ps")
    p.add_argument("service", nargs="*")
    p.add_argument("--all", "-a", action="store_true")
    p.add_argument("--format", choices=["table", "json"], default=None)
    p.set_defaults(func=cmd_ps)
    p = sub.add_parser("logs")
    p.add_argument("service", nargs="*")
    p.add_argument("--tail", default="120")
    p.add_argument("--follow", "-f", action="store_true")
    p.set_defaults(func=cmd_logs)

    tenant = sub.add_parser("tenant")
    tenant_sub = tenant.add_subparsers(dest="tenant_cmd", required=True)
    p = tenant_sub.add_parser("list")
    p.set_defaults(func=cmd_tenant)
    p = tenant_sub.add_parser("describe")
    p.add_argument("tenant_id_pos", nargs="?")
    p.add_argument("--tenant-id")
    p.set_defaults(func=cmd_tenant)
    p = tenant_sub.add_parser("create")
    p.add_argument("--tenant-id")
    p.add_argument("--timeline-id")
    p.add_argument("--branch-name", default="main")
    p.add_argument("--set-default", action="store_true")
    p.add_argument("--pg-version", type=parse_pg_version, default=14)
    p.add_argument("--shard-count", type=int)
    p.add_argument("--shard-stripe-size", type=int)
    p.add_argument("--placement-policy", type=parse_placement_policy)
    p.add_argument("-c", "--config", action="append", default=[])
    p.set_defaults(func=cmd_tenant)
    p = tenant_sub.add_parser("locate")
    p.add_argument("tenant_id")
    p.set_defaults(func=cmd_tenant)
    p = tenant_sub.add_parser("delete")
    p.add_argument("tenant_id_pos", nargs="?")
    p.add_argument("--tenant-id")
    p.set_defaults(func=cmd_tenant)
    p = tenant_sub.add_parser("policy")
    p.add_argument("tenant_id_pos", nargs="?")
    p.add_argument("--tenant-id")
    p.add_argument("--placement", type=parse_storcon_placement)
    p.add_argument("--scheduling", type=parse_shard_scheduling)
    p.set_defaults(func=cmd_tenant)
    p = tenant_sub.add_parser("shard-split")
    p.add_argument("tenant_id_pos", nargs="?")
    p.add_argument("--tenant-id")
    p.add_argument("--shard-count", type=int, required=True)
    p.add_argument("--stripe-size", type=int)
    p.set_defaults(func=cmd_tenant)
    p = tenant_sub.add_parser("set-preferred-az")
    p.add_argument("tenant_id_pos", nargs="?")
    p.add_argument("--tenant-id")
    p.add_argument("--preferred-az")
    p.set_defaults(func=cmd_tenant)
    p = tenant_sub.add_parser("set-default")
    p.add_argument("tenant_id_pos", nargs="?")
    p.add_argument("--tenant-id")
    p.set_defaults(func=cmd_tenant)
    p = tenant_sub.add_parser("config")
    p.add_argument("tenant_id_pos", nargs="?")
    p.add_argument("--tenant-id")
    p.add_argument("-c", "--config", "--set", action="append", default=[])
    p.set_defaults(func=cmd_tenant)
    p = tenant_sub.add_parser("import")
    p.add_argument("tenant_id_pos", nargs="?")
    p.add_argument("--tenant-id")
    p.set_defaults(func=cmd_tenant)

    timeline = sub.add_parser("timeline")
    timeline_sub = timeline.add_subparsers(dest="timeline_cmd", required=True)
    p = timeline_sub.add_parser("list")
    p.add_argument("--tenant-id")
    p.set_defaults(func=cmd_timeline)
    p = timeline_sub.add_parser("branch")
    p.add_argument("branch_name_pos", nargs="?")
    p.add_argument("--branch-name")
    p.add_argument("--tenant-id")
    p.add_argument("--ancestor-branch-name")
    p.add_argument("--ancestor-timeline-id")
    p.add_argument("--ancestor-start-lsn")
    p.add_argument("--timeline-id")
    p.set_defaults(func=cmd_timeline)
    p = timeline_sub.add_parser("create")
    p.add_argument("branch_name_pos", nargs="?")
    p.add_argument("--branch-name")
    p.add_argument("--tenant-id")
    p.add_argument("--timeline-id")
    p.add_argument("--pg-version", type=parse_pg_version, default=14)
    p.set_defaults(func=cmd_timeline)
    p = timeline_sub.add_parser("delete")
    p.add_argument("timeline_id")
    p.add_argument("--tenant-id")
    p.add_argument("-t", "--timeout", type=int, default=120)
    p.set_defaults(func=cmd_timeline)
    p = timeline_sub.add_parser("import")
    p.add_argument("branch_name_pos", nargs="?")
    p.add_argument("--branch-name")
    p.add_argument("--tenant-id")
    p.add_argument("--timeline-id", required=True)
    p.add_argument("--base-tarfile", required=True)
    p.add_argument("--base-lsn", required=True)
    p.add_argument("--wal-tarfile")
    p.add_argument("--end-lsn")
    p.add_argument("--pg-version", type=parse_pg_version, default=14)
    p.add_argument("--pageserver-id", type=int)
    p.add_argument("--timeout", type=int, default=900)
    p.set_defaults(func=cmd_timeline)

    mappings = sub.add_parser("mappings")
    mappings_sub = mappings.add_subparsers(dest="mappings_cmd", required=True)
    p = mappings_sub.add_parser("list")
    p.set_defaults(func=cmd_mappings)
    p = mappings_sub.add_parser("map")
    p.add_argument("branch_name")
    p.add_argument("--tenant-id", required=True)
    p.add_argument("--timeline-id", required=True)
    p.set_defaults(func=cmd_mappings)

    endpoint = sub.add_parser("endpoint")
    endpoint_sub = endpoint.add_subparsers(dest="endpoint_cmd", required=True)
    p = endpoint_sub.add_parser("list")
    p.add_argument("--tenant-id")
    p.set_defaults(func=cmd_endpoint)
    p = endpoint_sub.add_parser("status")
    p.add_argument("endpoint_id")
    p.set_defaults(func=cmd_endpoint)
    p = endpoint_sub.add_parser("create")
    p.add_argument("endpoint_id", nargs="?")
    p.add_argument("--tenant-id")
    p.add_argument("--timeline-id")
    p.add_argument("--branch-name", default="main")
    p.add_argument("--pg-port", type=int, default=55434)
    p.add_argument("--http-port", type=int)
    p.add_argument("--external-http-port", type=int)
    p.add_argument("--internal-http-port", type=int)
    p.add_argument("--service-name")
    p.add_argument("--lsn")
    p.add_argument("--static-lsn")
    p.add_argument("--pageserver-id", type=int)
    p.add_argument("--config-only", action="store_true")
    p.add_argument("--enable-oggit", action="store_true")
    p.add_argument("--oggit-database")
    p.add_argument("--pg-version", type=parse_pg_version, default=14)
    p.add_argument("--grpc", action="store_true")
    p.add_argument("--hot-standby", action="store_true")
    p.add_argument("--update-catalog", action="store_true")
    p.add_argument("--allow-multiple", action="store_true")
    p.add_argument("--privileged-role-name")
    p.add_argument("--autoprewarm", action="store_true")
    p.add_argument("--offload-lfc-interval-seconds", type=int)
    p.add_argument("--config", action="append", default=[])
    p.set_defaults(func=cmd_endpoint)
    for name in ["start", "restart"]:
        p = endpoint_sub.add_parser(name)
        p.add_argument("endpoint_id")
        p.add_argument("--pageserver-id", type=int)
        p.add_argument("--safekeepers-generation", type=int)
        p.add_argument("--safekeepers")
        p.add_argument("--remote-ext-base-url", "--remote-ext-config")
        p.add_argument("--create-test-user", action="store_true")
        p.add_argument("--enable-oggit", action="store_true")
        p.add_argument("--oggit-database")
        p.add_argument("--allow-multiple", action="store_true")
        p.add_argument("--autoprewarm", action="store_true")
        p.add_argument("--offload-lfc-interval-seconds", type=int)
        p.add_argument("--dev", action="store_true")
        p.add_argument("-t", "--timeout", type=int, default=180)
        p.set_defaults(func=cmd_endpoint)
    for name in ["stop", "refresh-configuration"]:
        p = endpoint_sub.add_parser(name)
        p.add_argument("endpoint_id")
        if name == "stop":
            p.add_argument("--destroy", action="store_true")
            p.add_argument("--mode", choices=["fast", "immediate"], default="fast")
        p.set_defaults(func=cmd_endpoint)
    p = endpoint_sub.add_parser("reconfigure")
    p.add_argument("endpoint_id")
    p.add_argument("--tenant-id")
    p.add_argument("--pageserver-id", type=int)
    p.add_argument("--safekeepers")
    p.set_defaults(func=cmd_endpoint)
    p = endpoint_sub.add_parser("update-pageservers")
    p.add_argument("endpoint_id")
    p.add_argument("-p", "--pageserver-id", type=int)
    p.set_defaults(func=cmd_endpoint)
    p = endpoint_sub.add_parser("generate-jwt")
    p.add_argument("endpoint_id")
    p.add_argument("-s", "--scope")
    p.set_defaults(func=cmd_endpoint)
    p = endpoint_sub.add_parser("destroy")
    p.add_argument("endpoint_id")
    p.add_argument("--remove-data", action="store_true")
    p.set_defaults(func=cmd_endpoint)

    pageserver = sub.add_parser("pageserver")
    pageserver_sub = pageserver.add_subparsers(dest="pageserver_cmd", required=True)
    p = pageserver_sub.add_parser("list")
    p.set_defaults(func=cmd_pageserver)
    p = pageserver_sub.add_parser("status")
    p.add_argument("ref", nargs="?")
    p.add_argument("--id", dest="pageserver_id", type=int)
    p.set_defaults(func=cmd_pageserver)
    p = pageserver_sub.add_parser("add")
    p.add_argument("ordinal")
    p.add_argument("--node-id", type=int)
    p.add_argument("--http-port", type=int)
    p.add_argument("--pg-port", type=int)
    p.add_argument("--image", default=DEFAULT_STORAGE_IMAGE)
    p.add_argument("--og-version", default=DEFAULT_OG_VERSION)
    p.add_argument("--storage-controller-http", default=os.environ.get("STORAGE_CONTROLLER_HTTP", "http://storage_controller:1234"))
    p.add_argument("--broker-endpoint", default=os.environ.get("BROKER_ENDPOINT", "http://storage_broker:50051"))
    p.add_argument("--timeout", type=int, default=120)
    p.set_defaults(func=cmd_pageserver)
    for name in ["start", "restart"]:
        p = pageserver_sub.add_parser(name)
        p.add_argument("ref", nargs="?")
        p.add_argument("--id", dest="pageserver_id", type=int)
        p.add_argument("-t", "--timeout", type=int, default=120)
        if name == "restart":
            p.add_argument("-m", "--stop-mode", "--mode", choices=["fast", "immediate"], default="fast")
        p.set_defaults(func=cmd_pageserver)
    p = pageserver_sub.add_parser("stop")
    p.add_argument("ref", nargs="?")
    p.add_argument("--id", dest="pageserver_id", type=int)
    p.add_argument("-m", "--stop-mode", "--mode", choices=["fast", "immediate"], default="fast")
    p.set_defaults(func=cmd_pageserver)
    p = pageserver_sub.add_parser("remove")
    p.add_argument("ref")
    p.add_argument("--dest-node-id", type=int)
    p.add_argument("--force", action="store_true")
    p.add_argument("--remove-data", action="store_true")
    p.add_argument("--allow-remove-primary", action="store_true")
    p.add_argument("-t", "--timeout", type=int, default=120)
    p.set_defaults(func=cmd_pageserver)
    for name in ["fill", "drain"]:
        p = pageserver_sub.add_parser(name)
        p.add_argument("node_id", type=int)
        p.set_defaults(func=cmd_pageserver)
    p = pageserver_sub.add_parser("migrate")
    p.add_argument("tenant_shard_id")
    p.add_argument("node_id", type=int)
    p.add_argument("--origin-node-id", type=int)
    p.add_argument("--prewarm", action="store_true", default=None)
    p.add_argument("--override-scheduler", action="store_true")
    p.add_argument("-t", "--timeout", type=int, default=900)
    p.set_defaults(func=cmd_pageserver)
    p = pageserver_sub.add_parser("migrate-secondary")
    p.add_argument("tenant_shard_id")
    p.add_argument("node_id", type=int)
    p.add_argument("--origin-node-id", type=int)
    p.add_argument("--prewarm", action="store_true", default=None)
    p.add_argument("--override-scheduler", action="store_true")
    p.add_argument("-t", "--timeout", type=int, default=900)
    p.set_defaults(func=cmd_pageserver)
    p = pageserver_sub.add_parser("cancel-reconcile")
    p.add_argument("tenant_shard_id")
    p.set_defaults(func=cmd_pageserver)
    p = pageserver_sub.add_parser("bulk-migrate")
    p.add_argument("--nodes", required=True)
    p.add_argument("--concurrency", type=int)
    p.add_argument("--max-shards", type=int)
    p.add_argument("--dry-run", action="store_true")
    p.add_argument("--override-scheduler", action="store_true")
    p.add_argument("-t", "--timeout", type=int, default=900)
    p.set_defaults(func=cmd_pageserver)
    for name in ["cancel-drain", "cancel-fill"]:
        p = pageserver_sub.add_parser(name)
        p.add_argument("node_id", type=int)
        p.add_argument("-t", "--timeout", type=int, default=120)
        p.set_defaults(func=cmd_pageserver)
    p = pageserver_sub.add_parser("shards")
    p.add_argument("--tenant-id")
    p.set_defaults(func=cmd_pageserver)

    safekeeper = sub.add_parser("safekeeper")
    safekeeper_sub = safekeeper.add_subparsers(dest="safekeeper_cmd", required=True)
    p = safekeeper_sub.add_parser("list")
    p.set_defaults(func=cmd_safekeeper)
    p = safekeeper_sub.add_parser("status")
    p.add_argument("id", nargs="?", type=int, default=1)
    p.set_defaults(func=cmd_safekeeper)
    for name in ["start", "stop", "restart"]:
        p = safekeeper_sub.add_parser(name)
        p.add_argument("id", nargs="?", type=int, default=1)
        p.add_argument("-t", "--timeout", type=int, default=120)
        if name in {"start", "restart"}:
            p.add_argument("-e", "--safekeeper-extra-opt", action="append", default=[])
        if name in {"stop", "restart"}:
            p.add_argument("-m", "--stop-mode", "--mode", choices=["fast", "immediate"], default="fast")
        p.set_defaults(func=cmd_safekeeper)
    p = safekeeper_sub.add_parser("add")
    p.add_argument("id", type=int)
    p.add_argument("--http-port", type=int)
    p.add_argument("--image", default=DEFAULT_STORAGE_IMAGE)
    p.add_argument("--og-version", default=DEFAULT_OG_VERSION)
    p.add_argument("--broker-endpoint", default=os.environ.get("BROKER_ENDPOINT", "http://storage_broker:50051"))
    p.add_argument("--timeout", type=int, default=120)
    p.set_defaults(func=cmd_safekeeper)
    p = safekeeper_sub.add_parser("remove")
    p.add_argument("id", type=int)
    p.add_argument("--force", action="store_true")
    p.add_argument("--remove-data", action="store_true")
    p.set_defaults(func=cmd_safekeeper)
    p = safekeeper_sub.add_parser("scheduling")
    p.add_argument("id", type=int)
    p.add_argument("--scheduling-policy", required=True, type=parse_safekeeper_scheduling)
    p.set_defaults(func=cmd_safekeeper)
    p = safekeeper_sub.add_parser("migrate")
    p.add_argument("--tenant-id")
    p.add_argument("--timeline-id", required=True)
    p.add_argument("--new-sk-set", required=True)
    p.add_argument("-t", "--timeout", type=int, default=900)
    p.set_defaults(func=cmd_safekeeper)

    add_service_parser(sub, "endpoint-storage", "endpoint_storage", aliases=["endpoint_storage"])
    add_service_parser(sub, "storage-controller", "storage_controller", aliases=["storage_controller"])
    add_service_parser(sub, "storage-broker", "storage_broker", aliases=["storage_broker"])

    branch = sub.add_parser("branch")
    branch_sub = branch.add_subparsers(dest="branch_cmd", required=True)
    for name in ["diff", "merge"]:
        p = branch_sub.add_parser(name)
        p.add_argument("--source-endpoint", required=True)
        p.add_argument("--target-endpoint", required=True)
        p.add_argument("--source-schema", default="public")
        p.add_argument("--target-schema", default="public")
        p.add_argument("--database", required=True)
        p.add_argument("--incremental-oggit", action="store_true")
        p.add_argument("--keep-fdw", action="store_true")
        if name == "merge":
            p.add_argument("--strategy", default="fail", choices=["fail", "ours", "theirs", "manual"])
        p.set_defaults(func=cmd_branch)
    for name in ["merge-status", "conflicts", "continue", "abort"]:
        p = branch_sub.add_parser(name)
        p.add_argument("--tenant-id")
        p.add_argument("--target-branch")
        p.add_argument("--target-endpoint")
        p.add_argument("--target-connstr")
        p.add_argument("--merge-id", required=True)
        p.add_argument("--database", required=True)
        p.add_argument("--user", default="branch_merge")
        p.set_defaults(func=cmd_branch)
    p = branch_sub.add_parser("resolve")
    p.add_argument("--tenant-id")
    p.add_argument("--target-branch")
    p.add_argument("--target-endpoint")
    p.add_argument("--target-connstr")
    p.add_argument("--merge-id", required=True)
    p.add_argument("--conflict-id", type=int, required=True)
    p.add_argument("--resolution", choices=["ours", "theirs", "skip"], default="theirs")
    p.add_argument("--custom-sql")
    p.add_argument("--database", required=True)
    p.add_argument("--user", default="branch_merge")
    p.set_defaults(func=cmd_branch)

    oggit = sub.add_parser("oggit")
    oggit_sub = oggit.add_subparsers(dest="oggit_cmd", required=True)
    p = oggit_sub.add_parser("gc")
    p.add_argument("--retention-lsn-distance", type=int, default=16 * 1024 * 1024)
    p.add_argument("-t", "--timeout", type=int, default=900)
    p.set_defaults(func=cmd_oggit)

    return parser


def main():
    args = build_parser().parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
