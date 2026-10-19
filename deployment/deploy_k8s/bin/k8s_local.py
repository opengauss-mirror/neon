#!/usr/bin/env python3
from __future__ import annotations

import argparse
import base64
import copy
import json
import os
import re
import secrets
import subprocess
import sys
import time
import urllib.parse
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Sequence


ROOT_DIR = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT_DIR.parent / "common"))
sys.path.insert(0, str(Path(__file__).resolve().parent))
from pitr_common import pitr_timestamp, pitr_select_lsn, pitr_shard_zero, pitr_check_lease
from coordinator_client import request as coordinator_http_request

GENERATED_DIR = ROOT_DIR / "generated"

ARGS_ENV_FILE = ROOT_DIR / "quick-start-branching.env"


def load_args_env() -> dict[str, str]:
    """Read the plain KEY=VALUE cache written by quick-start-branching.sh."""
    values: dict[str, str] = {}
    if not ARGS_ENV_FILE.is_file():
        return values

    for raw_line in ARGS_ENV_FILE.read_text(encoding="utf-8").splitlines():
        line = raw_line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        key = key.strip()
        if key:
            values[key] = value
    return values


ARGS_ENV = load_args_env()


def config_value(file_key: str, env_key: str, fallback: str) -> str:
    """Prefer quick-start-branching.env, then process environment, then fallback."""
    return ARGS_ENV.get(file_key, os.environ.get(env_key, fallback))


DEFAULT_NAMESPACE = config_value("NAMESPACE", "K8S_LOCAL_NAMESPACE", "bw")
DEFAULT_DATA_ROOT = config_value("DATA_ROOT", "K8S_LOCAL_DATA_ROOT", str(ROOT_DIR / "data"))
DEFAULT_DEPLOY_ROOT = config_value("DEPLOY_ROOT", "K8S_LOCAL_DEPLOY_ROOT", str(ROOT_DIR / "deploy"))
DEFAULT_STORAGE_IMAGE = config_value(
    "STORAGE_IMAGE",
    "K8S_LOCAL_STORAGE_IMAGE",
    "og_storage:k8s",
)
DEFAULT_COMPUTE_IMAGE = config_value(
    "COMPUTE_IMAGE",
    "K8S_LOCAL_COMPUTE_IMAGE",
    "og_compute:k8s",
)
DEFAULT_OG_VERSION = config_value("OG_VERSION", "K8S_LOCAL_OG_VERSION", "V702")
DEFAULT_NODE_NAME = config_value("NODE_NAME", "K8S_LOCAL_NODE_NAME", "opengauss201")
DEFAULT_SECRET = config_value("SECRET", "K8S_LOCAL_SECRET", "branching-secret")
DEFAULT_SAFEKEEPER_HTTP_URLS = "http://safekeeper:7676"
DEFAULT_SAFEKEEPER_CONNSTRINGS = "safekeeper:5454"
DEFAULT_OPERATOR_URL = config_value(
    "OPERATOR_URL",
    "K8S_LOCAL_OPERATOR_URL",
    "http://serverlessdb-operator.serverlessdb.svc.cluster.local:8080",
)

DNS_LABEL_RE = re.compile(r"^[a-z]([a-z0-9-]*[a-z0-9])?$")


class CommandError(RuntimeError):
    def __init__(self, cmd: Sequence[str], result: subprocess.CompletedProcess[str]):
        detail = (result.stderr or result.stdout).strip()
        super().__init__(f"command failed ({result.returncode}): {' '.join(cmd)}\n{detail}")
        self.result = result


def run(
    cmd: Sequence[str],
    *,
    input_text: str | None = None,
    timeout: int = 120,
    check: bool = True,
    capture: bool = True,
) -> subprocess.CompletedProcess[str]:
    result = subprocess.run(
        list(cmd),
        input=input_text,
        text=True,
        capture_output=capture,
        timeout=timeout,
        check=False,
        env=os.environ.copy(),
    )
    if check and result.returncode != 0:
        raise CommandError(cmd, result)
    return result


class KubernetesAPIError(RuntimeError):
    def __init__(self, status, method, path, detail):
        self.status = status
        super().__init__("Kubernetes API %s %s failed HTTP %s: %s" %
                         (method, path, status, detail))


class KubernetesAPI:
    """In-cluster Kubernetes API transport for coordinator requests."""
    ROUTES = {
        "configmap": ("/api/v1", "configmaps"),
        "pod": ("/api/v1", "pods"),
        "dbinstance": ("/apis/serverlessdb.openfuyao.cn/v1", "dbinstances"),
        "deployment": ("/apis/apps/v1", "deployments"),
        "dbresourcepool": ("/apis/serverlessdb.openfuyao.cn/v1", "dbresourcepools"),
        "svc": ("/api/v1", "services"),
    }

    def __init__(self):
        import http.client
        import queue
        import ssl
        self.http = http.client
        self.connections = queue.LifoQueue()
        host = os.environ["KUBERNETES_SERVICE_HOST"]
        port = os.environ.get("KUBERNETES_SERVICE_PORT_HTTPS", "443")
        self.url = urllib.parse.urlsplit("https://%s:%s" % (host, port))
        self.context = ssl.create_default_context(cafile="/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")
        self.token_path = Path("/var/run/secrets/kubernetes.io/serviceaccount/token")
        self.headers = {"Accept": "application/json"}

    def request_headers(self):
        headers = dict(self.headers)
        if self.token_path is not None:
            headers["Authorization"] = "Bearer " + self.token_path.read_text().strip()
        return headers

    def request(self, method, path, body=None, content_type="application/json", missing_ok=False):
        import queue
        data = None if body is None else json.dumps(body).encode()
        # A peer can close an idle keep-alive socket while it is in the pool.
        # Retry only read-only GETs, once, using a NEW connection. A failed write
        # may already have taken effect and must never be replayed here.
        attempts = 2 if method == "GET" and body is None else 1
        for attempt in range(attempts):
            try:
                connection = self.connections.get_nowait() if attempt == 0 else None
            except queue.Empty:
                connection = None
            if connection is None:
                connection = self.http.HTTPSConnection(
                    self.url.hostname, self.url.port or 443, context=self.context, timeout=120)
            headers = self.request_headers()
            if data is not None:
                headers["Content-Type"] = content_type
            try:
                connection.request(method, self.url.path.rstrip("/") + path, body=data, headers=headers)
                response = connection.getresponse()
                raw = response.read()
                status = response.status
                if response.will_close:
                    connection.close()
                else:
                    self.connections.put(connection)
                break
            except (self.http.RemoteDisconnected, ConnectionResetError, BrokenPipeError):
                connection.close()
                if attempt + 1 == attempts:
                    raise
            except BaseException:
                connection.close()
                raise
        if status == 404 and missing_ok:
            return None
        if not 200 <= status < 300:
            raise KubernetesAPIError(status, method, path, raw.decode(errors="replace"))
        return json.loads(raw) if raw else None

    def path(self, kind, namespace, name):
        prefix, plural = self.ROUTES[kind]
        quote = lambda value: urllib.parse.quote(value, safe="")
        return "%s/namespaces/%s/%s/%s" % (prefix, quote(namespace), plural, quote(name))

    def close(self):
        import queue
        while True:
            try:
                self.connections.get_nowait().close()
            except queue.Empty:
                break


def kubectl(args: Sequence[str], **kwargs: Any) -> subprocess.CompletedProcess[str]:
    return run(["kubectl", *args], **kwargs)


def kubectl_json(args: Sequence[str]) -> Any:
    result = kubectl([*args, "-o", "json"], timeout=60)
    return json.loads(result.stdout)


def kubectl_json_optional(args: Sequence[str]) -> Any | None:
    result = kubectl([*args, "-o", "json"], timeout=60, check=False)
    if result.returncode != 0:
        return None
    return json.loads(result.stdout)


def print_json(value: Any) -> None:
    print(json.dumps(value, indent=2, sort_keys=True, ensure_ascii=False))


def validate_dns_label(value: str) -> str:
    if len(value) > 63 or not DNS_LABEL_RE.fullmatch(value):
        raise argparse.ArgumentTypeError(f"invalid Kubernetes DNS label: {value}")
    return value


def ensure_generated_dir() -> None:
    GENERATED_DIR.mkdir(parents=True, exist_ok=True)


def write_generated(name: str, items: list[dict[str, Any]]) -> Path:
    ensure_generated_dir()
    path = GENERATED_DIR / name
    doc = {"apiVersion": "v1", "kind": "List", "items": items}
    path.write_text(json.dumps(doc, indent=2, sort_keys=True) + "\n", encoding="utf-8")
    return path


def apply_items(items: list[dict[str, Any]], dry_run: bool = False) -> str:
    cmd = ["apply", "--server-side", "--force-conflicts", "-f", "-"]
    if dry_run:
        cmd.extend(["--dry-run=server", "-o", "name"])
    result = kubectl(cmd, input_text=json.dumps({"apiVersion": "v1", "kind": "List", "items": items}), timeout=120)
    return result.stdout


def delete_resource(namespace: str, kind: str, name: str, *, dry_run: bool = False) -> str:
    cmd = ["delete", kind, name, "-n", namespace, "--ignore-not-found"]
    if dry_run:
        cmd.append("--dry-run=server")
    return kubectl(cmd, timeout=120).stdout


def rollout(namespace: str, deployment: str, timeout: int) -> None:
    kubectl(["rollout", "status", f"deployment/{deployment}", "-n", namespace, f"--timeout={timeout}s"], timeout=timeout + 30)


def common_labels(name: str, component: str) -> dict[str, str]:
    return {
        "app.kubernetes.io/name": name,
        "app.kubernetes.io/part-of": "opengauss-branching",
        "app.kubernetes.io/component": component,
    }


def deployment_summary(item: dict[str, Any]) -> dict[str, Any]:
    containers = item.get("spec", {}).get("template", {}).get("spec", {}).get("containers", [])
    return {
        "name": item.get("metadata", {}).get("name"),
        "replicas": item.get("spec", {}).get("replicas", 0),
        "ready": item.get("status", {}).get("readyReplicas", 0),
        "image": containers[0].get("image") if containers else None,
    }


def container_curl_args(method: str, url: str, body: dict[str, Any] | None = None) -> tuple[list[str], str | None]:
    cmd = ["curl", "-sS", "-f", "-X", method]
    input_text = None
    if body is not None:
        cmd.extend(["-H", "Content-Type: application/json", "--data-binary", "@-"])
        input_text = json.dumps(body, separators=(",", ":"))
    cmd.append(url)
    return cmd, input_text


def exec_deploy(
    namespace: str,
    deployment: str,
    container: str,
    command: Sequence[str],
    *,
    input_text: str | None = None,
    timeout: int = 120,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    args = ["exec"]
    if input_text is not None:
        args.append("-i")
    args.extend(["-n", namespace, f"deploy/{deployment}", "-c", container, "--", *command])
    return kubectl(args, input_text=input_text, timeout=timeout, check=check)


def exec_pod(
    namespace: str,
    pod: str,
    container: str,
    command: Sequence[str],
    *,
    input_text: str | None = None,
    timeout: int = 120,
    check: bool = True,
) -> subprocess.CompletedProcess[str]:
    args = ["exec"]
    if input_text is not None:
        args.append("-i")
    args.extend(["-n", namespace, pod, "-c", container, "--", *command])
    return kubectl(args, input_text=input_text, timeout=timeout, check=check)


def storage_controller_request(namespace: str, method: str, path: str, body: dict[str, Any] | None = None) -> Any:
    cmd, input_text = container_curl_args(method, f"http://127.0.0.1:1234{path}", body)
    result = exec_deploy(namespace, "storage-controller", "storage-controller", cmd, input_text=input_text, timeout=900)
    return json.loads(result.stdout) if result.stdout.strip() else None


def docker_control_plane_request(namespace: str, method: str, path: str, body: dict[str, Any] | None = None, check: bool = True) -> Any:
    cmd, input_text = container_curl_args(method, f"http://127.0.0.1:8080{path}", body)
    result = exec_deploy(
        namespace,
        "docker-control-plane",
        "docker-control-plane",
        cmd,
        input_text=input_text,
        timeout=900,
        check=check,
    )
    return json.loads(result.stdout) if result.stdout.strip() else None


def gsql(namespace: str, deployment: str, database: str, sql: str, *, og_version: str, check: bool = True) -> str:
    result = exec_deploy(
        namespace,
        deployment,
        "compute",
        [
            f"/usr/local/{og_version}/bin/gsql",
            "-X",
            "-A",
            "-t",
            "-v",
            "ON_ERROR_STOP=1",
            "-d",
            database,
            "-U",
            "cloud_admin",
            "-p",
            "55433",
            "-h",
            "127.0.0.1",
            "-c",
            sql,
        ],
        timeout=120,
        check=check,
    )
    return result.stdout.strip()


def show_setting(namespace: str, deployment: str, setting: str, *, og_version: str) -> str:
    output = gsql(namespace, deployment, "postgres", f"SHOW {setting};", og_version=og_version)
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    return lines[-1] if lines else ""


def gsql_pod(namespace: str, pod: str, database: str, sql: str, *, og_version: str, check: bool = True) -> str:
    result = exec_pod(
        namespace,
        pod,
        "compute",
        [
            f"/usr/local/{og_version}/bin/gsql",
            "-X",
            "-A",
            "-t",
            "-v",
            "ON_ERROR_STOP=1",
            "-d",
            database,
            "-U",
            "cloud_admin",
            "-p",
            "55433",
            "-h",
            "127.0.0.1",
            "-c",
            sql,
        ],
        timeout=20,
        check=check,
    )
    return result.stdout.strip()


def secret_value(namespace: str, secret: str, key: str) -> str:
    obj = kubectl_json(["get", "secret", secret, "-n", namespace])
    encoded = obj.get("data", {}).get(key)
    if not encoded:
        raise RuntimeError(f"secret {namespace}/{secret} does not contain key {key}")
    return base64.b64decode(encoded).decode("utf-8")


def branch_connstr(namespace: str, service: str, database: str, user: str, password: str) -> str:
    encoded_user = urllib.parse.quote(user, safe="")
    encoded_password = urllib.parse.quote(password, safe="")
    host = f"{service}.{namespace}.svc.cluster.local"
    return f"postgresql://{encoded_user}:{encoded_password}@{host}:55433/{database}?sslmode=disable"


def upsert_env(container: dict[str, Any], values: dict[str, str]) -> None:
    env = container.setdefault("env", [])
    positions = {item.get("name"): index for index, item in enumerate(env)}
    for name, value in values.items():
        item = {"name": name, "value": value}
        if name in positions:
            env[positions[name]] = item
        else:
            env.append(item)


def service(name: str, namespace: str, selector: str, ports: list[dict[str, Any]], component: str) -> dict[str, Any]:
    return {
        "apiVersion": "v1",
        "kind": "Service",
        "metadata": {"name": name, "namespace": namespace, "labels": common_labels(name, component)},
        "spec": {"selector": {"app.kubernetes.io/name": selector}, "ports": ports},
    }


def safekeeper_items(args: argparse.Namespace) -> list[dict[str, Any]]:
    sk_id = int(args.id)
    name = f"safekeeper{sk_id}"
    labels = common_labels(name, "safekeeper")
    return [
        service(
            name,
            args.namespace,
            name,
            [
                {"name": "pg", "port": 5454, "targetPort": "pg"},
                {"name": "http", "port": 7676, "targetPort": "http"},
            ],
            "safekeeper",
        ),
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": name, "namespace": args.namespace, "labels": labels},
            "spec": {
                "replicas": 1,
                "strategy": {"type": "Recreate"},
                "selector": {"matchLabels": {"app.kubernetes.io/name": name}},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "nodeSelector": {"kubernetes.io/hostname": args.node_name},
                        "securityContext": {"fsGroup": 1000, "seLinuxOptions": {"type": "spc_t"}},
                        "initContainers": [
                            {
                                "name": "prepare-data",
                                "image": args.storage_image,
                                "imagePullPolicy": "Never",
                                "securityContext": {"runAsUser": 0, "runAsGroup": 0},
                                "command": ["/bin/sh", "-ec"],
                                "args": ["mkdir -p /data/.neon/safekeeper && chown -R 1000:1000 /data/.neon/safekeeper"],
                                "volumeMounts": [{"name": "data", "mountPath": "/data/.neon/safekeeper"}],
                            }
                        ],
                        "containers": [
                            {
                                "name": "safekeeper",
                                "image": args.storage_image,
                                "imagePullPolicy": "Never",
                                "command": ["safekeeper"],
                                "args": [
                                    "--listen-pg=0.0.0.0:5454",
                                    f"--advertise-pg={name}:5454",
                                    "--listen-http=0.0.0.0:7676",
                                    f"--id={sk_id}",
                                    "--broker-endpoint=http://storage-broker:50051",
                                    "-D",
                                    "/data/.neon/safekeeper",
                                ],
                                "ports": [
                                    {"name": "pg", "containerPort": 5454},
                                    {"name": "http", "containerPort": 7676},
                                ],
                                "readinessProbe": {
                                    "tcpSocket": {"port": "pg"},
                                    "periodSeconds": 2,
                                    "failureThreshold": 30,
                                },
                                "resources": {
                                    "requests": {"cpu": "250m", "memory": "512Mi"},
                                    "limits": {"cpu": "2", "memory": "4Gi"},
                                },
                                "volumeMounts": [{"name": "data", "mountPath": "/data/.neon/safekeeper"}],
                            }
                        ],
                        "volumes": [
                            {
                                "name": "data",
                                "hostPath": {"path": f"{args.data_root}/safekeeper{sk_id}", "type": "DirectoryOrCreate"},
                            }
                        ],
                    },
                },
            },
        },
    ]


def pageserver_items(args: argparse.Namespace) -> tuple[int, str, list[dict[str, Any]]]:
    ordinal = int(args.ordinal)
    if ordinal < 2:
        raise SystemExit("pageserver ordinal 1 is the built-in pageserver")
    node_id = args.node_id or 1000 + ordinal
    name = f"pageserver{ordinal}"
    config_name = f"{name}-config"
    labels = common_labels(name, "pageserver")
    pageserver_toml = "\n".join(
        [
            "broker_endpoint='http://storage-broker:50051'",
            f"pg_distrib_dir='/usr/local/{args.og_version}'",
            "listen_pg_addr='0.0.0.0:6400'",
            "listen_http_addr='0.0.0.0:9898'",
            "remote_storage={local_path='/data/.neon/shared_remote_storage'}",
            "control_plane_api='http://storage-controller:1234/upcall/v1/'",
            "control_plane_emergency_mode=false",
            "availability_zone='local'",
            'virtual_file_io_mode="buffered"',
            "",
        ]
    )
    metadata = {
        "host": name,
        "port": 6400,
        "http_host": name,
        "http_port": 9898,
        "https_port": None,
        "grpc_host": None,
        "grpc_port": None,
        "availability_zone_id": "local",
    }
    items = [
        {
            "apiVersion": "v1",
            "kind": "ConfigMap",
            "metadata": {"name": config_name, "namespace": args.namespace, "labels": labels},
            "data": {
                "identity.toml": f"id={node_id}\n",
                "pageserver.toml": pageserver_toml,
                "metadata.json": json.dumps(metadata, indent=2) + "\n",
            },
        },
        service(
            name,
            args.namespace,
            name,
            [
                {"name": "pg", "port": 6400, "targetPort": "pg"},
                {"name": "http", "port": 9898, "targetPort": "http"},
            ],
            "pageserver",
        ),
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": name, "namespace": args.namespace, "labels": labels},
            "spec": {
                "replicas": 1,
                "strategy": {"type": "Recreate"},
                "selector": {"matchLabels": {"app.kubernetes.io/name": name}},
                "template": {
                    "metadata": {"labels": labels},
                    "spec": {
                        "automountServiceAccountToken": False,
                        "nodeSelector": {"kubernetes.io/hostname": args.node_name},
                        "securityContext": {"fsGroup": 1000, "seLinuxOptions": {"type": "spc_t"}},
                        "initContainers": [
                            {
                                "name": "prepare-data",
                                "image": args.storage_image,
                                "imagePullPolicy": "Never",
                                "securityContext": {"runAsUser": 0, "runAsGroup": 0},
                                "command": ["/bin/sh", "-ec"],
                                "args": [
                                    "mkdir -p /data/.neon/pageserver /data/.neon/shared_remote_storage && "
                                    "chown -R 1000:1000 /data/.neon/pageserver /data/.neon/shared_remote_storage"
                                ],
                                "volumeMounts": [
                                    {"name": "data", "mountPath": "/data/.neon/pageserver"},
                                    {"name": "remote-storage", "mountPath": "/data/.neon/shared_remote_storage"},
                                ],
                            }
                        ],
                        "containers": [
                            {
                                "name": "pageserver",
                                "image": args.storage_image,
                                "imagePullPolicy": "Never",
                                "command": ["pageserver"],
                                "args": ["-D", "/data/.neon/pageserver"],
                                "ports": [
                                    {"name": "pg", "containerPort": 6400},
                                    {"name": "http", "containerPort": 9898},
                                ],
                                "readinessProbe": {
                                    "tcpSocket": {"port": "pg"},
                                    "periodSeconds": 2,
                                    "failureThreshold": 60,
                                },
                                "resources": {
                                    "requests": {"cpu": "500m", "memory": "2Gi"},
                                    "limits": {"cpu": "4", "memory": "16Gi"},
                                },
                                "volumeMounts": [
                                    {"name": "data", "mountPath": "/data/.neon/pageserver"},
                                    {"name": "remote-storage", "mountPath": "/data/.neon/shared_remote_storage"},
                                    {
                                        "name": "config",
                                        "mountPath": "/data/.neon/pageserver/identity.toml",
                                        "subPath": "identity.toml",
                                        "readOnly": True,
                                    },
                                    {
                                        "name": "config",
                                        "mountPath": "/data/.neon/pageserver/pageserver.toml",
                                        "subPath": "pageserver.toml",
                                        "readOnly": True,
                                    },
                                    {
                                        "name": "config",
                                        "mountPath": "/data/.neon/pageserver/metadata.json",
                                        "subPath": "metadata.json",
                                        "readOnly": True,
                                    },
                                ],
                            }
                        ],
                        "volumes": [
                            {
                                "name": "data",
                                "hostPath": {"path": f"{args.data_root}/{name}", "type": "DirectoryOrCreate"},
                            },
                            {
                                "name": "remote-storage",
                                "hostPath": {
                                    "path": f"{args.data_root}/shared-remote-storage",
                                    "type": "DirectoryOrCreate",
                                },
                            },
                            {"name": "config", "configMap": {"name": config_name}},
                        ],
                    },
                },
            },
        },
    ]
    return node_id, name, items


def cmd_safekeeper(args: argparse.Namespace) -> None:
    if args.action == "list":
        deployments = kubectl_json(["get", "deploy", "-n", args.namespace]).get("items", [])
        print_json(
            {
                "registered": storage_controller_request(args.namespace, "GET", "/control/v1/safekeeper"),
                "deployments": [
                    deployment_summary(item)
                    for item in deployments
                    if re.fullmatch(r"safekeeper[0-9]+", item.get("metadata", {}).get("name", ""))
                ],
            }
        )
    elif args.action == "status":
        print_json(
            {
                "registered": storage_controller_request(args.namespace, "GET", f"/control/v1/safekeeper/{args.id}"),
                "deployment": kubectl_json(["get", "deploy", f"safekeeper{args.id}", "-n", args.namespace]),
            }
        )
    elif args.action == "add":
        if int(args.id) == 1:
            raise SystemExit("safekeeper1 is part of the base storage stack")
        items = safekeeper_items(args)
        path = write_generated(f"safekeeper{args.id}.yaml", items)
        output = apply_items(items, dry_run=args.dry_run)
        if args.dry_run:
            print(output, end="")
            print(f"generated={path}")
            return
        rollout(args.namespace, f"safekeeper{args.id}", args.timeout)
        storage_controller_request(
            args.namespace,
            "POST",
            f"/control/v1/safekeeper/{args.id}",
            {
                "id": int(args.id),
                "region_id": "local",
                "version": 1,
                "host": f"safekeeper{args.id}",
                "port": 5454,
                "http_port": 7676,
                "https_port": None,
                "availability_zone_id": f"local-{args.id}",
            },
        )
        storage_controller_request(
            args.namespace,
            "POST",
            f"/control/v1/safekeeper/{args.id}/scheduling_policy",
            {"scheduling_policy": "Active"},
        )
        print(f"started safekeeper{args.id}")
        print(f"generated={path}")
    elif args.action == "remove":
        if int(args.id) == 1 and not args.force:
            raise SystemExit("refusing to remove safekeeper1; pass --force if you really mean it")
        if not args.dry_run:
            try:
                storage_controller_request(
                    args.namespace,
                    "POST",
                    f"/control/v1/safekeeper/{args.id}/scheduling_policy",
                    {"scheduling_policy": "Decomissioned"},
                )
            except Exception as exc:
                print(f"warning: could not mark safekeeper{args.id} decomissioned: {exc}", file=sys.stderr)
        print(delete_resource(args.namespace, "deploy", f"safekeeper{args.id}", dry_run=args.dry_run), end="")
        print(delete_resource(args.namespace, "svc", f"safekeeper{args.id}", dry_run=args.dry_run), end="")


def cmd_pageserver(args: argparse.Namespace) -> None:
    if args.action == "list":
        deployments = kubectl_json(["get", "deploy", "-n", args.namespace]).get("items", [])
        print_json(
            {
                "registered": storage_controller_request(args.namespace, "GET", "/control/v1/node"),
                "deployments": [
                    deployment_summary(item)
                    for item in deployments
                    if re.fullmatch(r"pageserver[0-9]*", item.get("metadata", {}).get("name", ""))
                ],
            }
        )
    elif args.action == "status":
        ordinal = int(args.ref)
        name = "pageserver" if ordinal == 1 else f"pageserver{ordinal}"
        print_json(
            {
                "registered": storage_controller_request(args.namespace, "GET", "/control/v1/node"),
                "deployment": kubectl_json(["get", "deploy", name, "-n", args.namespace]),
            }
        )
    elif args.action == "add":
        node_id, name, items = pageserver_items(args)
        path = write_generated(f"{name}.yaml", items)
        output = apply_items(items, dry_run=args.dry_run)
        if args.dry_run:
            print(output, end="")
            print(f"generated={path}")
            return
        rollout(args.namespace, name, args.timeout)
        deadline = time.time() + args.timeout
        while time.time() < deadline:
            nodes = storage_controller_request(args.namespace, "GET", "/control/v1/node")
            if any(node.get("id") == node_id and node.get("availability") == "Active" for node in nodes):
                print(f"started {name}")
                print(f"node_id={node_id}")
                print(f"generated={path}")
                return
            time.sleep(1)
        raise SystemExit(f"timed out waiting for {name} node_id={node_id} to register")
    elif args.action == "remove":
        ordinal = int(args.ref)
        if ordinal == 1 and not args.force:
            raise SystemExit("refusing to remove built-in pageserver; pass --force if you really mean it")
        name = "pageserver" if ordinal == 1 else f"pageserver{ordinal}"
        node_id = args.node_id or 1000 + ordinal
        if not args.force and not args.dry_run:
            shards = docker_control_plane_request(args.namespace, "GET", "/v1/pageserver/shards", check=False) or []
            attached = [row for row in shards if row.get("node_attached") == node_id]
            if attached:
                raise SystemExit(f"refusing to remove {name}: tenant shards are still attached to node {node_id}; pass --force")
        if not args.dry_run:
            try:
                docker_control_plane_request(args.namespace, "POST", f"/v1/pageserver/{node_id}/delete", {"force": args.force}, check=False)
            except Exception as exc:
                print(f"warning: could not delete pageserver node {node_id} from control plane: {exc}", file=sys.stderr)
        print(delete_resource(args.namespace, "deploy", name, dry_run=args.dry_run), end="")
        print(delete_resource(args.namespace, "svc", name, dry_run=args.dry_run), end="")
        print(delete_resource(args.namespace, "configmap", f"{name}-config", dry_run=args.dry_run), end="")


def current_parent_identity(args: argparse.Namespace) -> tuple[str, str]:
    if args.tenant_id and args.ancestor_timeline_id:
        return args.tenant_id, args.ancestor_timeline_id

    # One kubectl invocation for metadata, Deployment and pool preflight.
    resources = [f"configmap/{args.parent}-metadata", f"deployment/{args.parent}"]
    pool = getattr(args, "from_pool", None)
    if pool:
        resources.append(f"dbresourcepool/{pool}")
    objects = kubectl_json(["get", *resources, "-n", args.namespace, "--ignore-not-found"])
    items = objects.get("items", []) if objects.get("kind") == "List" else [objects]
    if pool:
        if not any(item.get("kind") == "DBResourcePool"
                   and item["metadata"]["name"] == pool for item in items):
            raise RuntimeError(f"resource pool {args.namespace}/{pool} does not exist")
        args._validated_pool = (args.namespace, pool)
    for item in items:
        if item.get("kind") == "ConfigMap":
            raw = item.get("data", {}).get("branch.json")
            try:
                metadata = json.loads(raw) if raw else None
            except json.JSONDecodeError:
                metadata = None
            if isinstance(metadata, dict):
                return (args.tenant_id or str(metadata["tenant_id"]),
                        args.ancestor_timeline_id or str(metadata["timeline_id"]))
    if not any(item.get("kind") == "Deployment" for item in items):
        raise SystemExit(
            f"parent {args.parent} is neither a Deployment nor a pool branch metadata ConfigMap; "
            "pass --tenant-id and --ancestor-timeline-id explicitly if needed"
        )
    output = gsql(
        args.namespace,
        args.parent,
        "postgres",
        "SHOW neon.tenant_id; SHOW neon.timeline_id;",
        og_version=args.og_version,
    )
    values = [line.strip() for line in output.splitlines() if line.strip()]
    if len(values) < 2:
        raise RuntimeError(f"parent {args.parent} did not return tenant and timeline settings: {output!r}")
    tenant_id = args.tenant_id or values[0]
    timeline_id = args.ancestor_timeline_id or values[1]
    return tenant_id, timeline_id


def timeline_last_record_lsn(namespace: str, tenant_id: str, timeline_id: str) -> str:
    info = storage_controller_request(namespace, "GET", f"/control/v1/tenant/{tenant_id}/timeline/{timeline_id}")
    if isinstance(info, dict):
        if info.get("last_record_lsn"):
            return str(info["last_record_lsn"])
        shards = info.get("shards") or []
        for shard in shards:
            if shard.get("last_record_lsn"):
                return str(shard["last_record_lsn"])
            if shard.get("remote_consistent_lsn"):
                return str(shard["remote_consistent_lsn"])
    raise RuntimeError(f"cannot find last_record_lsn for tenant={tenant_id} timeline={timeline_id}: {info}")


def pageserver_connstring_for_tenant(namespace: str, tenant_id: str) -> str:
    try:
        locate = storage_controller_request(namespace, "GET", f"/debug/v1/tenant/{tenant_id}/locate")
        shard = (locate.get("shards") or [])[0]
        host = shard.get("listen_pg_addr")
        port = shard.get("listen_pg_port")
        if host and port:
            return f"host={host} port={port}"
    except Exception as exc:
        print(f"warning: could not locate tenant pageserver, falling back to pageserver:6400: {exc}", file=sys.stderr)
    return "host=pageserver port=6400"


def pool_branch_metadata_item(
    args: argparse.Namespace,
    tenant_id: str,
    timeline_id: str,
    ancestor_timeline_id: str,
    ancestor_start_lsn: str,
    allocation: dict[str, str],
    warm_settings: dict[str, str] | None = None,
) -> dict[str, Any]:
    labels = common_labels(args.branch_name, "pool-branch-compute")
    labels["branching.openfuyao.cn/tenant-id"] = tenant_id
    labels["branching.openfuyao.cn/timeline-id"] = timeline_id
    labels["branching.openfuyao.cn/ancestor-timeline-id"] = ancestor_timeline_id
    return {
        "apiVersion": "v1",
        "kind": "ConfigMap",
        "metadata": {
            "name": f"{args.branch_name}-metadata",
            "namespace": args.namespace,
            "labels": labels,
        },
        "data": {
            "branch.json": json.dumps(
                {
                    "branch": args.branch_name,
                    "mode": args.mode,
                    "pool": args.from_pool,
                    "pod_name": allocation["pod_name"],
                    "pod_id": allocation["pod_id"],
                    "endpoint": allocation["endpoint"],
                    "tenant_id": tenant_id,
                    "timeline_id": timeline_id,
                    "ancestor_timeline_id": ancestor_timeline_id,
                    "ancestor_start_lsn": ancestor_start_lsn,
                    "oggit_enabled": args.enable_oggit,
                    "oggit_database": args.oggit_database,
                    "warm_settings": warm_settings or {},
                },
                indent=2,
                sort_keys=True,
            )
            + "\n"
        },
    }


def get_pool_branch_metadata(namespace: str, branch_name: str) -> dict[str, Any] | None:
    cm = kubectl_json_optional(["get", "configmap", f"{branch_name}-metadata", "-n", namespace])
    if not cm:
        return None
    raw = cm.get("data", {}).get("branch.json")
    if not raw:
        return None
    try:
        data = json.loads(raw)
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def get_pending_pool_metadata(namespace: str, branch_name: str) -> dict[str, Any] | None:
    cm = kubectl_json_optional(["get", "configmap", f"{branch_name}-pending", "-n", namespace])
    if not cm:
        return None
    raw = cm.get("data", {}).get("branch.json")
    try:
        data = json.loads(raw) if raw else None
    except json.JSONDecodeError:
        return None
    return data if isinstance(data, dict) else None


def pool_branch_pod(namespace: str, branch_name: str) -> str | None:
    instances = kubectl_json_optional(["get", "dbinstance", "-n", namespace, "-l", f"app-ref={branch_name}"])
    if instances:
        for item in instances.get("items", []):
            pod_name = item.get("status", {}).get("podName")
            if pod_name:
                return str(pod_name)
    metadata = get_pool_branch_metadata(namespace, branch_name)
    if metadata and metadata.get("pod_name"):
        return str(metadata["pod_name"])
    return None


def safekeeper_routes_from_timeline_response(timeline_response: Any) -> tuple[str, str]:
    if isinstance(timeline_response, dict):
        safekeepers = timeline_response.get("safekeepers", {}).get("safekeepers", [])
        hosts = [str(item.get("hostname")) for item in safekeepers if item.get("hostname")]
        if hosts:
            return ",".join(f"{host}:5454" for host in hosts), ",".join(f"http://{host}:7676" for host in hosts)
    return DEFAULT_SAFEKEEPER_CONNSTRINGS, DEFAULT_SAFEKEEPER_HTTP_URLS


def branch_items(args: argparse.Namespace, tenant_id: str, timeline_id: str, ancestor_timeline_id: str, ancestor_start_lsn: str) -> list[dict[str, Any]]:
    parent = kubectl_json(["get", "deploy", "compute-main", "-n", args.namespace])
    return branch_items_from_template(args, parent, tenant_id, timeline_id, ancestor_timeline_id, ancestor_start_lsn)


def branch_items_from_template(args, parent, tenant_id, timeline_id, ancestor_timeline_id, ancestor_start_lsn):
    # Template supplies only compute configuration; ancestry always comes from the selected parent.
    spec = copy.deepcopy(parent["spec"])
    name = args.branch_name
    labels = common_labels(name, "branch-compute")
    labels["branching.openfuyao.cn/tenant-id"] = tenant_id
    labels["branching.openfuyao.cn/timeline-id"] = timeline_id
    labels["branching.openfuyao.cn/ancestor-timeline-id"] = ancestor_timeline_id
    spec["replicas"] = 1
    spec["selector"] = {"matchLabels": {"app.kubernetes.io/name": name}}
    spec["template"].setdefault("metadata", {})["labels"] = labels
    for container in spec["template"]["spec"].get("initContainers", []):
        container["args"] = [value.replace("/endpoints/main", "/endpoints/" + name)
                             .replace("chown -R 1000:1000 /work/gaussdb /data/.neon/control_plane",
                                      "chown -R 1000:1000 /work/gaussdb /data/.neon/control_plane/endpoints/" + name)
                             for value in container.get("args", [])]
    containers = spec["template"]["spec"]["containers"]
    compute = next((item for item in containers if item.get("name") == "compute"), None)
    if compute is None:
        raise RuntimeError(f"{args.parent} does not contain compute container")
    upsert_env(
        compute,
        {
            "TENANT_ID": tenant_id,
            "TIMELINE_ID": timeline_id,
            "ANCESTOR_TIMELINE_ID": ancestor_timeline_id,
            "ANCESTOR_START_LSN": ancestor_start_lsn,
            "BRANCH_NAME": name,
            "ENDPOINT_ID": name,
            "ENDPOINT_SERVICE_NAME": name,
            "ENDPOINT_STATUS_DIR": f"/data/.neon/control_plane/endpoints/{name}",
                    "OGGIT_ENABLED": "true" if args.enable_oggit else "false",
                    "OGGIT_DATABASE": args.oggit_database,
                    "SAFEKEEPER_CONNSTRINGS": DEFAULT_SAFEKEEPER_CONNSTRINGS,
            "OGGIT_SAFEKEEPER_HTTP_URLS": DEFAULT_SAFEKEEPER_HTTP_URLS,
        },
    )
    return [
        service(
            name,
            args.namespace,
            name,
            [
                {"name": "postgres", "port": 55433, "targetPort": "postgres"},
                {"name": "http", "port": 3080, "targetPort": "http"},
            ],
            "branch-compute",
        ),
        {
            "apiVersion": "apps/v1",
            "kind": "Deployment",
            "metadata": {"name": name, "namespace": args.namespace, "labels": labels},
            "spec": spec,
        },
    ]


def ensure_oggit_enabled(args: argparse.Namespace, deployment: str) -> None:
    try:
        enabled = show_branch_setting(args.namespace, deployment, "neon.oggit_enabled", og_version=args.og_version).lower()
    except Exception as exc:
        raise SystemExit(
            f"{deployment} has no readable OGGIT setting: {exc}"
        ) from exc
    if enabled not in {"on", "true", "1"}:
        raise SystemExit(
            f"{deployment} has OGGIT disabled; run 'branch oggit enable {deployment}'"
        )


def set_existing_branch_oggit(args: argparse.Namespace) -> None:
    """Toggle OGGIT and restart the compute process managed by the Pod."""
    deployment = kubectl_json_optional(["get", "deploy", args.branch_name, "-n", args.namespace])
    pod_name = None if deployment else pool_branch_pod(args.namespace, args.branch_name)
    if not deployment and not pod_name:
        raise SystemExit(f"cannot find Deployment or pool Pod for {args.branch_name}")
    if args.dry_run:
        target = f"deploy/{args.branch_name}" if deployment else pod_name
        state = "on" if args.oggit_action == "enable" else "off"
        print(f"would set OGGIT {state} in {target} and restart its compute process in-place")
        return

    state = "on" if args.oggit_action == "enable" else "off"
    database = args.oggit_database
    script = r'''set -euo pipefail
oggit_state=$1
oggit_database=$2
og_version=$3
export GAUSSHOME="/usr/local/${og_version}"
export PATH="${GAUSSHOME}/bin:${PATH}"
export LD_LIBRARY_PATH="/usr/lib64:${GAUSSHOME}/lib:${LD_LIBRARY_PATH:-}"
gsql=("${GAUSSHOME}/bin/gsql" -X -A -t -d postgres -U cloud_admin -p 55433 -h 127.0.0.1)
show_setting() {
  "${gsql[@]}" -c "SHOW $1;" | awk 'NF { value=$0 } END { print value }'
}
sql_quote() {
  local value=${1//\'/\'\'}
  printf "%s" "${value}"
}
tenant_id=$(show_setting neon.tenant_id)
timeline_id=$(show_setting neon.timeline_id)
ancestor_timeline_id=$(show_setting neon.oggit_ancestor_timeline_id)
ancestor_start_lsn=$(show_setting neon.oggit_branch_start_lsn)
pageserver_connstring=$(show_setting neon.pageserver_connstring)
safekeeper_connstrings=$(show_setting neon.safekeepers)
safekeeper_http_urls=$(show_setting neon.oggit_safekeeper_http_urls)
set_string() {
  local name=$1 value=$2
  [[ -n "${value}" ]] || return 0
  "${gsql[@]}" -c "ALTER SYSTEM SET ${name} = '$(sql_quote "${value}")';"
}
set_string neon.oggit_tenant_id "${tenant_id}"
set_string neon.oggit_timeline_id "${timeline_id}"
set_string neon.oggit_ancestor_timeline_id "${ancestor_timeline_id}"
set_string neon.oggit_branch_start_lsn "${ancestor_start_lsn}"
set_string neon.pageserver_connstring "${pageserver_connstring}"
set_string neon.safekeepers "${safekeeper_connstrings}"
set_string neon.oggit_safekeeper_http_urls "${safekeeper_http_urls:-http://safekeeper:7676}"
if [[ "${oggit_database}" == "postgres" ]]; then
  # openGauss does not support PostgreSQL's ALTER SYSTEM RESET syntax.
  # The default OGGIT database is postgres, so set it explicitly instead.
  "${gsql[@]}" -c "ALTER SYSTEM SET neon.oggit_database = 'postgres';"
else
  set_string neon.oggit_database "${oggit_database}"
fi
"${gsql[@]}" -c "ALTER SYSTEM SET enable_subscription = on;"
"${gsql[@]}" -c "ALTER SYSTEM SET neon.oggit_enabled = ${oggit_state};"
if [[ "${oggit_state}" == "on" ]]; then
  oggit_enabled=true
else
  oggit_enabled=false
fi
pool_env=/var/db/gaussdb/pool-rebind.env
if [[ -f "${pool_env}" ]]; then
  pool_env_tmp=$(mktemp)
  grep -vE '^export OGGIT_(ENABLED|DATABASE)=' "${pool_env}" > "${pool_env_tmp}" || true
  printf 'export OGGIT_ENABLED=%q\n' "${oggit_enabled}" >> "${pool_env_tmp}"
  printf 'export OGGIT_DATABASE=%q\n' "${oggit_database}" >> "${pool_env_tmp}"
  cat "${pool_env_tmp}" > "${pool_env}"
  rm -f "${pool_env_tmp}"
fi
"${GAUSSHOME}/bin/gs_ctl" restart -D /var/db/gaussdb/compute -m fast -w -t 60
'''
    command = ["bash", "-s", "--", state, database, args.og_version]
    if deployment:
        # Restarting compute from inside the exec session can kill that session
        # and make kubectl report 137 even though the database restart itself
        # was requested successfully.  Inspect the result below and tolerate
        # only that expected exit code.
        result = exec_deploy(
            args.namespace,
            args.branch_name,
            "compute",
            command,
            input_text=script,
            timeout=args.timeout,
            check=False,
        )
    else:
        result = exec_pod(
            args.namespace,
            pod_name,
            "compute",
            command,
            input_text=script,
            timeout=args.timeout,
            check=False,
        )
    if result.returncode not in (0, 137):
        raise CommandError(["kubectl", "exec", *command], result)
    if result.returncode == 137:
        print(
            "warning: compute restart terminated the kubectl exec session (exit 137); "
            "the restart was initiated",
            file=sys.stderr,
        )
    if deployment:
        # compute.sh regenerates its PostgreSQL settings from these
        # Deployment variables whenever the container starts. Persist the
        # requested state there as well, otherwise a restart restores the
        # previous OGGIT_ENABLED value (for example, false).
        kubectl(
            [
                "set",
                "env",
                f"deployment/{args.branch_name}",
                "-c",
                "compute",
                "-n",
                args.namespace,
                f"OGGIT_ENABLED={'true' if state == 'on' else 'false'}",
                f"OGGIT_DATABASE={database}",
            ],
            timeout=60,
        )
        rollout(args.namespace, args.branch_name, args.timeout)
    else:
        metadata = get_pool_branch_metadata(args.namespace, args.branch_name)
        if metadata is not None:
            metadata["oggit_enabled"] = state == "on"
            metadata["oggit_database"] = database
            apply_items(
                [
                    {
                        "apiVersion": "v1",
                        "kind": "ConfigMap",
                        "metadata": {
                            "name": f"{args.branch_name}-metadata",
                            "namespace": args.namespace,
                        },
                        "data": {
                            "branch.json": json.dumps(metadata, indent=2, sort_keys=True) + "\n"
                        },
                    }
                ]
            )
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    print_json({"branch": args.branch_name, "oggit_enabled": state, "oggit_database": database, "pod": pod_name})


def show_branch_setting(namespace: str, branch_name: str, setting: str, *, og_version: str) -> str:
    if kubectl_json_optional(["get", "deploy", branch_name, "-n", namespace]):
        return show_setting(namespace, branch_name, setting, og_version=og_version)
    pod_name = pool_branch_pod(namespace, branch_name)
    if not pod_name:
        raise RuntimeError(f"cannot find Deployment or pool Pod for branch {branch_name}")
    output = gsql_pod(namespace, pod_name, "postgres", f"SHOW {setting};", og_version=og_version)
    lines = [line.strip() for line in output.splitlines() if line.strip()]
    return lines[-1] if lines else ""


def merge_user_password(args: argparse.Namespace) -> tuple[str, str]:
    user = args.user
    password = args.password
    if password is None:
        if user == "branch_merge":
            password = secret_value(args.namespace, args.secret_name, "branch-merge-password")
        else:
            raise SystemExit("--password is required when --user is not branch_merge")
    return user, password


def run_branching_local(args: argparse.Namespace, branching_args: list[str]) -> None:
    result = exec_deploy(
        args.namespace,
        "endpoint-storage",
        "endpoint-storage",
        ["/usr/local/bin/neon_local", *branching_args],
        timeout=args.timeout,
        check=False,
    )
    if result.stdout:
        print(result.stdout, end="")
    if result.stderr:
        print(result.stderr, end="", file=sys.stderr)
    raise SystemExit(result.returncode)


def cmd_branch(args: argparse.Namespace) -> None:
    if args.action == "oggit":
        set_existing_branch_oggit(args)
    elif args.action == "list":
        print_json(
            {
                "deployments": kubectl_json(
                    ["get", "deploy", "-n", args.namespace, "-l", "app.kubernetes.io/component=branch-compute"]
                ),
                "pool_branches": kubectl_json(
                    ["get", "configmap", "-n", args.namespace, "-l", "app.kubernetes.io/component=pool-branch-compute"]
                ),
            }
        )
    elif args.action == "status":
        deployment = kubectl_json_optional(["get", "deploy", args.branch_name, "-n", args.namespace])
        if not deployment:
            pod_name = pool_branch_pod(args.namespace, args.branch_name)
            print_json(
                {
                    "metadata": get_pool_branch_metadata(args.namespace, args.branch_name),
                    "dbinstance": kubectl_json_optional(
                        ["get", "dbinstance", "-n", args.namespace, "-l", f"app-ref={args.branch_name}"]
                    ),
                    "service": kubectl_json_optional(["get", "svc", args.branch_name, "-n", args.namespace]),
                    "pod": kubectl_json_optional(["get", "pod", pod_name, "-n", args.namespace]) if pod_name else None,
                }
            )
            return
        print_json(
            {
                "deployment": deployment,
                "pods": kubectl_json(["get", "pod", "-n", args.namespace, "-l", f"app.kubernetes.io/name={args.branch_name}"]),
            }
        )
    elif args.action == "connect":
        target = f"deploy/{args.branch_name}"
        if not kubectl_json_optional(["get", "deploy", args.branch_name, "-n", args.namespace]):
            pod_name = pool_branch_pod(args.namespace, args.branch_name)
            if not pod_name:
                raise SystemExit(f"cannot find Deployment or pool Pod for {args.branch_name}")
            target = pod_name
        cmd = [
            "kubectl",
            "exec",
            "-it",
            "-n",
            args.namespace,
            target,
            "-c",
            "compute",
            "--",
            f"/usr/local/{args.og_version}/bin/gsql",
            "-d",
            args.database,
            "-U",
            args.db_user,
            "-p",
            "55433",
            "-h",
            "127.0.0.1",
        ]
        os.execvp("kubectl", cmd)
    elif args.action == "create":
        # Pool-backed branches always reuse an allocated compute Pod; regular
        # branches create a dedicated Deployment. Keep this internal mode
        # metadata, but derive it from --from-pool instead of exposing a
        # redundant CLI switch.
        if args.from_pool and not args.dry_run:
            raise RuntimeError("resource-pool creation requires COORDINATOR_URL and its token file")
        args.mode = "restart-only" if args.from_pool else "deployment"
        tenant_id, ancestor_timeline_id = current_parent_identity(args)
        timeline_id = args.timeline_id or secrets.token_hex(16)

        if args.from_pool:
            if args.dry_run:
                with ThreadPoolExecutor(max_workers=2) as executor:
                    pageserver_future = executor.submit(
                        pageserver_connstring_for_tenant,
                        args.namespace,
                        tenant_id,
                    )
                    ancestor_lsn_future = None
                    if not args.ancestor_start_lsn:
                        ancestor_lsn_future = executor.submit(
                            timeline_last_record_lsn,
                            args.namespace,
                            tenant_id,
                            ancestor_timeline_id,
                        )
                    pageserver_connstring = pageserver_future.result()
                    ancestor_start_lsn = (
                        args.ancestor_start_lsn
                        if args.ancestor_start_lsn
                        else ancestor_lsn_future.result()
                    )
                allocation_placeholder = {
                    "endpoint": f"{args.branch_name}.{args.namespace}.svc.cluster.local:55433",
                    "pod_name": "<allocated-by-operator>",
                    "pod_id": "<allocated-by-operator>",
                }
                path = write_generated(
                    f"{args.branch_name}-pool-metadata.yaml",
                    [pool_branch_metadata_item(args, tenant_id, timeline_id, ancestor_timeline_id, ancestor_start_lsn, allocation_placeholder)],
                )
                print_json(
                    {
                        "dry_run": True,
                        "branch": args.branch_name,
                        "parent": args.parent,
                        "mode": args.mode,
                        "pool": args.from_pool,
                        "tenant_id": tenant_id,
                        "timeline_id": timeline_id,
                        "ancestor_timeline_id": ancestor_timeline_id,
                        "ancestor_start_lsn": ancestor_start_lsn,
                        "pageserver_connstring": pageserver_connstring,
                        "safekeeper_connstrings": DEFAULT_SAFEKEEPER_CONNSTRINGS,
                        "oggit_enabled": args.enable_oggit,
                        "generated": str(path),
                        "note": "dry-run does not allocate a pool instance or create the child timeline",
                    }
                )
                return

        ancestor_start_lsn = args.ancestor_start_lsn or timeline_last_record_lsn(
            args.namespace,
            tenant_id,
            ancestor_timeline_id,
        )
        items = branch_items(args, tenant_id, timeline_id, ancestor_timeline_id, ancestor_start_lsn)
        path = write_generated(f"{args.branch_name}.yaml", items)
        output = apply_items(items, dry_run=args.dry_run)
        if args.dry_run:
            print(output, end="")
            print(f"generated={path}")
            return
        rollout(args.namespace, args.branch_name, args.timeout)
        print_json(
            {
                "branch": args.branch_name,
                "parent": args.parent,
                "tenant_id": tenant_id,
                "timeline_id": timeline_id,
                "ancestor_timeline_id": ancestor_timeline_id,
                "ancestor_start_lsn": ancestor_start_lsn,
                "oggit_enabled": args.enable_oggit,
                "generated": str(path),
            }
        )
    elif args.action == "delete":
        deployment = kubectl_json_optional(["get", "deploy", args.branch_name, "-n", args.namespace])
        metadata = get_pool_branch_metadata(args.namespace, args.branch_name)
        pending = get_pending_pool_metadata(args.namespace, args.branch_name)
        pool_instance = kubectl_json_optional(["get", "dbinstance", "-n", args.namespace, "-l", f"app-ref={args.branch_name}"])
        pool_service = kubectl_json_optional(["get", "svc", args.branch_name, "-n", args.namespace])
        if not deployment and (metadata or pending or pool_instance or pool_service):
            if args.dry_run:
                print(f"pool branch {args.branch_name} would be returned through the coordinator")
                return
            raise RuntimeError("resource-pool deletion requires COORDINATOR_URL and its token file")
        if not deployment:
            raise SystemExit(f"cannot find Deployment or pool branch metadata for {args.branch_name}")
        labels = deployment.get("metadata", {}).get("labels", {})
        tenant_id = labels.get("branching.openfuyao.cn/tenant-id")
        timeline_id = labels.get("branching.openfuyao.cn/timeline-id")
        print(delete_resource(args.namespace, "deploy", args.branch_name, dry_run=args.dry_run), end="")
        print(delete_resource(args.namespace, "svc", args.branch_name, dry_run=args.dry_run), end="")
        if args.delete_timeline and tenant_id and timeline_id and not args.dry_run:
            cmd, input_text = container_curl_args("DELETE", f"http://127.0.0.1:9898/v1/tenant/{tenant_id}/timeline/{timeline_id}")
            exec_deploy(args.namespace, "pageserver", "pageserver", cmd, input_text=input_text, timeout=args.timeout, check=False)
    elif args.action == "oggit-status":
        settings = [
            "neon.oggit_enabled",
            "neon.oggit_tenant_id",
            "neon.oggit_timeline_id",
            "neon.oggit_ancestor_timeline_id",
            "neon.oggit_branch_start_lsn",
        ]
        rows = {}
        for setting in settings:
            try:
                rows[setting] = show_branch_setting(args.namespace, args.branch_name, setting, og_version=args.og_version)
            except Exception as exc:
                rows[setting] = f"ERROR: {exc}"
        print_json(rows)
    elif args.action in {"diff", "merge"}:
        source = args.source
        target = "compute-main" if args.target == "main" else args.target
        ensure_oggit_enabled(args, source)
        ensure_oggit_enabled(args, target)
        user, password = merge_user_password(args)
        branching_args = [
            "branch",
            args.action,
            "--incremental-oggit",
            "--database",
            args.database,
            "--source-connstr",
            branch_connstr(args.namespace, source, args.database, user, password),
            "--target-connstr",
            branch_connstr(args.namespace, target, args.database, user, password),
            "--user",
            user,
            "--source-schema",
            args.source_schema,
            "--target-schema",
            args.target_schema,
        ]
        if args.keep_fdw:
            branching_args.append("--keep-fdw")
        if args.action == "merge":
            branching_args.extend(["--strategy", args.strategy])
        run_branching_local(args, branching_args)
    elif args.action in {"merge-status", "conflicts", "continue", "abort"}:
        target = "compute-main" if args.target == "main" else args.target
        ensure_oggit_enabled(args, target)
        user, password = merge_user_password(args)
        branching_args = [
            "branch",
            args.action,
            "--database",
            args.database,
            "--target-connstr",
            branch_connstr(args.namespace, target, args.database, user, password),
            "--merge-id",
            args.merge_id,
            "--user",
            user,
        ]
        run_branching_local(args, branching_args)
    elif args.action == "resolve":
        target = "compute-main" if args.target == "main" else args.target
        ensure_oggit_enabled(args, target)
        user, password = merge_user_password(args)
        branching_args = [
            "branch",
            "resolve",
            "--database",
            args.database,
            "--target-connstr",
            branch_connstr(args.namespace, target, args.database, user, password),
            "--merge-id",
            args.merge_id,
            "--conflict-id",
            str(args.conflict_id),
            "--user",
            user,
        ]
        if args.custom_sql:
            branching_args.extend(["--custom-sql", args.custom_sql])
        else:
            branching_args.extend(["--resolution", args.resolution])
        run_branching_local(args, branching_args)


def add_common(parser: argparse.ArgumentParser) -> None:
    parser.add_argument("--namespace", default=DEFAULT_NAMESPACE)
    parser.add_argument("--data-root", default=DEFAULT_DATA_ROOT)
    parser.add_argument("--deploy-root", default=DEFAULT_DEPLOY_ROOT)
    parser.add_argument("--storage-image", default=DEFAULT_STORAGE_IMAGE)
    parser.add_argument("--compute-image", default=DEFAULT_COMPUTE_IMAGE)
    parser.add_argument("--og-version", default=DEFAULT_OG_VERSION)
    parser.add_argument("--node-name", default=DEFAULT_NODE_NAME)
    parser.add_argument("--operator-url", default=DEFAULT_OPERATOR_URL)
    parser.add_argument("--timeout", type=int, default=300)
    parser.add_argument("--dry-run", action="store_true")


def pitr_pageserver_request(namespace, method, shard, timeline_id, query=None, body=None):
    host = shard["listen_http_addr"]
    if ":" in host and not host.startswith("["):
        host = "[" + host + "]"
    url = "http://%s:%s/v1/tenant/%s/timeline/%s" % (
        host, shard["listen_http_port"], shard["shard_id"], timeline_id)
    url += "/get_lsn_by_timestamp" if method == "GET" else "/lsn_lease"
    if query:
        url += "?" + urllib.parse.urlencode(query)
    # Capture response bodies on HTTP errors as well as successful responses.
    cmd = ["curl", "--noproxy", "*", "-sS", "--max-time", "120", "-X", method,
           "-w", "\n%{http_code}"]
    input_text = None
    if body is not None:
        cmd += ["-H", "Content-Type: application/json", "--data-binary", "@-"]
        input_text = json.dumps(body)
    cmd.append(url)
    result = exec_deploy(namespace, "storage-controller", "storage-controller", cmd,
                         input_text=input_text, timeout=130)
    payload, status = result.stdout.rsplit("\n", 1)
    if not status.startswith("2"):
        raise RuntimeError("pageserver HTTP %s: %s" % (status, payload))
    return json.loads(payload)


def cmd_pitr(args):
    if args.describe:
        raise RuntimeError("PITR metadata requires the configured coordinator")
    timestamp = pitr_timestamp(args.time)
    if args.from_pool:
        argv = ["pitr", args.branch_name]
        for key in ("parent", "time", "from_pool", "pool_fallback", "timeline_id", "tenant_id",
                    "ancestor_timeline_id", "source_timeline_id", "data_root", "deploy_root",
                    "storage_image", "compute_image", "og_version", "node_name", "operator_url", "timeout"):
            value = getattr(args, key, None)
            if value is not None:
                argv += ["--" + key.replace("_", "-"), str(value)]
        if args.dry_run:
            argv.append("--dry-run")
        if coordinator_request(args, argv):
            return
        raise RuntimeError("resource-pool PITR requires COORDINATOR_URL and its token file")
    resources = ["deployment/" + args.branch_name, "service/" + args.branch_name,
                 "configmap/" + args.branch_name + "-metadata",
                 "configmap/" + args.branch_name + "-pending"]
    result = kubectl(["get", *resources, "-n", args.namespace, "--ignore-not-found", "-o", "json"])
    existing = json.loads(result.stdout) if result.stdout.strip() else {"items": []}
    if existing.get("items") or existing.get("metadata", {}).get("name"):
        raise RuntimeError("recovery branch '%s' already exists" % args.branch_name)
    tenant_id, ancestor = current_parent_identity(args)
    located = storage_controller_request(args.namespace, "GET", "/debug/v1/tenant/%s/locate" % tenant_id)
    shards = located.get("shards") or []
    zero = pitr_shard_zero(shards, tenant_id)
    response = pitr_pageserver_request(args.namespace, "GET", zero, ancestor,
                                      query={"timestamp": timestamp, "with_lease": "false"})
    lsn = pitr_select_lsn(response)
    plan = {"branch": args.branch_name, "parent": args.parent, "requested_time": args.time,
            "timestamp": timestamp, "tenant_id": tenant_id,
            "ancestor_timeline_id": ancestor, "ancestor_start_lsn": lsn,
            "namespace": args.namespace, "dry_run": args.dry_run}
    if args.dry_run:
        print_json(plan)
        return
    for shard in shards:
        pitr_check_lease(pitr_pageserver_request(args.namespace, "POST", shard, ancestor, body={"lsn": lsn}))
    print("PITR: %s -> %s at %s" % (args.time, timestamp, lsn), file=sys.stderr)
    # Parse the existing create command so all compute/pool defaults stay identical.
    argv = ["branch", "create", args.branch_name, "--parent", args.parent,
            "--tenant-id", tenant_id, "--ancestor-timeline-id", ancestor, "--lsn", lsn]
    for name in ("namespace", "data_root", "deploy_root", "storage_image", "compute_image",
                 "og_version", "node_name", "operator_url", "timeout"):
        argv += ["--" + name.replace("_", "-"), str(getattr(args, name))]
    if args.from_pool:
        argv += ["--from-pool", args.from_pool]
    argv += ["--pool-fallback", args.pool_fallback]
    if args.timeline_id:
        argv += ["--timeline-id", args.timeline_id]
    create = build_parser().parse_args(argv)
    try:
        cmd_branch(create)
    except (Exception, SystemExit):
        print("PITR create did not finish. Inspect branch '%s' with branch status before retrying; "
              "a timeline or compute may already exist." % args.branch_name, file=sys.stderr)
        raise


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(description="Kubernetes local helper for openGauss branching.")
    sub = parser.add_subparsers(dest="resource", required=True)

    p = sub.add_parser("pitr", help="create a recovery branch at a timestamp")
    p.add_argument("branch_name", type=validate_dns_label)
    p.add_argument("--parent", default="compute-main")
    p.add_argument("--time")
    p.add_argument("--describe", action="store_true")
    p.add_argument("--source-timeline-id")
    p.add_argument("--timeline-id")
    p.add_argument("--pool-fallback", choices=["none", "deployment"], default="none")
    p.add_argument("--tenant-id")
    p.add_argument("--ancestor-timeline-id")
    p.add_argument("--from-pool")
    add_common(p)
    p.set_defaults(func=cmd_pitr)


    safekeeper = sub.add_parser("safekeeper")
    safekeeper_sub = safekeeper.add_subparsers(dest="action", required=True)
    for action in ["list"]:
        p = safekeeper_sub.add_parser(action)
        add_common(p)
        p.set_defaults(func=cmd_safekeeper)
    for action in ["status", "add", "remove"]:
        p = safekeeper_sub.add_parser(action)
        p.add_argument("id", type=int)
        if action == "remove":
            p.add_argument("--force", action="store_true")
        add_common(p)
        p.set_defaults(func=cmd_safekeeper)

    pageserver = sub.add_parser("pageserver")
    pageserver_sub = pageserver.add_subparsers(dest="action", required=True)
    p = pageserver_sub.add_parser("list")
    add_common(p)
    p.set_defaults(func=cmd_pageserver)
    p = pageserver_sub.add_parser("status")
    p.add_argument("ref")
    add_common(p)
    p.set_defaults(func=cmd_pageserver)
    p = pageserver_sub.add_parser("add")
    p.add_argument("ordinal", type=int)
    p.add_argument("--node-id", type=int)
    add_common(p)
    p.set_defaults(func=cmd_pageserver)
    p = pageserver_sub.add_parser("remove")
    p.add_argument("ref")
    p.add_argument("--node-id", type=int)
    p.add_argument("--force", action="store_true")
    add_common(p)
    p.set_defaults(func=cmd_pageserver)

    branch = sub.add_parser("branch")
    branch_sub = branch.add_subparsers(dest="action", required=True)
    p = branch_sub.add_parser("oggit")
    oggit_sub = p.add_subparsers(dest="oggit_action", required=True)
    for action in ["enable", "disable"]:
        oggit = oggit_sub.add_parser(action)
        oggit.add_argument("branch_name", type=validate_dns_label)
        if action == "enable":
            oggit.add_argument("--oggit-database", default="postgres")
        else:
            oggit.set_defaults(oggit_database="postgres")
        add_common(oggit)
        oggit.set_defaults(func=cmd_branch)
    p = branch_sub.add_parser("list")
    add_common(p)
    p.set_defaults(func=cmd_branch)
    for action in ["status", "oggit-status"]:
        p = branch_sub.add_parser(action)
        p.add_argument("branch_name", type=validate_dns_label)
        add_common(p)
        p.set_defaults(func=cmd_branch)
    p = branch_sub.add_parser("connect")
    p.add_argument("branch_name", type=validate_dns_label)
    p.add_argument("--database", default="postgres")
    p.add_argument("--db-user", default="cloud_admin")
    add_common(p)
    p.set_defaults(func=cmd_branch)
    p = branch_sub.add_parser("create")
    p.add_argument("branch_name", type=validate_dns_label)
    p.add_argument("--parent", default="compute-main", help="parent compute Deployment to branch from")
    p.add_argument("--main-deployment", dest="parent", help=argparse.SUPPRESS)
    p.add_argument("--tenant-id")
    p.add_argument("--ancestor-timeline-id")
    p.add_argument("--ancestor-start-lsn")
    p.add_argument("--lsn", dest="ancestor_start_lsn", help="alias for --ancestor-start-lsn")
    p.add_argument("--timeline-id")
    p.add_argument("--from-pool")
    p.add_argument("--pool-fallback", choices=["none", "deployment"], default="none")
    p.add_argument("--pg-version", type=int, choices=[16, 17], default=17)
    p.add_argument("--cpu", default="2")
    p.add_argument("--memory", default="4Gi")
    p.add_argument("--cpu-limit", default="8")
    p.add_argument("--memory-limit", default="16Gi")
    p.add_argument("--enable-oggit", dest="enable_oggit", action="store_true", default=True,
                   help="create the branch with OGGIT enabled (default; pass --disable-oggit to opt out)")
    p.add_argument("--disable-oggit", dest="enable_oggit", action="store_false",
                   help="create the branch without OGGIT")
    p.add_argument("--oggit-database", default="postgres")
    p.add_argument("--database", default="postgres")
    add_common(p)
    p.set_defaults(func=cmd_branch)
    p = branch_sub.add_parser("delete")
    p.add_argument("branch_name", type=validate_dns_label)
    p.add_argument("--delete-timeline", action="store_true")
    add_common(p)
    p.set_defaults(func=cmd_branch)

    for action in ["diff", "merge"]:
        p = branch_sub.add_parser(action)
        p.add_argument("--source", required=True, type=validate_dns_label, help="source branch to merge or diff")
        p.add_argument("--target", default="main")
        p.add_argument("--database", default="postgres")
        p.add_argument("--user", default="branch_merge")
        p.add_argument("--password")
        p.add_argument("--secret-name", default=DEFAULT_SECRET)
        p.add_argument("--source-schema", default="public")
        p.add_argument("--target-schema", default="public")
        p.add_argument("--keep-fdw", action="store_true")
        if action == "merge":
            p.add_argument("--strategy", default="fail", choices=["fail", "ours", "theirs", "manual"])
        add_common(p)
        p.set_defaults(func=cmd_branch)

    for action in ["merge-status", "conflicts", "continue", "abort"]:
        p = branch_sub.add_parser(action)
        p.add_argument("--merge-id", required=True)
        p.add_argument("--target", default="main")
        p.add_argument("--database", default="postgres")
        p.add_argument("--user", default="branch_merge")
        p.add_argument("--password")
        p.add_argument("--secret-name", default=DEFAULT_SECRET)
        add_common(p)
        p.set_defaults(func=cmd_branch)

    p = branch_sub.add_parser("resolve")
    p.add_argument("--merge-id", required=True)
    p.add_argument("--conflict-id", required=True, type=int)
    p.add_argument("--target", default="main")
    p.add_argument("--database", default="postgres")
    p.add_argument("--user", default="branch_merge")
    p.add_argument("--password")
    p.add_argument("--secret-name", default=DEFAULT_SECRET)
    p.add_argument("--resolution", default="theirs", choices=["ours", "theirs", "skip"])
    p.add_argument("--custom-sql")
    add_common(p)
    p.set_defaults(func=cmd_branch)

    return parser


def coordinator_request(args, argv):
    """Send mutations through the same authenticated client as the fast entry point."""
    url = config_value("COORDINATOR_URL", "K8S_LOCAL_COORDINATOR_URL", "").rstrip("/")
    if not url:
        return False
    configured_token = config_value("COORDINATOR_TOKEN_FILE", "K8S_LOCAL_COORDINATOR_TOKEN_FILE", "")
    if not configured_token:
        raise RuntimeError("COORDINATOR_TOKEN_FILE is required when COORDINATOR_URL is set")
    token_file = Path(configured_token).expanduser()
    if not token_file.is_absolute():
        token_file = ROOT_DIR / token_file
    result = coordinator_http_request({
        "url": url, "token_file": str(token_file),
        "argv": [*argv, "--namespace", args.namespace], "timeout": args.timeout,
    })
    print_json(result)
    return True


def main() -> int:
    args = build_parser().parse_args()
    coordinator_action = (args.func == cmd_branch and getattr(args, "action", None) in ("create", "delete")
                          and not args.dry_run
                          and (args.action == "delete" or getattr(args, "from_pool", None)))
    coordinator_action = coordinator_action or (args.func == cmd_pitr and bool(args.from_pool))
    if coordinator_action and coordinator_request(args, sys.argv[1:]):
        return 0
    args.func(args)
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except KeyboardInterrupt:
        raise SystemExit(130)
