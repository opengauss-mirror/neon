#!/usr/bin/env python3
"""Persistent, namespace-scoped branch coordinator. No subprocesses or SQLite.

The operator remains the allocation authority. Kubernetes ConfigMaps are the
durable operation journal; the watch cache is only a routing/index cache.
"""
from __future__ import annotations

import argparse
import concurrent.futures
import copy
import datetime
import hmac
import http.client
import json
import os
import queue
import re
import secrets
import threading
import time
import urllib.parse
from contextlib import contextmanager
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

import k8s_local as cli


class APIError(RuntimeError):
    def __init__(self, status, message, payload=None):
        super().__init__(message)
        self.status = status
        self.payload = payload


class HTTPPool:
    """Keep-alive connections, one checked-out connection per concurrent request."""
    def __init__(self):
        self.lock = threading.Lock()
        self.pools = {}

    def request(self, method, url, body=None, token=None, missing_ok=False, timeout=120):
        parsed = urllib.parse.urlsplit(url)
        if parsed.scheme != "http":
            raise RuntimeError("internal endpoints require explicitly configured HTTP URLs")
        key = (parsed.hostname, parsed.port or 80)
        with self.lock:
            pool = self.pools.setdefault(key, queue.LifoQueue())
        try:
            connection = pool.get_nowait()
        except queue.Empty:
            connection = http.client.HTTPConnection(*key, timeout=timeout)
        connection.timeout = timeout
        if connection.sock is not None:
            connection.sock.settimeout(timeout)
        headers = {"Accept": "application/json"}
        if token:
            headers["Authorization"] = "Bearer " + token
        data = None if body is None else json.dumps(body).encode()
        if data is not None:
            headers["Content-Type"] = "application/json"
        try:
            connection.request(method, parsed.path + ("?" + parsed.query if parsed.query else ""), data, headers)
            response = connection.getresponse()
            raw = response.read()
            status = response.status
            if response.will_close:
                connection.close()
            else:
                pool.put(connection)
        except BaseException:
            connection.close()
            # Never automatically retry a mutating HTTP request: its result may be unknown.
            raise
        if status == 404 and missing_ok:
            return None
        if not 200 <= status < 300:
            try:
                payload = json.loads(raw)
            except (ValueError, UnicodeDecodeError):
                payload = None
            raise APIError(status, "%s %s returned HTTP %s: %s" %
                           (method, parsed.path, status, raw.decode(errors="replace")), payload)
        return json.loads(raw) if raw else None


def is_pool_exhausted(error):
    payload = error.payload if isinstance(error, APIError) else None
    message = (payload.get("message") or payload.get("error")) if isinstance(payload, dict) else None
    return (isinstance(error, APIError) and error.status == 404
            and isinstance(message, str)
            and message.startswith("no WarmReady instance available in namespace "))


class WatchCache:
    """Watch-backed routing cache; relist after a disconnect or expired version."""
    def __init__(self, api, namespace):
        self.api, self.namespace = api, namespace
        self.lock = threading.RLock()
        self.items = {}
        self.ready = set()
        for kind in ("pod", "dbresourcepool"):
            threading.Thread(target=self.watch, args=(kind,), daemon=True).start()

    def watch(self, kind):
        path = self.api.path(kind, self.namespace, "").rstrip("/")
        while True:
            connection = None
            try:
                listing = self.api.request("GET", path)
                with self.lock:
                    self.items[kind] = {o["metadata"]["name"]: o for o in listing["items"]}
                    self.ready.add(kind)
                version = listing["metadata"]["resourceVersion"]
                connection = self.api.http.HTTPSConnection(
                    self.api.url.hostname, self.api.url.port or 443,
                    context=self.api.context, timeout=35)
                query = urllib.parse.urlencode({"watch": "true", "resourceVersion": version,
                                                "timeoutSeconds": 25, "allowWatchBookmarks": "true"})
                connection.request("GET", path + "?" + query, headers=self.api.request_headers())
                response = connection.getresponse()
                if response.status != 200:
                    raise RuntimeError("watch HTTP %s" % response.status)
                while True:
                    line = response.readline()
                    if not line:
                        break
                    event = json.loads(line)
                    if event["type"] == "ERROR":
                        raise RuntimeError("watch version expired")
                    if event["type"] == "BOOKMARK":
                        continue
                    obj = event["object"]
                    with self.lock:
                        if event["type"] == "DELETED":
                            self.items[kind].pop(obj["metadata"]["name"], None)
                        else:
                            self.items[kind][obj["metadata"]["name"]] = obj
            except Exception as exc:
                with self.lock:
                    self.ready.discard(kind)
                print("cache watch %s reconnecting: %s" % (kind, exc), flush=True)
                time.sleep(0.5)
            finally:
                if connection:
                    connection.close()

    def get(self, kind, name):
        with self.lock:
            if kind in self.ready:
                found = self.items.get(kind, {}).get(name)
                if found:
                    return copy.deepcopy(found)
        # A watch can lag a just-created resource. Cache misses always refresh.
        return self.api.request("GET", self.api.path(kind, self.namespace, name), missing_ok=True)


class Coordinator:
    def __init__(self, api, namespace, http=None, cache=None):
        self.api, self.namespace = api, namespace
        self.http = http or HTTPPool()
        self.cache = cache or WatchCache(api, namespace)
        self.compute_token = os.environ["COMPUTE_CTL_POOL_API_TOKEN"]
        self.operator_url = os.environ.get("OPERATOR_URL", cli.DEFAULT_OPERATOR_URL).rstrip("/")
        self.storage_url = os.environ.get("STORAGE_CONTROLLER_HTTP", "http://storage-controller:1234").rstrip("/")
        self.branch_locks = {}
        self.lock = threading.Lock()

    @contextmanager
    def exclusive(self, name):
        with self.lock:
            lock = self.branch_locks.setdefault(name, threading.Lock())
        if not lock.acquire(blocking=False):
            raise APIError(409, "another operation for this branch is running")
        try:
            yield
        finally:
            lock.release()

    def cm(self, name):
        obj = self.api.request("GET", self.api.path("configmap", self.namespace, name), missing_ok=True)
        return json.loads(obj["data"]["branch.json"]) if obj else None

    def save(self, name, data, *, create=False, labels=None):
        item = {"apiVersion": "v1", "kind": "ConfigMap",
                "metadata": {"name": name, "namespace": self.namespace, "labels": labels or {}},
                "data": {"branch.json": json.dumps(data, sort_keys=True)}}
        path = self.api.path("configmap", self.namespace, name)
        if create:
            # POST is an atomic branch-name reservation; never overwrite an existing pending operation.
            return self.api.request("POST", path.rsplit("/", 1)[0], item)
        return self.api.request("PATCH", path + "?fieldManager=branching-coordinator&force=true", item,
                                content_type="application/apply-patch+yaml")

    def remove(self, kind, name):
        return self.api.request("DELETE", self.api.path(kind, self.namespace, name),
                                {"apiVersion": "v1", "kind": "DeleteOptions"}, missing_ok=True)

    def context(self, url):
        result = self.http.request("GET", url + "/binding_context", token=self.compute_token)
        if result.get("status") != "running":
            raise APIError(409, "compute is not running; operation retained for recovery")
        active = result.get("active_operation") or {}
        if active.get("phase") not in (None, "completed"):
            raise APIError(409, "compute has an unresolved native operation; metadata retained for recovery")
        return result

    def pod_url(self, name, uid=None, pool=None):
        # Mutating paths validate the current Pod UID with the API, not a watch
        # event that may lag a deletion/recreation and IP reuse.
        obj = (self.api.request("GET", self.api.path("pod", self.namespace, name), missing_ok=True)
               if uid else self.cache.get("pod", name))
        if not obj or obj["metadata"].get("deletionTimestamp"):
            raise APIError(409, "allocated Pod is absent or terminating")
        if uid and obj["metadata"]["uid"] != uid:
            raise APIError(409, "allocated Pod UID changed; refusing to bind replacement")
        if pool and obj["metadata"].get("labels", {}).get("dbresourcepool") != pool:
            raise APIError(409, "operator allocated a Pod from a different resource pool; allocation retained for recovery")
        ip = obj.get("status", {}).get("podIP")
        if not ip:
            raise APIError(409, "allocated Pod has no IP")
        return "http://%s:3080" % ("[" + ip + "]" if ":" in ip else ip), obj["metadata"]["uid"]

    def parent(self, args):
        if args.tenant_id and args.ancestor_timeline_id:
            return args.tenant_id, args.ancestor_timeline_id
        metadata = self.cm(args.parent + "-metadata")
        if metadata and metadata.get("supply_mode") != "deployment":
            url, _ = self.pod_url(metadata["pod_name"], metadata.get("pod_uid"))
        else:
            # The main Service exposes compute_ctl's HTTP port. No SQL exec.
            url = "http://%s.%s.svc.cluster.local:3080" % (args.parent, self.namespace)
        ctx = self.context(url)
        warm = ctx["warm_settings"]
        return args.tenant_id or warm["neon.tenant_id"], args.ancestor_timeline_id or warm["neon.timeline_id"]

    def operator(self, method, path, body=None, missing_ok=False):
        # Projected SA tokens rotate; reread the short file, never cache/log its content.
        token = Path("/var/run/secrets/kubernetes.io/serviceaccount/token").read_text().strip()
        return self.http.request(method, self.operator_url + path + "?namespace=" + self.namespace,
                                 body, token=token, missing_ok=missing_ok)

    def allocate(self, args):
        payload = self.operator("POST", "/api/v1/instances", {
            "app_ref": args.branch_name, "pg_version": args.pg_version,
            "resources": {"cpu": args.cpu, "memory": args.memory,
                          "cpu_limit": args.cpu_limit, "memory_limit": args.memory_limit}})
        if not isinstance(payload, dict) or not all(payload.get(k) for k in ("endpoint", "pod_name", "pod_id")):
            raise RuntimeError("invalid allocation response; pending retained")
        return payload

    def storage_context(self, tenant, ancestor, explicit_lsn):
        locate = self.http.request("GET", self.storage_url + "/debug/v1/tenant/%s/locate" % tenant)
        shard = locate["shards"][0]
        pageserver = "host=%s port=%s" % (shard["listen_pg_addr"], shard["listen_pg_port"])
        lsn = explicit_lsn
        if not lsn:
            info = self.http.request("GET", self.storage_url + "/control/v1/tenant/%s/timeline/%s" % (tenant, ancestor))
            lsn = info.get("last_record_lsn")
            if not lsn:
                for shard in info.get("shards", []):
                    lsn = shard.get("last_record_lsn") or shard.get("remote_consistent_lsn")
                    if lsn:
                        break
        if not lsn:
            raise RuntimeError("parent has no LSN")
        return pageserver, lsn

    @staticmethod
    def pageserver_url(shard, timeline, suffix):
        host = shard["listen_http_addr"]
        if ":" in host and not host.startswith("["):
            host = "[" + host + "]"
        return "http://%s:%s/v1/tenant/%s/timeline/%s/%s" % (
            host, shard["listen_http_port"], shard["shard_id"], timeline, suffix)

    @staticmethod
    def main_creation_time(tenant, timeline):
        directory = os.environ.get("MAIN_TIMELINE_CREATION_DIR")
        if not directory:
            return None
        if not all(re.fullmatch(r"[a-f0-9]{32}", value) for value in (tenant, timeline)):
            raise APIError(409, "Invalid main timeline identity")
        path = Path(directory) / (tenant + "-" + timeline + ".json")
        try:
            record = json.loads(path.read_text())
        except FileNotFoundError:
            return None
        except (OSError, ValueError) as exc:
            raise APIError(409, "Unable to read main timeline creation record") from exc
        try:
            if (record["version"] != 1 or record["tenant_id"] != tenant
                    or record["timeline_id"] != timeline
                    or record["source"] != "compute.sh root timeline creation"):
                raise ValueError("identity mismatch")
            created = record["created_at"]
            timestamp = datetime.datetime.fromisoformat(created.replace("Z", "+00:00"))
            if timestamp.tzinfo is None or timestamp > datetime.datetime.now(datetime.timezone.utc):
                raise ValueError("invalid creation time")
        except (KeyError, TypeError, AttributeError, ValueError) as exc:
            raise APIError(409, "Invalid main timeline creation record") from exc
        return created

    def restore_info(self, args, tenant=None, timeline=None):
        if tenant is None:
            tenant, timeline = self.parent(args)
        metadata = self.cm(args.parent + "-metadata") or {}
        record = self.cm("timeline-" + timeline + "-creation") or {}
        created = (metadata.get("created_at") if metadata.get("timeline_id") == timeline else None) or record.get("created_at")
        if not created and args.parent in ("main", "compute-main"):
            created = self.main_creation_time(tenant, timeline)
        return {"name": args.parent, "id": timeline, "tenant_id": tenant,
                "created_at": created, "server_time": datetime.datetime.now(datetime.timezone.utc).isoformat()}

    def validate_restore_time(self, args, tenant, ancestor):
        info = self.restore_info(args, tenant, ancestor)
        target = datetime.datetime.fromisoformat(args._pitr_timestamp.replace("Z", "+00:00"))
        now = datetime.datetime.now(datetime.timezone.utc)
        if target > now:
            raise APIError(400, "Date is in the future")
        if getattr(args, "_pitr_source_id", None) and args._pitr_source_id != ancestor:
            raise APIError(409, "Source branch identity changed. Refresh and try again.")
        if not info["created_at"]:
            raise APIError(409, "Branch creation time is unavailable. Restore is disabled.")
        created = datetime.datetime.fromisoformat(info["created_at"].replace("Z", "+00:00"))
        if created.tzinfo is None:
            raise APIError(409, "Branch creation time is unavailable. Restore is disabled.")
        if target < created:
            raise APIError(400, "Date is before branch create")
        return info

    def pitr_context(self, args, tenant, ancestor, measured):
        self.validate_restore_time(args, tenant, ancestor)
        located = measured("pitr_locate", self.http.request, "GET",
                          self.storage_url + "/debug/v1/tenant/%s/locate" % tenant)
        shards = located.get("shards") or []
        zero = cli.pitr_shard_zero(shards, tenant)
        query = urllib.parse.urlencode({"timestamp": args._pitr_timestamp, "with_lease": "false"})
        response = measured("pitr_timestamp_lsn", self.http.request, "GET",
                            self.pageserver_url(zero, ancestor, "get_lsn_by_timestamp") + "?" + query)
        try:
            lsn = cli.pitr_select_lsn(response)
        except RuntimeError as exc:
            raise APIError(422, str(exc)) from exc
        plan = {"branch": args.branch_name, "parent": args.parent,
                "requested_time": args._pitr_requested_time, "timestamp": args._pitr_timestamp,
                "tenant_id": tenant, "ancestor_timeline_id": ancestor,
                "ancestor_start_lsn": lsn, "namespace": self.namespace, "dry_run": args.dry_run}
        if not args.dry_run:
            def grant(shard):
                lease = self.http.request("POST", self.pageserver_url(shard, ancestor, "lsn_lease"),
                                          {"lsn": lsn})
                cli.pitr_check_lease(lease)

            def protect_history():
                with concurrent.futures.ThreadPoolExecutor(max_workers=min(4, len(shards))) as executor:
                    futures = [executor.submit(grant, shard) for shard in shards]
                    for future in futures:
                        future.result()
            # Every shard must grant a valid lease before allocation or timeline creation.
            measured("pitr_lease", protect_history)
        pageserver = "host=%s port=%s" % (zero["listen_pg_addr"], zero["listen_pg_port"])
        return plan, (pageserver, lsn)


    def no_allocation(self, name):
        """A negative allocator response must also have no owned instance/service."""
        listing = self.api.request("GET", self.api.path("dbinstance", self.namespace, "") +
                                   "?labelSelector=" + urllib.parse.quote("app-ref=" + name, safe=""))
        if listing.get("items"):
            return False
        return all(self.api.request("GET", self.api.path(kind, self.namespace, name),
                                   missing_ok=True) is None for kind in ("svc", "deployment"))

    def apply_owned(self, item, state):
        kind = "deployment" if item["kind"] == "Deployment" else "svc"
        name = state["branch"]
        path = self.api.path(kind, self.namespace, name)
        existing = self.api.request("GET", path, missing_ok=True)
        if existing:
            labels = existing["metadata"].get("labels", {})
            if (labels.get("branching.openfuyao.cn/timeline-id") != state["timeline_id"]
                    or labels.get("branching.openfuyao.cn/operation-id") != state["operation_id"]):
                raise APIError(409, "resource identity conflict; refusing to overwrite " + name)
            return existing
        # POST, never force-apply: a concurrent name collision must fail safely.
        return self.api.request("POST", path.rsplit("/", 1)[0], item)

    def create_deployment(self, args, state, storage=None):
        name = state["branch"]
        if state.get("phase") == "deployment_creating_timeline" and not state.get("timeline_created"):
            raise APIError(409, "timeline creation outcome is unknown; pending retained for recovery")
        template = self.api.request("GET", self.api.path("deployment", self.namespace, "compute-main"))
        if storage is None:
            storage = self.storage_context(state["tenant_id"], state["ancestor_timeline_id"],
                                           state.get("ancestor_start_lsn") or args.ancestor_start_lsn)
        pageserver, lsn = storage
        state.update(supply_mode="deployment", ancestor_start_lsn=lsn)
        if not state.get("timeline_created"):
            url = self.storage_url + "/v1/tenant/%s/timeline/%s" % (state["tenant_id"], state["timeline_id"])
            if self.http.request("GET", url, missing_ok=True) is not None:
                raise APIError(409, "timeline already exists; refusing to adopt it")
            state["phase"] = "deployment_creating_timeline"
            self.save(name + "-pending", state)
            response = self.http.request("POST", self.storage_url + "/v1/tenant/%s/timeline" % state["tenant_id"],
                {"new_timeline_id": state["timeline_id"], "ancestor_timeline_id": state["ancestor_timeline_id"],
                 "ancestor_start_lsn": lsn})
            if isinstance(response, dict) and response.get("msg"):
                raise RuntimeError("timeline creation failed; pending retained for recovery")
            state.update(timeline_created=True, phase="deployment_starting")
            self.save(name + "-pending", state)
        items = cli.branch_items_from_template(args, template, state["tenant_id"], state["timeline_id"],
                                              state["ancestor_timeline_id"], lsn)
        for item in items:
            item["metadata"].setdefault("labels", {}).update({
                "branching.openfuyao.cn/timeline-id": state["timeline_id"],
                "branching.openfuyao.cn/tenant-id": state["tenant_id"],
                "branching.openfuyao.cn/operation-id": state["operation_id"],
                "branching.openfuyao.cn/compute-mode": "deployment"})
            self.apply_owned(item, state)
        deadline = time.monotonic() + args.timeout
        while time.monotonic() < deadline:
            deploy = self.api.request("GET", self.api.path("deployment", self.namespace, name))
            status = deploy.get("status", {})
            if (status.get("observedGeneration", 0) >= deploy["metadata"].get("generation", 1)
                    and status.get("availableReplicas", 0) >= 1):
                try:
                    context = self.context("http://%s.%s.svc.cluster.local:3080" % (name, self.namespace))
                    settings = context["warm_settings"]
                    if (settings["neon.tenant_id"], settings["neon.timeline_id"]) == (state["tenant_id"], state["timeline_id"]):
                        break
                except (APIError, OSError, http.client.HTTPException):
                    pass
            time.sleep(1)
        else:
            raise APIError(504, "dedicated compute did not become ready; creation record retained for retry")
        state["phase"] = "ready"
        self.save(name + "-metadata", state, labels={
            **cli.common_labels(name, "dedicated-branch-metadata"),
            "branching.openfuyao.cn/tenant-id": state["tenant_id"],
            "branching.openfuyao.cn/timeline-id": state["timeline_id"],
            "branching.openfuyao.cn/ancestor-timeline-id": state["ancestor_timeline_id"],
            "branching.openfuyao.cn/compute-mode": "deployment"})
        self.remove("configmap", name + "-pending")
        return {"branch": name, "tenant_id": state["tenant_id"], "timeline_id": state["timeline_id"],
                "ancestor_timeline_id": state["ancestor_timeline_id"], "ancestor_start_lsn": lsn,
                "compute_mode": "deployment", "pool_fallback": True}

    def delete_deployment(self, args, state):
        name = args.branch_name
        if name in ("main", "default", "compute-main"):
            raise APIError(403, "default main cannot be deleted")
        if not state.get("timeline_created"):
            raise APIError(409, "timeline ownership is unconfirmed; pending retained")
        # Storage is the authority for child timelines, including children outside Studio.
        timelines = self.http.request("GET", self.storage_url + "/v1/tenant/%s/timeline" % state["tenant_id"])
        children = [t.get("timeline_id") for t in timelines
                    if t.get("ancestor_timeline_id") == state["timeline_id"]]
        if children:
            raise APIError(409, "delete child timelines first: " + ", ".join(children))
        state["phase"] = "deployment_deleting"
        self.save(name + "-pending", state)
        for kind, resource_name in (("deployment", name), ("svc", name), ("svc", name + "-np")):
            item = self.api.request("GET", self.api.path(kind, self.namespace, resource_name), missing_ok=True)
            if item:
                labels = item["metadata"].get("labels", {})
                if labels.get("branching.openfuyao.cn/timeline-id") != state["timeline_id"]:
                    raise APIError(409, "resource identity changed; refusing deletion")
                self.api.request("DELETE", self.api.path(kind, self.namespace, resource_name),
                    {"apiVersion": "v1", "kind": "DeleteOptions", "propagationPolicy": "Foreground",
                     "preconditions": {"uid": item["metadata"]["uid"]}}, missing_ok=True)
        deadline = time.monotonic() + args.timeout
        while self.api.request("GET", self.api.path("deployment", self.namespace, name), missing_ok=True):
            if time.monotonic() >= deadline:
                raise APIError(504, "waiting for dedicated compute removal; retry deletion")
            time.sleep(1)
        if args.delete_timeline:
            self.http.request("DELETE", self.storage_url + "/v1/tenant/%s/timeline/%s" %
                              (state["tenant_id"], state["timeline_id"]), missing_ok=True)
        self.remove("configmap", name + "-metadata")
        self.remove("configmap", name + "-pending")
        return {"branch": name, "compute_mode": "deployment", "timeline_deleted": bool(args.delete_timeline)}


    def create(self, args):
        if getattr(args, "_pitr_timestamp", None):
            with self.exclusive(args.parent):
                return self._create_locked(args)
        return self._create_locked(args)

    def _create_locked(self, args):
        name = args.branch_name
        with self.exclusive(name):
            pending = self.cm(name + "-pending")
            if pending and pending.get("supply_mode") == "deployment":
                if args.timeline_id != pending.get("timeline_id"):
                    raise APIError(409, "dedicated creation is already reserved; retry with its original timeline ID")
                return self.create_deployment(args, pending)
            return self._create(args)

    def _create(self, args):
        name = args.branch_name
        start = time.monotonic()
        timings = []

        def measured(label, function, *a, **kw):
            before = time.monotonic()
            try:
                return function(*a, **kw)
            finally:
                timings.append({"name": label, "start_ms": (before-start)*1000,
                                "ms": (time.monotonic()-before)*1000})

        def preflight():
            if (self.cm(name + "-metadata") or self.cm(name + "-pending") or
                    any(self.api.request("GET", self.api.path(kind, self.namespace, name), missing_ok=True)
                        for kind in ("deployment", "svc"))):
                raise APIError(409, "branch or pending operation already exists; inspect it or delete it before retrying")
            if getattr(args, "_pitr_timestamp", None):
                for kind in ("deployment", "svc"):
                    if self.api.request("GET", self.api.path(kind, self.namespace, name), missing_ok=True):
                        raise APIError(409, "recovery branch resource already exists")
            if not self.cache.get("dbresourcepool", args.from_pool):
                raise APIError(404, "resource pool does not exist")
        measured("preflight", preflight)
        tenant, ancestor = measured("parent_identity", self.parent, args)
        pitr_plan, resolved_storage = {}, None
        if getattr(args, "_pitr_timestamp", None):
            pitr_plan, resolved_storage = self.pitr_context(args, tenant, ancestor, measured)
            if args.dry_run:
                return dict(pitr_plan, timings=timings, coordinator_total_ms=(time.monotonic()-start)*1000)
            args.ancestor_start_lsn = pitr_plan["ancestor_start_lsn"]
        timeline = args.timeline_id or secrets.token_hex(16)
        operation = secrets.token_hex(16)
        state = {"branch": name, "pool": args.from_pool, "operation_id": operation,
                 "phase": "allocating", "tenant_id": tenant, "timeline_id": timeline,
                 "ancestor_timeline_id": ancestor, "allocation": None,
                 "oggit_enabled": args.enable_oggit, "oggit_database": args.oggit_database,
                 "created_at": datetime.datetime.now(datetime.timezone.utc).isoformat()}
        if pitr_plan:
            state["pitr"] = pitr_plan
        measured("reserve_pending", self.save, name + "-pending", state, create=True)
        try:
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                allocation_future = executor.submit(measured, "allocate", self.allocate, args)
                storage_future = (executor.submit(lambda: resolved_storage) if resolved_storage else
                                  executor.submit(measured, "storage_context", self.storage_context,
                                                  tenant, ancestor, args.ancestor_start_lsn))
                try:
                    allocation = allocation_future.result()
                except APIError as error:
                    if not is_pool_exhausted(error):
                        raise
                    if not self.no_allocation(name):
                        raise APIError(409, "allocation ownership is unresolved; pending retained")
                    if getattr(args, "pool_fallback", "none") != "deployment":
                        self.remove("configmap", name + "-pending")
                        raise APIError(409, "resource pool has no idle instance", {"code": "POOL_EXHAUSTED"})
                    # One reservation and timeline ID are retained across the supply-mode transition.
                    state.update(supply_mode="deployment", phase="deployment_reserved")
                    self.save(name + "-pending", state)
                    return self.create_deployment(args, state, storage_future.result())
                state["allocation"] = allocation
                url, uid = measured("pod_validate", self.pod_url,
                                    allocation["pod_name"], allocation["pod_id"], args.from_pool)
                context = measured("compute_context", self.context, url)
                pageserver, lsn = storage_future.result()
            warm = context["warm_settings"]
            state.update(warm_settings=warm, pod_uid=uid, ancestor_start_lsn=lsn,
                         pageserver_connstring=pageserver, phase="creating_timeline",
                         expected_generation=context["generation"])
            # All recovery identities precede the first timeline mutation.
            measured("pending_prepared", self.save, name + "-pending", state)
            response = measured("create_timeline", self.http.request, "POST",
                self.storage_url + "/v1/tenant/%s/timeline" % tenant,
                {"new_timeline_id": timeline, "ancestor_timeline_id": ancestor, "ancestor_start_lsn": lsn})
            if isinstance(response, dict) and response.get("msg"):
                raise RuntimeError("timeline creation failed: %s" % response)
            sk, sk_http = cli.safekeeper_routes_from_timeline_response(response)
            body = {"operation_id": operation, "expected_generation": context["generation"],
                    "tenant_id": tenant, "timeline_id": timeline,
                    "ancestor_timeline_id": ancestor, "ancestor_start_lsn": lsn,
                    "pageserver_connstring": pageserver, "safekeeper_connstrings": sk,
                    "safekeeper_http_urls": sk_http, "oggit_enabled": args.enable_oggit,
                    "oggit_database": args.oggit_database}
            state.update(phase="rebinding", rebind_request=body, timeline_created=True)
            # Persist the exact idempotent request before it can take effect.
            measured("pending_rebind", self.save, name + "-pending", state)
            def label_service():
                labels = cli.common_labels(name, "pool-branch-compute")
                labels.update({"branching.openfuyao.cn/tenant-id": tenant,
                               "branching.openfuyao.cn/timeline-id": timeline})
                self.api.request("PATCH", self.api.path("svc", self.namespace, name),
                                 {"metadata": {"labels": labels}}, content_type="application/merge-patch+json")
            # These labels identify the allocated Service; they do not advertise
            # readiness. Await both mutations before committing ready metadata.
            with concurrent.futures.ThreadPoolExecutor(max_workers=2) as executor:
                labels_future = executor.submit(measured, "label_service", label_service)
                rebound_future = executor.submit(measured, "rebind", self.http.request,
                    "POST", url + "/pool_rebind", body, token=self.compute_token, timeout=args.timeout)
                rebound = rebound_future.result()
                labels_future.result()
            status = rebound.get("status", {})
            if (status.get("status") != "running" or status.get("tenant") != tenant
                    or status.get("timeline") != timeline):
                raise RuntimeError("compute returned an unexpected branch identity")
            args.mode = "restart-only"
            metadata = cli.pool_branch_metadata_item(args, tenant, timeline, ancestor, lsn, allocation, warm)
            data = json.loads(metadata["data"]["branch.json"])
            data.update(created_at=state["created_at"], pod_uid=uid, operation_id=operation, generation=rebound.get("generation"),
                        timeline_created=True, phase="ready")
            if pitr_plan:
                data["pitr"] = pitr_plan
            measured("commit_metadata", self.save, name + "-metadata", data, labels=metadata["metadata"]["labels"])

            measured("delete_pending", self.remove, "configmap", name + "-pending")
            return {**pitr_plan, "branch": name, "tenant_id": tenant, "timeline_id": timeline,
                    "ancestor_timeline_id": ancestor, "ancestor_start_lsn": lsn,
                    "allocation": allocation, "oggit_enabled": args.enable_oggit,
                    "rebind": rebound, "timings": timings, "coordinator_total_ms": (time.monotonic()-start)*1000}
        except BaseException:
            # Persisted stage + exact native journal let explicit delete recover.
            # A timeout is not proof that a mutation failed. Never release here.
            raise

    def release_instance(self, name, state, allocation):
        """Resolve the owned DBInstance, including a partially completed return."""
        reference = state.get("release_instance")
        if reference:
            instance = self.api.request("GET", self.api.path("dbinstance", self.namespace, reference["name"]))
            if instance["metadata"]["uid"] != reference["uid"]:
                raise APIError(409, "DBInstance UID changed during return; pending retained")
        else:
            path = self.api.path("dbinstance", self.namespace, "").rstrip("/")
            rows = self.api.request("GET", path + "?labelSelector=" +
                                    urllib.parse.quote("app-ref=" + name, safe=""))["items"]
            if len(rows) != 1:
                raise APIError(409, "cannot identify exactly one owned DBInstance; pending retained")
            instance = rows[0]
        owner = instance["metadata"].get("labels", {}).get("app-ref")
        if owner != name and not (reference and owner is None):
            raise APIError(409, "DBInstance has a different allocation owner; refusing return")
        status = instance.get("status", {})
        if (status.get("podName") != allocation["pod_name"]
                or status.get("podUID") != allocation["pod_id"]
                or instance["metadata"].get("labels", {}).get("dbresourcepool") != state["pool"]):
            raise APIError(409, "DBInstance no longer refers to the allocated pool Pod")
        return instance

    def clear_app_ref(self, kind, obj, name):
        meta = obj["metadata"]
        owner = meta.get("labels", {}).get("app-ref")
        if owner not in (None, name):
            raise APIError(409, kind + " belongs to a different branch")
        if owner is None:
            return obj
        return self.api.request("PATCH", self.api.path(kind, self.namespace, meta["name"]),
            {"metadata": {"uid": meta["uid"], "resourceVersion": meta["resourceVersion"],
                          "labels": {"app-ref": None}}}, content_type="application/merge-patch+json")

    def return_warm_instance(self, name, state, allocation, instance):
        """Publish WarmReady last, after obsolete routing/ownership is removed.

        The operator DELETE endpoint destroys the Pod; warm return deliberately
        updates Kubernetes resources instead, matching the original CLI protocol.
        """
        recovery = dict(state, phase="returning_pool", allocation=allocation,
                        release_instance={"name": instance["metadata"]["name"],
                                          "uid": instance["metadata"]["uid"]})
        self.save(name + "-pending", recovery)
        service = self.api.request("GET", self.api.path("svc", self.namespace, name), missing_ok=True)
        if service:
            meta = service["metadata"]
            self.api.request("DELETE", self.api.path("svc", self.namespace, name),
                {"apiVersion": "v1", "kind": "DeleteOptions", "preconditions": {
                    "uid": meta["uid"], "resourceVersion": meta["resourceVersion"]}}, missing_ok=True)
        for attempt in range(5):
            try:
                # The controller may reconcile between label and status patches.
                # A conflict is safe to retry only after rechecking all ownership.
                pod = self.api.request("GET", self.api.path("pod", self.namespace, allocation["pod_name"]))
                if (pod["metadata"]["uid"] != allocation["pod_id"]
                        or pod["metadata"].get("labels", {}).get("dbresourcepool") != state["pool"]):
                    raise APIError(409, "Pod was replaced before warm return")
                if pod["metadata"].get("labels", {}).get("app-ref") not in (None, name):
                    raise APIError(409, "Pod was allocated to another branch during return")
                instance = self.release_instance(name, recovery, allocation)
                meta = instance["metadata"]
                status = instance.get("status", {})
                if (meta.get("labels", {}).get("app-ref") is None
                        and pod["metadata"].get("labels", {}).get("app-ref") is None
                        and status.get("phase") == "WarmReady"
                        and not status.get("connectionString") and not status.get("allocatedAt")):
                    return meta["name"]
                self.clear_app_ref("pod", pod, name)
                self.clear_app_ref("dbinstance", instance, name)
                # Read again: the label removal itself can trigger reconciliation.
                instance = self.release_instance(name, recovery, allocation)
                meta = instance["metadata"]
                self.api.request("PATCH", self.api.path("dbinstance", self.namespace, meta["name"]) + "/status",
                    {"metadata": {"uid": meta["uid"], "resourceVersion": meta["resourceVersion"]},
                     "status": {"phase": "WarmReady", "connectionString": "", "allocatedAt": None}},
                    content_type="application/merge-patch+json")
                return meta["name"]
            except cli.KubernetesAPIError as exc:
                if exc.status != 409 or attempt == 4:
                    raise
                time.sleep(0.01 * (attempt + 1))

    def delete(self, args):
        name = args.branch_name
        if name in ("main", "default", "compute-main"):
            raise APIError(403, "default main cannot be deleted")
        with self.exclusive(name):
            metadata = self.cm(name + "-metadata")
            pending = self.cm(name + "-pending")
            state = pending if pending and pending.get("phase") in ("releasing", "returning_pool") else metadata or pending
            if not state:
                deploy = self.api.request("GET", self.api.path("deployment", self.namespace, name), missing_ok=True)
                if deploy:
                    labels = deploy["metadata"].get("labels", {})
                    if labels.get("app.kubernetes.io/component") != "branch-compute":
                        raise APIError(409, "deployment is not a managed branch")
                    state = {"branch": name, "tenant_id": labels.get("branching.openfuyao.cn/tenant-id"),
                             "timeline_id": labels.get("branching.openfuyao.cn/timeline-id"),
                             "supply_mode": "deployment", "timeline_created": True}
                else:
                    raise APIError(404, "no coordinator metadata or pending operation for this branch")
            if state.get("supply_mode") == "deployment":
                return self.delete_deployment(args, state)
            if args.delete_timeline and state.get("phase") != "allocating" and not state.get("timeline_created"):
                raise APIError(409, "timeline creation was not confirmed; refusing to delete a possibly pre-existing timeline")
            allocation = state.get("allocation") or ({k: state[k] for k in ("pod_name", "pod_id", "endpoint")}
                                                      if state.get("pod_name") else None)
            if not allocation or not state.get("warm_settings"):
                # An ambiguous allocation has no trustworthy warm restore record.
                # Keep it for explicit recovery; operator DELETE destroys its Pod.
                raise APIError(409, "allocation recovery is incomplete; pending retained for inspection")
            instance = self.release_instance(name, state, allocation)
            url, _ = self.pod_url(allocation["pod_name"], state.get("pod_uid"), state.get("pool"))
            context = self.context(url)
            warm = state["warm_settings"]
            current = context["warm_settings"]
            if (current["neon.tenant_id"], current["neon.timeline_id"]) not in (
                    (warm["neon.tenant_id"], warm["neon.timeline_id"]),
                    (state["tenant_id"], state["timeline_id"])):
                raise APIError(409, "compute belongs to a different branch; refusing release")
            if (current["neon.tenant_id"], current["neon.timeline_id"]) != (warm["neon.tenant_id"], warm["neon.timeline_id"]):
                body = state.get("release_request") or {"operation_id": state["operation_id"] + "-release",
                        "expected_generation": context["generation"],
                        "tenant_id": warm["neon.tenant_id"], "timeline_id": warm["neon.timeline_id"],
                        "ancestor_timeline_id": warm["neon.timeline_id"], "ancestor_start_lsn": "0/0",
                        "pageserver_connstring": warm["neon.pageserver_connstring"],
                        "safekeeper_connstrings": warm["neon.safekeepers"],
                        "safekeeper_http_urls": cli.DEFAULT_SAFEKEEPER_HTTP_URLS,
                        "oggit_enabled": warm.get("neon.oggit_enabled", "off").lower() == "on",
                        "oggit_database": state.get("oggit_database", "postgres")}
                recovery = dict(state, phase="releasing", allocation=allocation, release_request=body)
                self.save(name + "-pending", recovery)
                result = self.http.request("POST", url + "/pool_rebind", body, token=self.compute_token,
                                           timeout=args.timeout)
                restored = result.get("status", {})
                if (restored.get("status") != "running" or restored.get("tenant") != warm["neon.tenant_id"]
                        or restored.get("timeline") != warm["neon.timeline_id"]):
                    raise RuntimeError("warm restore did not confirm the expected identity")
            if args.delete_timeline:
                self.http.request("DELETE", self.storage_url + "/v1/tenant/%s/timeline/%s" %
                                  (state["tenant_id"], state["timeline_id"]), missing_ok=True)
            instance_name = self.return_warm_instance(name, state, allocation, instance)
            self.remove("configmap", name + "-metadata")
            self.remove("configmap", name + "-pending")
            return {"branch": name, "released_to_pool": True, "timeline_deleted": bool(args.delete_timeline),
                    "dbinstance": instance_name, "pod": allocation["pod_name"], "pod_uid": allocation["pod_id"]}


def parse_request(payload, namespace):
    if not isinstance(payload, dict) or not isinstance(payload.get("argv"), list):
        raise APIError(400, "expected argv array")
    argv = payload["argv"]
    if not all(isinstance(x, str) for x in argv) or len(argv) > 100:
        raise APIError(400, "invalid argv")
    try:
        args = cli.build_parser().parse_args(argv)
    except SystemExit:
        raise APIError(400, "invalid branch command")
    is_pitr = args.func == cli.cmd_pitr
    is_branch = args.func == cli.cmd_branch and args.action in ("create", "delete")
    if (not (is_pitr or is_branch) or args.namespace != namespace
            or (args.dry_run and not is_pitr)):
        raise APIError(400, "only pool create/delete/PITR in the coordinator namespace are supported")
    if is_pitr:
        if args.describe:
            cli.validate_dns_label(args.parent)
            args.action = "describe"
            return args
        if not args.time:
            raise APIError(400, "Point in time is required")
        try:
            timestamp = cli.pitr_timestamp(args.time)
        except ValueError as exc:
            raise APIError(400, str(exc)) from exc
        source = args
        create_argv = ["branch", "create", source.branch_name, "--parent", source.parent]
        for key in ("from_pool", "pool_fallback", "timeline_id", "tenant_id", "ancestor_timeline_id", "namespace",
                    "data_root", "deploy_root", "storage_image", "compute_image",
                    "og_version", "node_name", "operator_url", "timeout"):
            value = getattr(source, key)
            if value is not None:
                create_argv += ["--" + key.replace("_", "-"), str(value)]
        args = cli.build_parser().parse_args(create_argv)
        args.dry_run = source.dry_run
        args._pitr_timestamp = timestamp
        args._pitr_requested_time = source.time
        args._pitr_source_id = source.source_timeline_id
    if args.action == "create" and not args.from_pool:
        raise APIError(400, "coordinator requires --from-pool")
    if args.action == "create":
        try:
            cli.validate_dns_label(args.parent)
            cli.validate_dns_label(args.from_pool)
        except argparse.ArgumentTypeError:
            raise APIError(400, "parent and pool must be DNS labels")
        for key in ("tenant_id", "ancestor_timeline_id", "timeline_id"):
            value = getattr(args, key, None)
            if value is not None and not re.fullmatch(r"[0-9a-fA-F]{32}", value):
                raise APIError(400, key + " must be a 32-character hexadecimal ID")
        if args.ancestor_start_lsn and not re.fullmatch(r"[0-9a-fA-F]+/[0-9a-fA-F]+", args.ancestor_start_lsn):
            raise APIError(400, "ancestor LSN must use hexadecimal X/Y notation")
    if args.timeout < 1 or args.timeout > 600:
        raise APIError(400, "timeout must be between 1 and 600 seconds")
    return args


def main():
    namespace = os.environ["POOL_NAMESPACE"]
    api = cli.KubernetesAPI()
    coordinator = Coordinator(api, namespace)
    api_token = os.environ["BRANCHING_COORDINATOR_TOKEN"]
    if len(api_token) < 24 or len(coordinator.compute_token) < 24:
        raise RuntimeError("coordinator API tokens must be at least 24 characters")

    class Handler(BaseHTTPRequestHandler):
        def reply(self, status, body):
            encoded = json.dumps(body).encode()
            self.send_response(status)
            self.send_header("Content-Type", "application/json")
            self.send_header("Content-Length", str(len(encoded)))
            self.end_headers()
            self.wfile.write(encoded)

        def do_GET(self):
            if self.path == "/ready":
                ready = coordinator.cache.ready == {"pod", "dbresourcepool"}
                self.reply(200 if ready else 503, {"ready": ready})
            else:
                self.reply(404, {"error": "unknown endpoint"})

        def do_POST(self):
            if not hmac.compare_digest(self.headers.get("Authorization", ""), "Bearer " + api_token):
                return self.reply(401, {"error": "invalid coordinator authorization"})
            if self.path != "/v1/branch":
                return self.reply(404, {"error": "unknown endpoint"})
            try:
                size = int(self.headers.get("Content-Length", "0"))
                if size <= 0 or size > 65536:
                    raise APIError(400, "invalid request length")
                args = parse_request(json.loads(self.rfile.read(size)), namespace)
                result = (coordinator.restore_info(args) if args.action == "describe" else
                          coordinator.create(args) if args.action == "create" else coordinator.delete(args))
                self.reply(200, result)
            except APIError as exc:
                self.reply(exc.status, {"error": str(exc)})
            except Exception as exc:
                self.reply(500, {"error": str(exc), "recovery": "inspect pending ConfigMap before retrying; operation outcome may be unknown"})

        def log_message(self, fmt, *args):
            print("coordinator: " + fmt % args, flush=True)

    ThreadingHTTPServer(("0.0.0.0", int(os.environ.get("COORDINATOR_PORT", "8787"))), Handler).serve_forever()


if __name__ == "__main__":
    main()

