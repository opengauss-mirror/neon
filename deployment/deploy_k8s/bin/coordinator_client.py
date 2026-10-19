"""Authenticated coordinator transport, shared by both CLI entry points."""
import os


def request(config):
    import http.client
    import json
    from urllib.parse import urlsplit

    if not config["token_file"]:
        raise RuntimeError("COORDINATOR_TOKEN_FILE is required when COORDINATOR_URL is set")
    with open(os.path.expanduser(config["token_file"]), encoding="utf-8") as file:
        token = file.read().strip()
    if not token or any(ord(char) < 33 or ord(char) > 126 for char in token):
        raise RuntimeError("invalid coordinator token file contents")
    parsed = urlsplit(config["url"])
    if (parsed.scheme not in ("http", "https") or not parsed.hostname
            or parsed.username is not None or parsed.password is not None
            or parsed.query or parsed.fragment):
        raise RuntimeError("COORDINATOR_URL must be an HTTP(S) URL without credentials, query or fragment")
    connection_type = http.client.HTTPSConnection if parsed.scheme == "https" else http.client.HTTPConnection
    # Invalid timeout values are still sent to the authoritative server parser;
    # use a valid network timeout so its normal validation error can be returned.
    timeout = config["timeout"]
    connection = connection_type(parsed.hostname, parsed.port,
                                 timeout=(timeout if 1 <= timeout <= 600 else 300) + 10)
    try:
        connection.request("POST", parsed.path.rstrip("/") + "/v1/branch",
                           body=json.dumps({"argv": config["argv"]}).encode("utf-8"),
                           headers={"Authorization": "Bearer " + token,
                                    "Content-Type": "application/json", "Accept": "application/json"})
        response = connection.getresponse()
        body = response.read()
        if not 200 <= response.status < 300:
            detail = body.decode("utf-8", errors="replace").replace(token, "<redacted>")
            raise RuntimeError("coordinator HTTP %s: %s" % (response.status, detail))
        return json.loads(body)
    finally:
        # No redirects or automatic retries: a failed mutation may have executed.
        connection.close()
