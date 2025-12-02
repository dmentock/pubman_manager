# conftest.py
from __future__ import annotations
import base64
import copy
import json
import re
from collections import deque
from pathlib import Path
from types import SimpleNamespace
from typing import Any, Dict, Optional

import jwt
import pytest
import requests

from pubman_manager import PROJECT_ROOT
from pubman_manager.pubman_base import PubmanBase
from pubman_manager.pubman_creator import PubmanCreator

# ----------------------------
# pytest command-line option
# ----------------------------
def pytest_addoption(parser: pytest.Parser) -> None:
    parser.addoption(
        "--update",
        action="store_true",
        default=False,
        help="Record external API calls and refresh JSON caches.",
    )


@pytest.fixture
def test_resources_dir():
    """Return the base test/resources directory."""
    return PROJECT_ROOT / "test" / "resources"


@pytest.fixture
def mock_pubman(monkeypatch):
    """Force PubmanBase.search_publication_by_criteria to report no matches."""
    monkeypatch.setattr(
        PubmanBase,
        "search_publication_by_criteria",
        lambda self, match_criteria, size=100000: [],
    )


@pytest.fixture
def capture_pubman_creations(monkeypatch):
    """
    Capture calls to PubmanCreator.create_items so tests can inspect payloads.
    Returns a list of dicts describing each invocation.
    """
    captured: list[dict[str, Any]] = []

    def _capture(self, request_list, create_items=True, submit_items=False, overwrite=False):
        snapshot = copy.deepcopy(request_list)
        captured.append(
            {
                "requests": snapshot,
                "create_items": create_items,
                "submit_items": submit_items,
                "overwrite": overwrite,
            }
        )
        return []

    monkeypatch.setattr(PubmanCreator, "create_items", _capture)
    return captured

# ----------------------------
# Utilities
# ----------------------------
_SAFE_NAME = re.compile(r"[^A-Za-z0-9_.\-\[\]]+")

_SERVICE_PATTERNS: Dict[str, re.Pattern[str]] = {
    "crossref": re.compile(r"api\.crossref\.org"),
    "scopus":   re.compile(r"api\.elsevier\.com"),
    "pubman":   re.compile(r"pure\.mpg\.de"),
}

_PUBMAN_LOGIN_URL = "https://pure.mpg.de/rest/login"
_PUBMAN_CREATE_ITEM_URL = "https://pure.mpg.de/rest/items"


def _normalize(value: Any) -> Any:
    """Convert paths, sets, dicts, etc., to JSON-serializable values."""
    if isinstance(value, Path):
        return str(value)
    if isinstance(value, (bytes, bytearray)):
        return value.decode("utf-8", errors="replace")
    if isinstance(value, dict):
        return {str(k): _normalize(v)
                for k, v in sorted(value.items(), key=lambda kv: str(kv[0]))}
    if isinstance(value, (list, tuple)):
        return [_normalize(v) for v in value]
    if isinstance(value, (set, frozenset)):
        return sorted(_normalize(v) for v in value)
    return value


def _service_from_url(url: str) -> Optional[str]:
    """Identify which external service the URL targets."""
    for name, pattern in _SERVICE_PATTERNS.items():
        if pattern.search(url):
            return name
    return None


def _extract_body(kwargs: dict[str, Any]) -> Any:
    """Return the JSON body from kwargs if present."""
    if kwargs.get("json") is not None:
        return kwargs["json"]

    data = kwargs.get("data")
    if data is None:
        return None

    if isinstance(data, (bytes, bytearray)):
        data = data.decode("utf-8", errors="replace")

    if isinstance(data, str):
        try:
            return json.loads(data)
        except ValueError:
            return data

    return data


def _is_login_call(method: str, url: str) -> bool:
    return method.upper() == "POST" and url.rstrip("/") == _PUBMAN_LOGIN_URL


def _is_create_item_call(method: str, url: str) -> bool:
    return method.upper() == "POST" and url.rstrip("/") == _PUBMAN_CREATE_ITEM_URL


def _build_fake_token(user_id: str) -> str:
    """Create an unsigned JWT so jwt.decode(..., verify_signature=False) still works."""
    header = base64.urlsafe_b64encode(
        json.dumps({"alg": "none", "typ": "JWT"}).encode("utf-8")
    ).decode("utf-8").rstrip("=")
    payload = base64.urlsafe_b64encode(
        json.dumps({"id": user_id}).encode("utf-8")
    ).decode("utf-8").rstrip("=")
    return f"{header}.{payload}."


def _mask_login_entry(entry: dict[str, Any], real_token: Optional[str]) -> None:
    entry["data"] = "<masked>"
    entry["json"] = "<masked>"
    entry["body"] = "<masked>"

    response_headers = entry.setdefault("response_headers", {})
    if real_token:
        try:
            decoded = jwt.decode(real_token, options={"verify_signature": False})
            user_id = decoded.get("id", "masked-user")
        except Exception:
            user_id = "masked-user"
        response_headers["Token"] = _build_fake_token(str(user_id))
    if "Set-Cookie" in response_headers:
        response_headers["Set-Cookie"] = "<masked>"


def _record_create_payload(state: SimpleNamespace, payload: Any) -> None:
    if payload is None:
        return
    try:
        state.create_item_payloads.append(copy.deepcopy(payload))
    except Exception:
        state.create_item_payloads.append(payload)


def _make_response(entry: dict[str, Any]) -> requests.Response:
    """Re-create a Response object from recorded JSON data."""
    resp = requests.Response()
    resp.status_code = entry["status_code"]
    resp.url = entry["url"]
    resp.headers.update(entry.get("response_headers") or entry.get("headers") or {})
    if entry.get("is_json"):
        resp._content = json.dumps(entry["payload"]).encode("utf-8")
        resp.headers.setdefault("Content-Type", "application/json")
    else:
        payload = entry.get("payload", "")
        resp._content = payload.encode("utf-8")
        resp.headers.setdefault("Content-Type", "text/plain")
    resp.encoding = "utf-8"
    return resp


# ----------------------------
# Main fixture: record / replay
# ----------------------------
@pytest.fixture
def external_http_cache(monkeypatch, request, test_resources_dir):
    """
    Record / replay external HTTP calls (Crossref, Scopus, Pubman).

    • In --update mode:
        - Perform real requests
        - Record them to JSON under:
          test/resources/doi_cache/<file>/<test>/<service>.json

    • Without --update:
        - Replay from recorded JSON
        - Block any unexpected HTTP calls
    """

    update_mode = request.config.getoption("--update")

    # Unique test identifier
    path, _ = request.node.nodeid.split("::", 1)
    safe_file = _SAFE_NAME.sub("_", Path(path).stem)
    safe_test = _SAFE_NAME.sub("_", request.node.name)

    base_dir = test_resources_dir / "doi_cache" / safe_file / safe_test
    base_dir.mkdir(parents=True, exist_ok=True)

    service_files = {
        svc: base_dir / f"{svc}.json"
        for svc in _SERVICE_PATTERNS
    }

    # Tests can inspect collected payloads via external_http_cache.create_item_payloads.
    state = SimpleNamespace(create_item_payloads=[])

    # ---------------------------------------------------------
    # RECORD MODE
    # ---------------------------------------------------------
    if update_mode:
        recorded = {svc: [] for svc in service_files}
        real_request = requests.sessions.Session.request

        def _recording(self, method, url, **kwargs):
            service = _service_from_url(url)
            body = _extract_body(kwargs)
            is_login = service == "pubman" and _is_login_call(method, url)
            is_create_item = service == "pubman" and _is_create_item_call(method, url)

            if is_create_item:
                _record_create_payload(state, body)
                resp = requests.Response()
                resp.status_code = 201
                resp.url = url
                payload = {
                    "objectId": f"mock-pubman-item-{len(state.create_item_payloads)}",
                    "lastModificationDate": "1970-01-01T00:00:00Z",
                    "versionState": "PENDING",
                    "mocked": True,
                }
                resp._content = json.dumps(payload).encode("utf-8")
                resp.headers["Content-Type"] = "application/json"
                resp.headers["X-External-HTTP-Cache"] = "mocked-create-item"
                resp.encoding = "utf-8"
            else:
                resp = real_request(self, method, url, **kwargs)

            if service in recorded:
                entry = {
                    "method": method.upper(),
                    "url": url,
                    "params": _normalize(kwargs.get("params")),
                    "data":   _normalize(kwargs.get("data")),
                    "json":   _normalize(kwargs.get("json")),
                    "body":   _normalize(body),
                    "headers": _normalize(kwargs.get("headers")),
                    "status_code": resp.status_code,
                    "response_headers": _normalize(dict(resp.headers)),
                }
                if is_login:
                    real_token = resp.headers.get("Token")
                    _mask_login_entry(entry, real_token)
                if is_create_item:
                    entry.setdefault("response_headers", {})["X-External-HTTP-Cache"] = "mocked-create-item"
                try:
                    entry["payload"] = _normalize(resp.json())
                    entry["is_json"] = True
                except ValueError:
                    entry["payload"] = resp.text
                    entry["is_json"] = False

                recorded[service].append(entry)

            return resp

        monkeypatch.setattr(requests.sessions.Session, "request", _recording)

        yield state

        # persist recordings
        for svc, filepath in service_files.items():
            filepath.write_text(
                json.dumps({"calls": recorded[svc]}, ensure_ascii=False, indent=2),
                encoding="utf-8",
            )

        return

    caches = {}
    for svc, filepath in service_files.items():
        if not filepath.exists():
            raise RuntimeError(
                f"No cached {svc} interactions for test {request.node.nodeid}.\n"
                f"Run pytest --update to generate them."
            )
        data = json.loads(filepath.read_text(encoding="utf-8"))
        caches[svc] = deque(data.get("calls", []))

    def _replaying(self, method, url, **kwargs):
        service = _service_from_url(url)
        if service in caches:
            if not caches[service]:
                raise AssertionError(
                    f"Unexpected extra {service} call: {method} {url}"
                )

            entry = caches[service].popleft()

            if entry["method"] != method.upper() or entry["url"] != url:
                raise AssertionError(
                    f"Call mismatch for {service}.\n"
                    f"Expected: {entry['method']} {entry['url']}\n"
                    f"Got:      {method.upper()} {url}"
                )

            if service == "pubman" and _is_create_item_call(method, url):
                body = _extract_body(kwargs)
                _record_create_payload(state, body)

            return _make_response(entry)

        raise AssertionError(
            f"Outbound HTTP blocked in replay mode: {method} {url}\n"
            f"Run with --update to record this call."
        )

    monkeypatch.setattr(requests.sessions.Session, "request", _replaying)

    yield state

    # Detect missing expected calls
    for svc, remaining in caches.items():
        if remaining:
            raise AssertionError(
                f"{len(remaining)} recorded {svc} calls were NOT replayed."
            )
