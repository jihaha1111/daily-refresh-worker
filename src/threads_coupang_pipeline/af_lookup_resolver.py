"""Resolve Coupang short links into private affiliate lookup evidence."""

from __future__ import annotations

import time
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, TextIO, Tuple
from string import ascii_letters, digits
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urlencode, urljoin, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener

from .lookup_prepare import read_csv_rows, write_csv_rows, write_json


AF_ID_PARAM_NAMES: Tuple[str, ...] = (
    "lptag",
    "af_id",
    "afid",
    "affiliate_id",
)

SENSITIVE_QUERY_PARAM_NAMES: Tuple[str, ...] = (
    *AF_ID_PARAM_NAMES,
    "subid",
    "sub_id",
    "subid1",
    "subid2",
    "subid3",
    "subid4",
    "subid5",
    "traceid",
    "trace_id",
    "tracking_id",
    "trackingid",
    "token",
    "access_token",
    "credential",
    "credentials",
    "secret",
)

AF_LINK_LOOKUP_RESULT_FIELDS = (
    "idx",
    "export_key",
    "item_pk",
    "link_index",
    "coupang_url",
    "normalized_coupang_url",
    "source",
    "user_id",
    "username",
    "threads_url",
    "taken_at",
    "is_reply",
    "item_has_coupang_link",
    "match_id",
    "match_role",
    "exception_type",
    "af_lookup_status",
    "http_status",
    "redirect_count",
    "af_id",
    "af_id_param",
    "normalized_destination_url",
    "resolved_host",
    "resolved_path",
    "error",
    "resolved_at",
)

AF_LINK_LOOKUP_UNIQUE_RESULT_FIELDS = (
    "lookup_url_id",
    "normalized_coupang_url",
    "evidence_count",
    "af_lookup_status",
    "http_status",
    "redirect_count",
    "af_id",
    "af_id_param",
    "normalized_destination_url",
    "resolved_host",
    "resolved_path",
    "error",
    "resolved_at",
)

AF_ACCOUNT_MAP_FIELDS = (
    "export_key",
    "af_id",
    "thread_user_id",
    "thread_username",
    "evidence_count",
    "unique_item_count",
    "matched_item_count",
    "exception_item_count",
    "first_seen_at",
    "last_seen_at",
    "first_item_pk",
    "first_coupang_url",
)

AF_LOOKUP_MANIFEST_NAME = "af-link-lookup-manifest"

_AF_ID_PARAM_NAMES_LOWER = {name.lower() for name in AF_ID_PARAM_NAMES}
_SENSITIVE_QUERY_PARAM_NAMES_LOWER = {name.lower() for name in SENSITIVE_QUERY_PARAM_NAMES}
_SHORT_LINK_SLUG_CHARS = set(ascii_letters + digits + "-_")


@dataclass(frozen=True)
class HttpHop:
    status: int
    url: str
    location: str
    error: str = ""


@dataclass(frozen=True)
class AffiliateEvidence:
    af_id: str
    af_id_param: str
    normalized_destination_url: str
    resolved_host: str
    resolved_path: str


@dataclass
class AfLookupResult:
    export_key: str
    total_rows: int
    resolved_rows: int
    failed_rows: int
    unique_af_ids: int
    unique_url_rows: int
    account_pair_rows: int
    unique_results: List[Dict[str, Any]]
    results: List[Dict[str, Any]]
    account_map: List[Dict[str, Any]]

    def summary(self) -> Dict[str, Any]:
        return {
            "export_key": self.export_key,
            "total_rows": self.total_rows,
            "resolved_rows": self.resolved_rows,
            "failed_rows": self.failed_rows,
            "unique_af_ids": self.unique_af_ids,
            "unique_url_rows": self.unique_url_rows,
            "account_pair_rows": self.account_pair_rows,
        }


class NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def ensure_url_scheme(url: str) -> str:
    value = "".join(
        ch for ch in (url or "") if (ch >= " " and ch != "\ufffc")
    ).strip()
    if not value:
        return ""
    parsed = urlsplit(value)
    if parsed.scheme:
        return value
    if value.startswith("//"):
        return "https:" + value
    return "https://" + value


def normalize_coupang_short_link(url: str) -> str:
    value = ensure_url_scheme(url)
    if not value:
        return ""

    parsed = urlsplit(value)
    if host_without_port(parsed.netloc) != "link.coupang.com":
        return value
    if not parsed.path.startswith("/a/"):
        return value

    slug = []
    for ch in parsed.path[len("/a/"):]:
        if ch not in _SHORT_LINK_SLUG_CHARS:
            break
        slug.append(ch)

    if not slug:
        return value

    return urlunsplit(
        (
            parsed.scheme or "https",
            parsed.netloc.lower(),
            "/a/" + "".join(slug),
            "",
            "",
        )
    )


def host_without_port(netloc: str) -> str:
    host = (netloc or "").split("@")[-1]
    if host.startswith("["):
        return host.lower()
    return host.split(":")[0].lower()


def is_coupang_host(netloc: str) -> bool:
    host = host_without_port(netloc)
    return host == "coupang.com" or host.endswith(".coupang.com")


def sanitize_destination_url(url: str) -> str:
    parsed = urlsplit(ensure_url_scheme(url))
    safe_pairs = [
        (name, value)
        for name, value in parse_qsl(parsed.query, keep_blank_values=True)
        if name.lower() not in _SENSITIVE_QUERY_PARAM_NAMES_LOWER
    ]
    return urlunsplit(
        (
            parsed.scheme or "https",
            parsed.netloc.lower(),
            parsed.path,
            urlencode(safe_pairs, doseq=True),
            "",
        )
    )


def find_affiliate_evidence(url: str) -> Optional[AffiliateEvidence]:
    parsed = urlsplit(ensure_url_scheme(url))
    if not is_coupang_host(parsed.netloc):
        return None

    for name, value in parse_qsl(parsed.query, keep_blank_values=True):
        if name.lower() not in _AF_ID_PARAM_NAMES_LOWER:
            continue
        af_id = (value or "").strip()
        if not af_id:
            continue

        return AffiliateEvidence(
            af_id=af_id,
            af_id_param=name,
            normalized_destination_url=sanitize_destination_url(url),
            resolved_host=host_without_port(parsed.netloc),
            resolved_path=parsed.path,
        )

    return None


def http_get_no_redirect(url: str, timeout_seconds: float, user_agent: str) -> HttpHop:
    opener = build_opener(NoRedirectHandler)
    request = Request(
        ensure_url_scheme(url),
        headers={
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        method="GET",
    )
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            return HttpHop(
                status=int(getattr(response, "status", 200)),
                url=response.geturl(),
                location=response.headers.get("Location", ""),
            )
    except HTTPError as exc:
        if 300 <= exc.code < 400:
            return HttpHop(
                status=exc.code,
                url=exc.geturl(),
                location=exc.headers.get("Location", ""),
            )
        return HttpHop(status=exc.code, url=exc.geturl(), location="", error=str(exc))
    except URLError as exc:
        reason = getattr(exc, "reason", str(exc))
        return HttpHop(status=0, url=url, location="", error=str(reason))
    except (OSError, TimeoutError, UnicodeError, ValueError) as exc:
        return HttpHop(status=0, url=url, location="", error=exc.__class__.__name__)


def resolve_coupang_affiliate(
    url: str,
    *,
    max_redirects: int = 5,
    timeout_seconds: float = 15.0,
    user_agent: str = "Mozilla/5.0",
    request_func: Optional[Callable[[str], HttpHop]] = None,
) -> Tuple[Optional[AffiliateEvidence], int, int, str]:
    current_url = normalize_coupang_short_link(url)
    if not current_url:
        return None, 0, 0, "missing_url"

    direct_evidence = find_affiliate_evidence(current_url)
    if direct_evidence is not None:
        return direct_evidence, 0, 0, ""

    fetch = request_func or (
        lambda next_url: http_get_no_redirect(next_url, timeout_seconds, user_agent)
    )
    last_status = 0
    last_error = ""

    for redirect_count in range(1, max_redirects + 1):
        hop = fetch(current_url)
        last_status = hop.status
        last_error = hop.error
        location = (hop.location or "").strip()
        if not location:
            return None, last_status, redirect_count - 1, last_error or "no_redirect_location"

        next_url = urljoin(current_url, location)
        evidence = find_affiliate_evidence(next_url)
        if evidence is not None:
            return evidence, last_status, redirect_count, ""

        current_url = next_url

    return None, last_status, max_redirects, "max_redirects_exceeded"


def resolve_row(
    row: Mapping[str, Any],
    *,
    max_redirects: int = 5,
    timeout_seconds: float = 15.0,
    user_agent: str = "Mozilla/5.0",
    request_func: Optional[Callable[[str], HttpHop]] = None,
    resolved_at: Optional[str] = None,
) -> Dict[str, Any]:
    resolved_at_value = resolved_at or datetime.now(timezone.utc).isoformat()
    source_url = str(row.get("normalized_coupang_url") or row.get("coupang_url") or "").strip()

    base = {
        "idx": row.get("idx", ""),
        "export_key": row.get("export_key", ""),
        "item_pk": row.get("item_pk", ""),
        "link_index": row.get("link_index", ""),
        "coupang_url": row.get("coupang_url", ""),
        "normalized_coupang_url": row.get("normalized_coupang_url", ""),
        "source": row.get("source", ""),
        "user_id": row.get("user_id", ""),
        "username": row.get("username", ""),
        "threads_url": row.get("threads_url", ""),
        "taken_at": row.get("taken_at", ""),
        "is_reply": row.get("is_reply", ""),
        "item_has_coupang_link": row.get("item_has_coupang_link", ""),
        "match_id": row.get("match_id", ""),
        "match_role": row.get("match_role", ""),
        "exception_type": row.get("exception_type", ""),
        "af_lookup_status": "",
        "http_status": "",
        "redirect_count": "",
        "af_id": "",
        "af_id_param": "",
        "normalized_destination_url": "",
        "resolved_host": "",
        "resolved_path": "",
        "error": "",
        "resolved_at": resolved_at_value,
    }
    if not source_url:
        return {
            **base,
            "af_lookup_status": "missing_coupang_url",
            "error": "missing_coupang_url",
        }

    try:
        evidence, http_status, redirect_count, error = resolve_coupang_affiliate(
            source_url,
            max_redirects=max_redirects,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            request_func=request_func,
        )
    except Exception as exc:  # Defensive row isolation for runner-only collection.
        evidence = None
        http_status = 0
        redirect_count = 0
        error = exc.__class__.__name__
    if evidence is None:
        return {
            **base,
            "af_lookup_status": "not_resolved",
            "http_status": http_status,
            "redirect_count": redirect_count,
            "error": error,
        }

    status = "resolved_direct" if redirect_count == 0 else "resolved_redirect"
    return {
        **base,
        "af_lookup_status": status,
        "http_status": http_status,
        "redirect_count": redirect_count,
        "af_id": evidence.af_id,
        "af_id_param": evidence.af_id_param,
        "normalized_destination_url": evidence.normalized_destination_url,
        "resolved_host": evidence.resolved_host,
        "resolved_path": evidence.resolved_path,
    }


def unique_result_from_row(row: Mapping[str, Any], result: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "lookup_url_id": row.get("lookup_url_id", ""),
        "normalized_coupang_url": row.get("normalized_coupang_url", ""),
        "evidence_count": row.get("evidence_count", ""),
        "af_lookup_status": result.get("af_lookup_status", ""),
        "http_status": result.get("http_status", ""),
        "redirect_count": result.get("redirect_count", ""),
        "af_id": result.get("af_id", ""),
        "af_id_param": result.get("af_id_param", ""),
        "normalized_destination_url": result.get("normalized_destination_url", ""),
        "resolved_host": result.get("resolved_host", ""),
        "resolved_path": result.get("resolved_path", ""),
        "error": result.get("error", ""),
        "resolved_at": result.get("resolved_at", ""),
    }


def expand_result_row(row: Mapping[str, Any], unique_result: Mapping[str, Any]) -> Dict[str, Any]:
    return {
        "idx": row.get("idx", ""),
        "export_key": row.get("export_key", ""),
        "item_pk": row.get("item_pk", ""),
        "link_index": row.get("link_index", ""),
        "coupang_url": row.get("coupang_url", ""),
        "normalized_coupang_url": row.get("normalized_coupang_url", ""),
        "source": row.get("source", ""),
        "user_id": row.get("user_id", ""),
        "username": row.get("username", ""),
        "threads_url": row.get("threads_url", ""),
        "taken_at": row.get("taken_at", ""),
        "is_reply": row.get("is_reply", ""),
        "item_has_coupang_link": row.get("item_has_coupang_link", ""),
        "match_id": row.get("match_id", ""),
        "match_role": row.get("match_role", ""),
        "exception_type": row.get("exception_type", ""),
        "af_lookup_status": unique_result.get("af_lookup_status", ""),
        "http_status": unique_result.get("http_status", ""),
        "redirect_count": unique_result.get("redirect_count", ""),
        "af_id": unique_result.get("af_id", ""),
        "af_id_param": unique_result.get("af_id_param", ""),
        "normalized_destination_url": unique_result.get("normalized_destination_url", ""),
        "resolved_host": unique_result.get("resolved_host", ""),
        "resolved_path": unique_result.get("resolved_path", ""),
        "error": unique_result.get("error", ""),
        "resolved_at": unique_result.get("resolved_at", ""),
    }


def taken_at_iso(value: Any) -> str:
    try:
        timestamp = int(str(value))
    except (TypeError, ValueError):
        return ""
    return datetime.fromtimestamp(timestamp, timezone.utc).isoformat()


def build_account_map(rows: Sequence[Mapping[str, Any]], export_key: str) -> List[Dict[str, Any]]:
    grouped: Dict[Tuple[str, str], Dict[str, Any]] = {}
    for row in rows:
        af_id = str(row.get("af_id") or "").strip()
        thread_user_id = str(row.get("user_id") or "").strip()
        thread_username = str(row.get("username") or "").strip()
        if not af_id or not (thread_user_id or thread_username):
            continue

        key = (af_id, thread_user_id or thread_username)
        existing = grouped.get(key)
        if existing is None:
            grouped[key] = {
                "export_key": export_key,
                "af_id": af_id,
                "thread_user_id": thread_user_id,
                "thread_username": thread_username,
                "evidence_count": 1,
                "unique_item_count": 0,
                "matched_item_count": 0,
                "exception_item_count": 0,
                "first_seen_at": taken_at_iso(row.get("taken_at")),
                "last_seen_at": taken_at_iso(row.get("taken_at")),
                "first_item_pk": row.get("item_pk", ""),
                "first_coupang_url": row.get("coupang_url", ""),
                "_item_pks": {row.get("item_pk", "")},
                "_matched_item_pks": {row.get("item_pk", "")} if row.get("match_id") else set(),
                "_exception_item_pks": {row.get("item_pk", "")} if row.get("exception_type") else set(),
            }
            continue

        existing["evidence_count"] = int(existing["evidence_count"]) + 1
        existing["_item_pks"].add(row.get("item_pk", ""))
        if row.get("match_id"):
            existing["_matched_item_pks"].add(row.get("item_pk", ""))
        if row.get("exception_type"):
            existing["_exception_item_pks"].add(row.get("item_pk", ""))
        current_seen = taken_at_iso(row.get("taken_at"))
        if current_seen:
            if not existing["first_seen_at"] or current_seen < existing["first_seen_at"]:
                existing["first_seen_at"] = current_seen
            if not existing["last_seen_at"] or current_seen > existing["last_seen_at"]:
                existing["last_seen_at"] = current_seen

    for row in grouped.values():
        row["unique_item_count"] = len({value for value in row.pop("_item_pks") if value})
        row["matched_item_count"] = len({value for value in row.pop("_matched_item_pks") if value})
        row["exception_item_count"] = len({value for value in row.pop("_exception_item_pks") if value})

    return sorted(
        grouped.values(),
        key=lambda row: (
            str(row.get("af_id", "")),
            str(row.get("thread_user_id", "")),
            str(row.get("thread_username", "")),
        ),
    )


def resolve_af_lookup_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    export_key: str,
    max_redirects: int = 5,
    timeout_seconds: float = 15.0,
    sleep_seconds: float = 0.0,
    user_agent: str = "Mozilla/5.0",
    request_func: Optional[Callable[[str], HttpHop]] = None,
    resolved_at: Optional[str] = None,
    progress_interval: int = 0,
    progress_stream: Optional[TextIO] = None,
    max_workers: int = 1,
) -> AfLookupResult:
    source_rows = list(rows)
    unique_by_url: Dict[str, Dict[str, Any]] = {}
    for row in source_rows:
        normalized_url = str(row.get("normalized_coupang_url") or row.get("coupang_url") or "").strip()
        if normalized_url not in unique_by_url:
            unique_by_url[normalized_url] = {
                "lookup_url_id": len(unique_by_url) + 1,
                "normalized_coupang_url": normalized_url,
                "evidence_count": 0,
            }
        unique_by_url[normalized_url]["evidence_count"] = int(unique_by_url[normalized_url]["evidence_count"]) + 1

    unique_source_rows = list(unique_by_url.values())
    unique_results: List[Dict[str, Any]] = [{} for _ in unique_source_rows]

    def resolve_indexed_row(indexed_row: Tuple[int, Mapping[str, Any]]) -> Tuple[int, Dict[str, Any]]:
        index, row = indexed_row
        result = resolve_row(
            {
                "idx": row.get("lookup_url_id", ""),
                "normalized_coupang_url": row.get("normalized_coupang_url", ""),
            },
            max_redirects=max_redirects,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            request_func=request_func,
            resolved_at=resolved_at,
        )
        return index, unique_result_from_row(row, result)

    workers = max(1, int(max_workers))
    completed = 0
    if workers == 1:
        for index, row in enumerate(unique_source_rows):
            if index > 0 and sleep_seconds > 0:
                time.sleep(sleep_seconds)
            result_index, result_row = resolve_indexed_row((index, row))
            unique_results[result_index] = result_row
            completed += 1
            if progress_interval > 0 and progress_stream and (
                completed == len(unique_source_rows) or completed % progress_interval == 0
            ):
                resolved_count = sum(
                    1
                    for current_row in unique_results
                    if str(current_row.get("af_lookup_status", "")).startswith("resolved_")
                )
                progress_stream.write(
                    f"af lookup progress: processed={completed}/{len(unique_source_rows)} "
                    f"resolved={resolved_count} failed={completed - resolved_count}\n"
                )
                progress_stream.flush()
    else:
        with ThreadPoolExecutor(max_workers=workers) as executor:
            futures = []
            for index, row in enumerate(unique_source_rows):
                if index > 0 and sleep_seconds > 0:
                    time.sleep(sleep_seconds)
                futures.append(executor.submit(resolve_indexed_row, (index, row)))

            for future in as_completed(futures):
                result_index, result_row = future.result()
                unique_results[result_index] = result_row
                completed += 1
                if progress_interval > 0 and progress_stream and (
                    completed == len(unique_source_rows) or completed % progress_interval == 0
                ):
                    resolved_count = sum(
                        1
                        for current_row in unique_results
                        if str(current_row.get("af_lookup_status", "")).startswith("resolved_")
                    )
                    progress_stream.write(
                        f"af lookup progress: processed={completed}/{len(unique_source_rows)} "
                        f"resolved={resolved_count} failed={completed - resolved_count}\n"
                    )
                    progress_stream.flush()

    unique_result_by_url = {
        str(row.get("normalized_coupang_url") or ""): row
        for row in unique_results
    }
    compact_results = [
        expand_result_row(
            row,
            unique_result_by_url.get(
                str(row.get("normalized_coupang_url") or row.get("coupang_url") or "").strip(),
                {},
            ),
        )
        for row in source_rows
    ]
    account_map = build_account_map(compact_results, export_key)
    resolved_rows = sum(
        1 for row in compact_results if str(row.get("af_lookup_status", "")).startswith("resolved_")
    )
    return AfLookupResult(
        export_key=export_key,
        total_rows=len(compact_results),
        resolved_rows=resolved_rows,
        failed_rows=len(compact_results) - resolved_rows,
        unique_af_ids=len({row.get("af_id") for row in compact_results if row.get("af_id")}),
        unique_url_rows=len(unique_results),
        account_pair_rows=len(account_map),
        unique_results=unique_results,
        results=compact_results,
        account_map=account_map,
    )


def output_paths(output_dir: Path, export_key: str) -> Dict[str, Path]:
    return {
        "af_link_lookup_results_csv": output_dir / f"af-link-lookup-results-{export_key}.csv",
        "af_link_lookup_unique_results_csv": output_dir / f"af-link-lookup-unique-results-{export_key}.csv",
        "af_account_map_csv": output_dir / f"af-account-map-{export_key}.csv",
        "af_lookup_manifest_json": output_dir / f"{AF_LOOKUP_MANIFEST_NAME}-{export_key}.json",
    }


def write_af_lookup_outputs(
    result: AfLookupResult,
    output_dir: Path,
    *,
    source_input: Optional[Path] = None,
    extra_manifest: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    paths = output_paths(output_dir, result.export_key)
    write_csv_rows(paths["af_link_lookup_results_csv"], result.results, AF_LINK_LOOKUP_RESULT_FIELDS)
    write_csv_rows(
        paths["af_link_lookup_unique_results_csv"],
        result.unique_results,
        AF_LINK_LOOKUP_UNIQUE_RESULT_FIELDS,
    )
    write_csv_rows(paths["af_account_map_csv"], result.account_map, AF_ACCOUNT_MAP_FIELDS)

    manifest: Dict[str, Any] = {
        "workflow_stage": "resolve_af_lookup",
        **result.summary(),
        "source_input": str(source_input) if source_input else None,
        "output_files": {name: str(path) for name, path in paths.items()},
    }
    if extra_manifest:
        manifest.update(dict(extra_manifest))
    write_json(paths["af_lookup_manifest_json"], manifest)
    return manifest


def read_af_lookup_input(path: Path) -> List[Dict[str, Any]]:
    return read_csv_rows(path)


__all__ = [
    "AF_ACCOUNT_MAP_FIELDS",
    "AF_LINK_LOOKUP_RESULT_FIELDS",
    "AF_LINK_LOOKUP_UNIQUE_RESULT_FIELDS",
    "AfLookupResult",
    "AffiliateEvidence",
    "HttpHop",
    "build_account_map",
    "find_affiliate_evidence",
    "normalize_coupang_short_link",
    "read_af_lookup_input",
    "resolve_af_lookup_rows",
    "resolve_coupang_affiliate",
    "resolve_row",
    "write_af_lookup_outputs",
]
