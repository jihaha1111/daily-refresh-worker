"""Prepare view-count inputs and full Coupang link AF lookup evidence."""

from __future__ import annotations

import csv
import json
from dataclasses import dataclass
from pathlib import Path
from string import ascii_letters, digits
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple
from urllib.parse import urlsplit, urlunsplit

from .view_counts import extract_post_key_from_url


DEFAULT_BODY_LIKE_THRESHOLD = 4

MATCH_VIEW_CANDIDATE_FIELDS = (
    "export_key",
    "match_id",
    "match_confidence",
    "view_lookup_status",
    "view_lookup_policy_version",
    "view_lookup_url_count",
    "first_coupang_url",
    "link_coupang_urls",
    "body_username",
    "body_taken_at_iso",
    "body_threads_url",
    "body_like_count",
    "body_direct_reply_count",
    "body_repost_count",
    "body_reshare_count",
    "body_quote_count",
    "body_view_count",
    "link_username",
    "link_taken_at_iso",
    "link_threads_url",
    "link_like_count",
    "link_direct_reply_count",
    "link_repost_count",
    "link_reshare_count",
    "link_quote_count",
    "link_view_count",
)

VIEW_LOOKUP_INPUT_FIELDS = (
    "idx",
    "url",
)

VIEW_LOOKUP_MAP_FIELDS = (
    "idx",
    "lookup_id",
    "export_key",
    "match_id",
    "role",
    "target_view_field",
    "threads_url",
    "threads_post_key",
    "body_like_count",
    "item_like_count",
    "first_coupang_url",
    "view_lookup_policy_version",
)

VIEW_LOOKUP_SKIPPED_FIELDS = (
    "export_key",
    "match_id",
    "match_confidence",
    "view_lookup_status",
    "view_lookup_skip_reason",
    "view_lookup_policy_version",
    "first_coupang_url",
    "body_username",
    "body_threads_url",
    "body_like_count",
    "link_username",
    "link_threads_url",
    "link_like_count",
)

AF_LINK_LOOKUP_INPUT_FIELDS = (
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
)

AF_LINK_LOOKUP_UNIQUE_URL_FIELDS = (
    "lookup_url_id",
    "normalized_coupang_url",
    "evidence_count",
)

_SHORT_LINK_SLUG_CHARS = set(ascii_letters + digits + "-_")


@dataclass
class LookupPrepareResult:
    export_key: str
    body_like_threshold: int
    policy_version: str
    total_matches: int
    eligible_matches: int
    skipped_matches: int
    lookup_url_rows: int
    af_link_lookup_rows: int
    af_link_unique_url_rows: int
    missing_lookup_url_rows: int
    match_view_candidates: List[Dict[str, Any]]
    view_lookup_input: List[Dict[str, Any]]
    view_lookup_map: List[Dict[str, Any]]
    view_lookup_skipped: List[Dict[str, Any]]
    af_link_lookup_input: List[Dict[str, Any]]
    af_link_lookup_unique_urls: List[Dict[str, Any]]

    def summary(self) -> Dict[str, Any]:
        return {
            "export_key": self.export_key,
            "body_like_threshold": self.body_like_threshold,
            "policy_version": self.policy_version,
            "total_matches": self.total_matches,
            "eligible_matches": self.eligible_matches,
            "skipped_matches": self.skipped_matches,
            "lookup_url_rows": self.lookup_url_rows,
            "af_link_lookup_rows": self.af_link_lookup_rows,
            "af_link_unique_url_rows": self.af_link_unique_url_rows,
            "missing_lookup_url_rows": self.missing_lookup_url_rows,
        }


def policy_version_for_threshold(body_like_threshold: int) -> str:
    return f"v1_body_like_ge_{body_like_threshold}_after_matching"


def parse_optional_int(value: Any) -> Optional[int]:
    if value is None:
        return None
    if isinstance(value, bool):
        return int(value)
    if isinstance(value, int):
        return value
    text = str(value).strip()
    if text == "" or text.lower() in {"none", "null"}:
        return None
    return int(text.replace(",", ""))


def parse_bool(value: Any) -> bool:
    if isinstance(value, bool):
        return value
    text = str(value or "").strip().lower()
    return text in {"1", "true", "t", "yes", "y"}


def ensure_url_scheme(url: str) -> str:
    value = "".join(ch for ch in (url or "") if (ch >= " " and ch != "\ufffc")).strip()
    if not value:
        return ""
    parsed = urlsplit(value)
    if parsed.scheme:
        return value
    if value.startswith("//"):
        return "https:" + value
    return "https://" + value


def host_without_port(netloc: str) -> str:
    host = (netloc or "").split("@")[-1]
    if host.startswith("["):
        return host.lower()
    return host.split(":")[0].lower()


def normalize_coupang_lookup_url(url: str) -> str:
    value = ensure_url_scheme(url)
    if not value:
        return ""

    parsed = urlsplit(value)
    if host_without_port(parsed.netloc) != "link.coupang.com":
        return urlunsplit(
            (
                parsed.scheme or "https",
                parsed.netloc.lower(),
                parsed.path,
                parsed.query,
                "",
            )
        )
    if not parsed.path.startswith("/a/"):
        return urlunsplit((parsed.scheme or "https", parsed.netloc.lower(), parsed.path, "", ""))

    slug = []
    for ch in parsed.path[len("/a/"):]:
        if ch not in _SHORT_LINK_SLUG_CHARS:
            break
        slug.append(ch)

    if not slug:
        return urlunsplit((parsed.scheme or "https", parsed.netloc.lower(), parsed.path, "", ""))

    return urlunsplit((parsed.scheme or "https", parsed.netloc.lower(), "/a/" + "".join(slug), "", ""))


def get_first_coupang_url(row: Mapping[str, Any]) -> str:
    return str(row.get("first_coupang_url") or row.get("link_first_coupang_url") or "")


def threads_post_key(url: str) -> str:
    key = extract_post_key_from_url(url)
    if key is None:
        return ""
    username, code = key
    return f"{username}/{code}"


def read_csv_rows(path: Path) -> List[Dict[str, Any]]:
    with path.open(encoding="utf-8-sig", newline="") as f:
        return list(csv.DictReader(f))


def write_csv_rows(path: Path, rows: Sequence[Mapping[str, Any]], fieldnames: Sequence[str]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=list(fieldnames))
        writer.writeheader()
        for row in rows:
            writer.writerow({field: row.get(field, "") for field in fieldnames})


def write_json(path: Path, value: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2) + "\n", encoding="utf-8")


def view_lookup_skip_reason(body_like_count: Optional[int], threshold: int) -> str:
    if body_like_count is None:
        return "body_like_count_missing"
    if body_like_count < threshold:
        return f"body_like_count_lt_{threshold}"
    return ""


def match_view_candidate_row(
    row: Mapping[str, Any],
    export_key: str,
    policy_version: str,
) -> Dict[str, Any]:
    body_url = str(row.get("body_threads_url") or "").strip()
    link_url = str(row.get("link_threads_url") or "").strip()
    return {
        "export_key": export_key,
        "match_id": row.get("match_id", ""),
        "match_confidence": row.get("match_confidence", ""),
        "view_lookup_status": "queued",
        "view_lookup_policy_version": policy_version,
        "view_lookup_url_count": int(bool(body_url)) + int(bool(link_url)),
        "first_coupang_url": get_first_coupang_url(row),
        "link_coupang_urls": row.get("link_coupang_urls", ""),
        "body_username": row.get("body_username", ""),
        "body_taken_at_iso": row.get("body_taken_at_iso", ""),
        "body_threads_url": body_url,
        "body_like_count": row.get("body_like_count", ""),
        "body_direct_reply_count": row.get("body_direct_reply_count", ""),
        "body_repost_count": row.get("body_repost_count", ""),
        "body_reshare_count": row.get("body_reshare_count", ""),
        "body_quote_count": row.get("body_quote_count", ""),
        "body_view_count": row.get("body_view_count", ""),
        "link_username": row.get("link_username", ""),
        "link_taken_at_iso": row.get("link_taken_at_iso", ""),
        "link_threads_url": link_url,
        "link_like_count": row.get("link_like_count", ""),
        "link_direct_reply_count": row.get("link_direct_reply_count", ""),
        "link_repost_count": row.get("link_repost_count", ""),
        "link_reshare_count": row.get("link_reshare_count", ""),
        "link_quote_count": row.get("link_quote_count", ""),
        "link_view_count": row.get("link_view_count", ""),
    }


def skipped_row(
    row: Mapping[str, Any],
    export_key: str,
    policy_version: str,
    threshold: int,
    body_like_count: Optional[int],
) -> Dict[str, Any]:
    return {
        "export_key": export_key,
        "match_id": row.get("match_id", ""),
        "match_confidence": row.get("match_confidence", ""),
        "view_lookup_status": "skipped",
        "view_lookup_skip_reason": view_lookup_skip_reason(body_like_count, threshold),
        "view_lookup_policy_version": policy_version,
        "first_coupang_url": get_first_coupang_url(row),
        "body_username": row.get("body_username", ""),
        "body_threads_url": row.get("body_threads_url", ""),
        "body_like_count": row.get("body_like_count", ""),
        "link_username": row.get("link_username", ""),
        "link_threads_url": row.get("link_threads_url", ""),
        "link_like_count": row.get("link_like_count", ""),
    }


def append_view_lookup_rows(
    *,
    source_row: Mapping[str, Any],
    export_key: str,
    policy_version: str,
    lookup_index: int,
    view_lookup_input: List[Dict[str, Any]],
    view_lookup_map: List[Dict[str, Any]],
) -> Tuple[int, int]:
    missing_urls = 0
    match_id = str(source_row.get("match_id") or "")
    body_like_count = source_row.get("body_like_count", "")
    first_coupang_url = get_first_coupang_url(source_row)

    role_specs = (
        ("body", "body_threads_url", "body_view_count", "body_like_count"),
        ("link", "link_threads_url", "link_view_count", "link_like_count"),
    )
    for role, url_field, target_view_field, like_field in role_specs:
        url = str(source_row.get(url_field) or "").strip()
        if not url:
            missing_urls += 1
            continue

        view_lookup_input.append(
            {
                "idx": lookup_index,
                "url": url,
            }
        )
        view_lookup_map.append(
            {
                "idx": lookup_index,
                "lookup_id": f"{match_id}:{role}",
                "export_key": export_key,
                "match_id": match_id,
                "role": role,
                "target_view_field": target_view_field,
                "threads_url": url,
                "threads_post_key": threads_post_key(url),
                "body_like_count": body_like_count,
                "item_like_count": source_row.get(like_field, ""),
                "first_coupang_url": first_coupang_url,
                "view_lookup_policy_version": policy_version,
            }
        )
        lookup_index += 1

    return lookup_index, missing_urls


def build_match_role_by_item(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Tuple[str, str]]:
    roles: Dict[str, Tuple[str, str]] = {}
    for row in rows:
        match_id = str(row.get("match_id") or "").strip()
        if not match_id:
            continue
        body_pk = str(row.get("body_pk") or "").strip()
        link_pk = str(row.get("link_pk") or "").strip()
        if body_pk and body_pk not in roles:
            roles[body_pk] = (match_id, "body")
        if link_pk and link_pk not in roles:
            roles[link_pk] = (match_id, "link")
    return roles


def build_exception_type_by_item(rows: Iterable[Mapping[str, Any]]) -> Dict[str, str]:
    result: Dict[str, str] = {}
    for row in rows:
        item_pk = str(row.get("item_pk") or "").strip()
        exception_type = str(row.get("exception_type") or "").strip()
        if item_pk and exception_type and item_pk not in result:
            result[item_pk] = exception_type
    return result


def build_af_link_lookup_input(
    *,
    item_links_rows: Iterable[Mapping[str, Any]],
    items_core_rows: Iterable[Mapping[str, Any]],
    matches_core_rows: Iterable[Mapping[str, Any]],
    exceptions_core_rows: Iterable[Mapping[str, Any]],
    export_key: str,
) -> List[Dict[str, Any]]:
    items_by_pk = {
        str(row.get("pk") or row.get("item_pk") or "").strip(): row
        for row in items_core_rows
        if str(row.get("pk") or row.get("item_pk") or "").strip()
    }
    match_role_by_item = build_match_role_by_item(matches_core_rows)
    exception_type_by_item = build_exception_type_by_item(exceptions_core_rows)
    rows: List[Dict[str, Any]] = []

    for row in item_links_rows:
        if not parse_bool(row.get("is_coupang_link")):
            continue
        item_pk = str(row.get("item_pk") or "").strip()
        link_index = str(row.get("link_index") or "").strip()
        coupang_url = str(row.get("url") or "").strip()
        if not item_pk or not link_index or not coupang_url:
            continue

        item = items_by_pk.get(item_pk, {})
        match_id, match_role = match_role_by_item.get(item_pk, ("", ""))
        rows.append(
            {
                "idx": len(rows) + 1,
                "export_key": export_key,
                "item_pk": item_pk,
                "link_index": link_index,
                "coupang_url": coupang_url,
                "normalized_coupang_url": normalize_coupang_lookup_url(coupang_url),
                "source": row.get("source", ""),
                "user_id": item.get("user_id", ""),
                "username": item.get("username", ""),
                "threads_url": item.get("threads_url", ""),
                "taken_at": item.get("taken_at", ""),
                "is_reply": item.get("is_reply", ""),
                "item_has_coupang_link": item.get("has_coupang_link", ""),
                "match_id": match_id,
                "match_role": match_role,
                "exception_type": exception_type_by_item.get(item_pk, ""),
            }
        )

    return rows


def build_af_link_unique_urls(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    grouped: Dict[str, int] = {}
    for row in rows:
        normalized_url = str(row.get("normalized_coupang_url") or "").strip()
        if not normalized_url:
            continue
        grouped[normalized_url] = grouped.get(normalized_url, 0) + 1

    return [
        {
            "lookup_url_id": index,
            "normalized_coupang_url": normalized_url,
            "evidence_count": grouped[normalized_url],
        }
        for index, normalized_url in enumerate(sorted(grouped), start=1)
    ]


def build_lookup_prepare_result(
    rows: Iterable[Mapping[str, Any]],
    export_key: str,
    body_like_threshold: int = DEFAULT_BODY_LIKE_THRESHOLD,
    item_links_rows: Optional[Iterable[Mapping[str, Any]]] = None,
    items_core_rows: Optional[Iterable[Mapping[str, Any]]] = None,
    matches_core_rows: Optional[Iterable[Mapping[str, Any]]] = None,
    exceptions_core_rows: Optional[Iterable[Mapping[str, Any]]] = None,
) -> LookupPrepareResult:
    if body_like_threshold < 0:
        raise ValueError("body_like_threshold must be non-negative")

    policy_version = policy_version_for_threshold(body_like_threshold)
    source_rows = list(rows)
    match_view_candidates: List[Dict[str, Any]] = []
    view_lookup_input: List[Dict[str, Any]] = []
    view_lookup_map: List[Dict[str, Any]] = []
    view_lookup_skipped: List[Dict[str, Any]] = []
    lookup_index = 1
    missing_lookup_url_rows = 0

    for row in source_rows:
        body_like_count = parse_optional_int(row.get("body_like_count"))
        if body_like_count is None or body_like_count < body_like_threshold:
            view_lookup_skipped.append(
                skipped_row(
                    row,
                    export_key,
                    policy_version,
                    body_like_threshold,
                    body_like_count,
                )
            )
            continue

        match_view_candidates.append(match_view_candidate_row(row, export_key, policy_version))
        lookup_index, missing_urls = append_view_lookup_rows(
            source_row=row,
            export_key=export_key,
            policy_version=policy_version,
            lookup_index=lookup_index,
            view_lookup_input=view_lookup_input,
            view_lookup_map=view_lookup_map,
        )
        missing_lookup_url_rows += missing_urls

    af_link_lookup_input = build_af_link_lookup_input(
        item_links_rows=item_links_rows or [],
        items_core_rows=items_core_rows or [],
        matches_core_rows=matches_core_rows or [],
        exceptions_core_rows=exceptions_core_rows or [],
        export_key=export_key,
    )
    af_link_lookup_unique_urls = build_af_link_unique_urls(af_link_lookup_input)

    return LookupPrepareResult(
        export_key=export_key,
        body_like_threshold=body_like_threshold,
        policy_version=policy_version,
        total_matches=len(source_rows),
        eligible_matches=len(match_view_candidates),
        skipped_matches=len(view_lookup_skipped),
        lookup_url_rows=len(view_lookup_input),
        af_link_lookup_rows=len(af_link_lookup_input),
        af_link_unique_url_rows=len(af_link_lookup_unique_urls),
        missing_lookup_url_rows=missing_lookup_url_rows,
        match_view_candidates=match_view_candidates,
        view_lookup_input=view_lookup_input,
        view_lookup_map=view_lookup_map,
        view_lookup_skipped=view_lookup_skipped,
        af_link_lookup_input=af_link_lookup_input,
        af_link_lookup_unique_urls=af_link_lookup_unique_urls,
    )


def output_paths(output_dir: Path, export_key: str) -> Dict[str, Path]:
    return {
        "match_view_candidates_csv": output_dir / f"match-view-candidates-{export_key}.csv",
        "view_lookup_input_csv": output_dir / f"view-lookup-input-{export_key}.csv",
        "view_lookup_map_csv": output_dir / f"view-lookup-map-{export_key}.csv",
        "view_lookup_skipped_csv": output_dir / f"view-lookup-skipped-{export_key}.csv",
        "af_link_lookup_input_csv": output_dir / f"af-link-lookup-input-{export_key}.csv",
        "af_link_lookup_unique_urls_csv": output_dir / f"af-link-lookup-unique-urls-{export_key}.csv",
        "run_manifest_json": output_dir / f"run-manifest-{export_key}.json",
    }


def write_lookup_prepare_outputs(
    result: LookupPrepareResult,
    output_dir: Path,
    *,
    source_match_summary: Optional[Path] = None,
    extra_manifest: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    paths = output_paths(output_dir, result.export_key)

    write_csv_rows(
        paths["match_view_candidates_csv"],
        result.match_view_candidates,
        MATCH_VIEW_CANDIDATE_FIELDS,
    )
    write_csv_rows(
        paths["view_lookup_input_csv"],
        result.view_lookup_input,
        VIEW_LOOKUP_INPUT_FIELDS,
    )
    write_csv_rows(
        paths["view_lookup_map_csv"],
        result.view_lookup_map,
        VIEW_LOOKUP_MAP_FIELDS,
    )
    write_csv_rows(
        paths["view_lookup_skipped_csv"],
        result.view_lookup_skipped,
        VIEW_LOOKUP_SKIPPED_FIELDS,
    )
    write_csv_rows(
        paths["af_link_lookup_input_csv"],
        result.af_link_lookup_input,
        AF_LINK_LOOKUP_INPUT_FIELDS,
    )
    write_csv_rows(
        paths["af_link_lookup_unique_urls_csv"],
        result.af_link_lookup_unique_urls,
        AF_LINK_LOOKUP_UNIQUE_URL_FIELDS,
    )

    manifest: Dict[str, Any] = {
        "workflow_stage": "prepare_lookup",
        **result.summary(),
        "source_match_summary": str(source_match_summary) if source_match_summary else None,
        "output_files": {name: str(path) for name, path in paths.items()},
    }
    if extra_manifest:
        manifest.update(dict(extra_manifest))
    write_json(paths["run_manifest_json"], manifest)
    return manifest


__all__ = [
    "AF_LINK_LOOKUP_INPUT_FIELDS",
    "AF_LINK_LOOKUP_UNIQUE_URL_FIELDS",
    "DEFAULT_BODY_LIKE_THRESHOLD",
    "MATCH_VIEW_CANDIDATE_FIELDS",
    "VIEW_LOOKUP_INPUT_FIELDS",
    "VIEW_LOOKUP_MAP_FIELDS",
    "VIEW_LOOKUP_SKIPPED_FIELDS",
    "LookupPrepareResult",
    "build_af_link_lookup_input",
    "build_af_link_unique_urls",
    "build_lookup_prepare_result",
    "normalize_coupang_lookup_url",
    "output_paths",
    "policy_version_for_threshold",
    "read_csv_rows",
    "write_lookup_prepare_outputs",
]
