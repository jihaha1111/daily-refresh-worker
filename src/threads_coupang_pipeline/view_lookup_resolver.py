"""Resolve prepared Threads view-count lookup rows."""

from __future__ import annotations

import csv
import html
import json
import logging
import re
import time
from contextlib import contextmanager, redirect_stderr, redirect_stdout
from dataclasses import dataclass
from datetime import datetime, timezone
from io import StringIO
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, Iterator, List, Mapping, Optional, Sequence, TextIO, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import urlparse
from urllib.request import Request, urlopen

from .lookup_prepare import read_csv_rows, write_csv_rows, write_json
from .view_counts import extract_post_key_from_url


ALLOWED_THREADS_HOSTS = {
    "threads.com",
    "www.threads.com",
    "threads.net",
    "www.threads.net",
}

VIEW_LOOKUP_STATE_FIELDS = (
    "idx",
    "url",
    "view_counts_value",
    "view_lookup_status",
    "http_status",
    "error",
    "internal_fetch_statuses",
    "redirect_observed",
    "fetch_warning_count",
    "fetch_warning_codes",
    "resolved_at",
)

VIEW_LOOKUP_RESULT_FIELDS = (
    "idx",
    "url",
    "lookup_id",
    "export_key",
    "match_id",
    "role",
    "target_view_field",
    "threads_post_key",
    "view_counts_value",
    "view_lookup_status",
    "http_status",
    "error",
    "internal_fetch_statuses",
    "redirect_observed",
    "fetch_warning_count",
    "fetch_warning_codes",
    "resolved_at",
)

THREADS_VIEWCOUNT_FIELDS = (
    "idx",
    "url",
    "view_counts_value",
)

DEFERRED_STATUSES = {
    "rate_limited",
    "request_error",
    "unexecuted_after_rate_limit",
    "unexecuted_missing_result",
}

RATE_LIMIT_HTTP_STATUSES = {403, 429}
COUNT_FIELD_NAMES = ("view_count", "public_view_count", "play_count")
VIEW_LOOKUP_MANIFEST_NAME = "view-lookup-manifest"
PROBE_MODES = ("static", "scrapling-dynamic", "scrapling-stealth")

_SCRIPT_RE = re.compile(r"<script\b[^>]*>(.*?)</script>", re.IGNORECASE | re.DOTALL)
_COUNT_FIELD_RE = re.compile(
    r'\\?"(?:view_count|public_view_count|play_count)\\?"\s*:\s*\\?"?([0-9][0-9,]*)',
    re.IGNORECASE,
)
_SCRAPLING_FETCHED_STATUS_RE = re.compile(r"\bFetched\s+\((\d{3})\)\s+<", re.IGNORECASE)
_FETCH_WARNING_CODE_RE = re.compile(r"\b(ERR_[A-Z0-9_]+|[A-Z][A-Z0-9_]*(?:ERROR|TIMEOUT|FAILURE)[A-Z0-9_]*)\b")
_VISIBLE_VIEW_COUNT_RES = (
    re.compile(r"([0-9][0-9,.]*\s*[KMBkmb]?)\s+views?\b", re.IGNORECASE),
    re.compile(r"조회수\s*([0-9][0-9,.]*\s*[KMBkmb만천억]?)"),
    re.compile(r"([0-9][0-9,.]*\s*[KMBkmb만천억]?)\s*조회"),
)
_ACCOUNT_UNAVAILABLE_MARKERS = (
    "account has been suspended",
    "account is suspended",
    "this account is unavailable",
    "this profile isn't available",
    "this profile is not available",
    "profile isn't available",
    "profile is not available",
    "\uacc4\uc815\uc774 \uc815\uc9c0",
    "\uc774 \uacc4\uc815\uc740 \uc0ac\uc6a9\ud560 \uc218 \uc5c6",
    "\ud504\ub85c\ud544\uc744 \uc0ac\uc6a9\ud560 \uc218 \uc5c6",
)
_POST_UNAVAILABLE_MARKERS = (
    "this content isn't available",
    "this content is not available",
    "this page isn't available",
    "this page is not available",
    "this post isn't available",
    "this post is not available",
    "page may have been removed",
    "post may have been removed",
    "content may have been removed",
    "page was removed",
    "post was removed",
    "\ucf58\ud150\uce20\ub97c \uc774\uc6a9\ud560 \uc218 \uc5c6",
    "\ud398\uc774\uc9c0\ub97c \uc0ac\uc6a9\ud560 \uc218 \uc5c6",
    "\uac8c\uc2dc\ubb3c\uc744 \uc0ac\uc6a9\ud560 \uc218 \uc5c6",
    "\uc0ad\uc81c\ub418\uc5c8\uc744 \uc218",
)
_LOGIN_REQUIRED_MARKERS = (
    "log in to see more",
    "log in to view",
    "login to view",
    "sign up to see more",
)


@dataclass(frozen=True)
class HttpResponse:
    status: int
    body: str
    final_url: str
    error: str = ""
    internal_fetch_statuses: Tuple[int, ...] = ()
    fetch_warning_count: int = 0
    fetch_warning_codes: Tuple[str, ...] = ()


@dataclass
class ViewLookupMergeResult:
    export_key: str
    total_rows: int
    resolved_rows: int
    retry_rows: int
    status_counts: Dict[str, int]
    results: List[Dict[str, Any]]
    final_rows: List[Dict[str, Any]]
    retry_state_rows: List[Dict[str, Any]]

    def summary(self) -> Dict[str, Any]:
        return {
            "export_key": self.export_key,
            "total_rows": self.total_rows,
            "resolved_rows": self.resolved_rows,
            "retry_rows": self.retry_rows,
            "status_counts": dict(self.status_counts),
        }


def utc_timestamp() -> str:
    return datetime.now(timezone.utc).isoformat()


def idx_sort_key(idx: str) -> Tuple[int, str]:
    try:
        return int(str(idx)), str(idx)
    except ValueError:
        return 10**18, str(idx)


def is_allowed_threads_url(url: str) -> bool:
    parsed = urlparse((url or "").strip())
    host = (parsed.hostname or "").lower()
    return parsed.scheme in {"http", "https"} and host in ALLOWED_THREADS_HOSTS


def read_allowed_lookup_rows(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    seen_idx = set()
    for row in read_csv_rows(path):
        idx = str(row.get("idx") or "").strip()
        url = str(row.get("url") or row.get("threads_url") or "").strip()
        if not idx or not url or not is_allowed_threads_url(url):
            continue
        if idx in seen_idx:
            continue
        rows.append({"idx": idx, "url": url})
        seen_idx.add(idx)
    return rows


def read_state_rows(path: Path) -> List[Dict[str, str]]:
    if not path.exists():
        return []
    rows: List[Dict[str, str]] = []
    seen_idx = set()
    for row in read_csv_rows(path):
        idx = str(row.get("idx") or "").strip()
        url = str(row.get("url") or "").strip()
        if not idx or not url or not is_allowed_threads_url(url):
            continue
        if idx in seen_idx:
            continue
        rows.append({field: str(row.get(field) or "") for field in VIEW_LOOKUP_STATE_FIELDS})
        seen_idx.add(idx)
    return rows


def write_numbered_rows(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", encoding="utf-8") as f:
        for row in rows:
            f.write(f"{row.get('idx', '')}\t{row.get('url', '')}\n")


def read_numbered_rows(path: Path) -> List[Dict[str, str]]:
    rows: List[Dict[str, str]] = []
    if not path.exists():
        return rows
    with path.open(encoding="utf-8") as f:
        for line in f:
            value = line.rstrip("\n")
            if not value:
                continue
            parts = value.split("\t", 1)
            if len(parts) != 2:
                continue
            idx, url = parts[0].strip(), parts[1].strip()
            if not idx or not url:
                continue
            rows.append({"idx": idx, "url": url})
    return rows


def limit_rows(rows: Sequence[Dict[str, str]], max_rows: int = 0) -> List[Dict[str, str]]:
    if max_rows <= 0:
        return list(rows)
    return list(rows[:max_rows])


def prepare_initial_state(input_path: Path, output_dir: Path, *, max_rows: int = 0) -> Dict[str, Any]:
    source_rows = read_allowed_lookup_rows(input_path)
    rows = limit_rows(source_rows, max_rows)
    output_dir.mkdir(parents=True, exist_ok=True)
    write_csv_rows(output_dir / "all_input.csv", rows, ("idx", "url"))
    write_numbered_rows(output_dir / "numbered.txt", rows)
    return {
        "mode": "initial",
        "source_rows": len(source_rows),
        "all_input_rows": len(rows),
        "numbered_rows": len(rows),
        "max_rows": max_rows,
    }


def prepare_retry_state(previous_state_dir: Path, output_dir: Path, *, max_rows: int = 0) -> Dict[str, Any]:
    retry_path = previous_state_dir / "rate_limited_and_unexecuted.csv"
    if not retry_path.exists():
        raise FileNotFoundError("Previous retry-state does not contain rate_limited_and_unexecuted.csv")

    output_dir.mkdir(parents=True, exist_ok=True)

    previous_final_rows = read_state_rows(previous_state_dir / "final.csv")
    if previous_final_rows:
        write_csv_rows(output_dir / "previous_final.csv", previous_final_rows, VIEW_LOOKUP_STATE_FIELDS)

    all_input_path = previous_state_dir / "all_input.csv"
    if all_input_path.exists():
        all_input_rows = read_allowed_lookup_rows(all_input_path)
    else:
        rows_by_idx: Dict[str, str] = {}
        for row in previous_final_rows + read_state_rows(retry_path):
            idx = str(row.get("idx") or "").strip()
            url = str(row.get("url") or "").strip()
            if idx and url and is_allowed_threads_url(url):
                rows_by_idx.setdefault(idx, url)
        all_input_rows = [
            {"idx": idx, "url": rows_by_idx[idx]}
            for idx in sorted(rows_by_idx, key=idx_sort_key)
        ]

    retry_source_rows = read_state_rows(retry_path)
    retry_rows = limit_rows(retry_source_rows, max_rows)
    if max_rows > 0:
        allowed_retry_idx = {str(row.get("idx") or "") for row in retry_rows}
        previous_input_rows = [
            {"idx": str(row.get("idx") or ""), "url": str(row.get("url") or "")}
            for row in previous_final_rows
            if row.get("idx") and row.get("url")
        ]
        retry_input_rows = [
            row
            for row in all_input_rows
            if str(row.get("idx") or "") in allowed_retry_idx
        ]
        rows_by_idx = {
            str(row.get("idx") or ""): row
            for row in previous_input_rows + retry_input_rows
            if row.get("idx")
        }
        all_input_rows = [
            rows_by_idx[idx]
            for idx in sorted(rows_by_idx, key=idx_sort_key)
        ]
    write_csv_rows(output_dir / "all_input.csv", all_input_rows, ("idx", "url"))
    write_numbered_rows(output_dir / "numbered.txt", retry_rows)
    return {
        "mode": "retry",
        "all_input_rows": len(all_input_rows),
        "numbered_rows": len(retry_rows),
        "previous_final_rows": len(previous_final_rows),
        "retry_source_rows": len(retry_source_rows),
        "max_rows": max_rows,
    }


def split_numbered_rows(
    numbered_path: Path,
    output_dir: Path,
    *,
    shard_size: int,
    target_shard_count: int = 0,
) -> Dict[str, Any]:
    if shard_size <= 0:
        raise ValueError("shard_size must be positive")
    if target_shard_count < 0:
        raise ValueError("target_shard_count must be non-negative")

    rows = read_numbered_rows(numbered_path)
    output_dir.mkdir(parents=True, exist_ok=True)
    for existing_shard in output_dir.glob("shard-*.tsv"):
        existing_shard.unlink()
    shard_rows: list[list[Dict[str, str]]] = []
    split_mode = "fixed_size"

    if target_shard_count > 0:
        split_mode = "balanced"
        if rows:
            base_size, remainder = divmod(len(rows), target_shard_count)
            start = 0
            for index in range(target_shard_count):
                size = base_size + (1 if index < remainder else 0)
                shard_rows.append(rows[start:start + size])
                start += size
    else:
        for start in range(0, len(rows), shard_size):
            shard_rows.append(rows[start:start + shard_size])

    for shard_index, shard in enumerate(shard_rows, start=1):
        write_numbered_rows(output_dir / f"shard-{shard_index}.tsv", shard)

    shard_count = len(shard_rows)

    matrix = {"include": [{"shard": shard} for shard in range(1, shard_count + 1)]}
    return {
        "total_rows": len(rows),
        "shard_count": shard_count,
        "has_work": bool(rows),
        "split_mode": split_mode,
        "shard_size": shard_size,
        "target_shard_count": target_shard_count,
        "shard_rows": [
            {"shard": index, "rows": len(shard)}
            for index, shard in enumerate(shard_rows, start=1)
        ],
        "matrix": matrix,
    }


def http_get(url: str, timeout_seconds: float, user_agent: str) -> HttpResponse:
    request = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        method="GET",
    )
    try:
        with urlopen(request, timeout=timeout_seconds) as response:
            body = response.read().decode("utf-8", errors="replace")
            return HttpResponse(
                status=int(getattr(response, "status", 200)),
                body=body,
                final_url=response.geturl(),
            )
    except HTTPError as exc:
        try:
            body = exc.read().decode("utf-8", errors="replace")
        except Exception:
            body = ""
        return HttpResponse(status=int(exc.code), body=body, final_url=exc.geturl(), error=str(exc))
    except URLError as exc:
        reason = getattr(exc, "reason", str(exc))
        return HttpResponse(status=0, body="", final_url=url, error=str(reason))
    except (OSError, TimeoutError, UnicodeError, ValueError) as exc:
        return HttpResponse(status=0, body="", final_url=url, error=exc.__class__.__name__)


def response_body_text(response: Any) -> str:
    body = getattr(response, "body", None)
    if isinstance(body, bytes):
        return body.decode("utf-8", errors="replace")
    if isinstance(body, str):
        return body

    html_value = getattr(response, "html", None)
    if callable(html_value):
        try:
            value = html_value()
            if isinstance(value, str):
                return value
        except Exception:
            pass
    elif isinstance(html_value, str):
        return html_value

    return str(response)


class _ScraplingLogCapture(logging.Handler):
    def __init__(self) -> None:
        super().__init__(level=logging.INFO)
        self.records: List[Tuple[int, str]] = []

    def emit(self, record: logging.LogRecord) -> None:
        try:
            message = record.getMessage()
        except Exception:
            message = str(record.msg)
        self.records.append((record.levelno, message))


@contextmanager
def capture_scrapling_logs() -> Iterator[_ScraplingLogCapture]:
    """Capture Scrapling's URL-heavy fetch logs so runner output stays compact."""
    logger = logging.getLogger("scrapling")
    previous_handlers = list(logger.handlers)
    previous_level = logger.level
    previous_propagate = logger.propagate
    capture = _ScraplingLogCapture()

    for handler in previous_handlers:
        logger.removeHandler(handler)
    logger.addHandler(capture)
    logger.setLevel(logging.INFO)
    logger.propagate = False

    try:
        yield capture
    finally:
        logger.removeHandler(capture)
        for handler in previous_handlers:
            logger.addHandler(handler)
        logger.setLevel(previous_level)
        logger.propagate = previous_propagate


def unique_preserving_order(values: Iterable[str]) -> Tuple[str, ...]:
    seen = set()
    ordered: List[str] = []
    for value in values:
        if value and value not in seen:
            ordered.append(value)
            seen.add(value)
    return tuple(ordered)


def parse_fetch_diagnostics(records: Sequence[Tuple[int, str]]) -> Tuple[Tuple[int, ...], int, Tuple[str, ...]]:
    statuses: List[int] = []
    warning_codes: List[str] = []
    warning_count = 0

    for levelno, message in records:
        for match in _SCRAPLING_FETCHED_STATUS_RE.finditer(message):
            statuses.append(int(match.group(1)))

        codes = [match.group(1) for match in _FETCH_WARNING_CODE_RE.finditer(message)]
        if levelno >= logging.WARNING or codes:
            warning_count += 1
            warning_codes.extend(codes)

    return tuple(statuses), warning_count, unique_preserving_order(warning_codes)


def text_stream_log_records(text: str) -> List[Tuple[int, str]]:
    records: List[Tuple[int, str]] = []
    for line in (text or "").splitlines():
        message = line.strip()
        if not message:
            continue
        upper_message = message.upper()
        if "WARNING" in upper_message or "ERROR" in upper_message or "ERR_" in upper_message:
            levelno = logging.WARNING
        else:
            levelno = logging.INFO
        records.append((levelno, message))
    return records


def summarize_internal_fetch_statuses(statuses: Sequence[int]) -> str:
    if not statuses:
        return ""
    parts: List[str] = []
    previous: Optional[int] = None
    count = 0
    for status in statuses:
        if previous is None:
            previous = status
            count = 1
            continue
        if status == previous:
            count += 1
            continue
        parts.append(f"{previous}x{count}" if count > 1 else str(previous))
        previous = status
        count = 1
    if previous is not None:
        parts.append(f"{previous}x{count}" if count > 1 else str(previous))
    return "|".join(parts)


def has_redirect_signal(url: str, response: HttpResponse) -> bool:
    if any(300 <= status < 400 for status in response.internal_fetch_statuses):
        return True
    final_url = (response.final_url or "").strip()
    return bool(final_url and final_url != (url or "").strip())


def format_exception_error(prefix: str, exc: BaseException) -> str:
    message = str(exc).strip().replace("\n", " ")
    if message:
        return f"{prefix}:{exc.__class__.__name__}:{message[:160]}"
    return f"{prefix}:{exc.__class__.__name__}"


def scrapling_fetch(
    url: str,
    timeout_seconds: float,
    user_agent: str,
    *,
    stealth: bool = False,
) -> HttpResponse:
    try:
        if stealth:
            from scrapling.fetchers import StealthyFetcher as Fetcher  # type: ignore
        else:
            from scrapling.fetchers import DynamicFetcher as Fetcher  # type: ignore
    except Exception as exc:
        return HttpResponse(status=0, body="", final_url=url, error=f"scrapling_import_error:{exc.__class__.__name__}")

    capture: Optional[_ScraplingLogCapture] = None
    stdout_buffer = StringIO()
    stderr_buffer = StringIO()
    try:
        with capture_scrapling_logs() as capture, redirect_stdout(stdout_buffer), redirect_stderr(stderr_buffer):
            response = Fetcher.fetch(
                url,
                headless=True,
                disable_resources=True,
                network_idle=True,
                timeout=int(timeout_seconds * 1000),
                wait=2000,
                useragent=user_agent,
            )
        log_records = list(capture.records)
        log_records.extend(text_stream_log_records(stdout_buffer.getvalue()))
        log_records.extend(text_stream_log_records(stderr_buffer.getvalue()))
        internal_statuses, warning_count, warning_codes = parse_fetch_diagnostics(log_records)
        status = int(getattr(response, "status", 200) or 200)
        final_url = str(getattr(response, "url", url) or url)
        return HttpResponse(
            status=status,
            body=response_body_text(response),
            final_url=final_url,
            internal_fetch_statuses=internal_statuses,
            fetch_warning_count=warning_count,
            fetch_warning_codes=warning_codes,
        )
    except Exception as exc:
        log_records = list(capture.records) if capture is not None else []
        log_records.extend(text_stream_log_records(stdout_buffer.getvalue()))
        log_records.extend(text_stream_log_records(stderr_buffer.getvalue()))
        internal_statuses, warning_count, warning_codes = parse_fetch_diagnostics(log_records)
        return HttpResponse(
            status=0,
            body="",
            final_url=url,
            error=format_exception_error("scrapling_fetch_error", exc),
            internal_fetch_statuses=internal_statuses,
            fetch_warning_count=warning_count,
            fetch_warning_codes=warning_codes,
        )


def fetch_for_mode(probe_mode: str, timeout_seconds: float, user_agent: str) -> Callable[[str], HttpResponse]:
    if probe_mode == "static":
        return lambda next_url: http_get(next_url, timeout_seconds, user_agent)
    if probe_mode == "scrapling-dynamic":
        return lambda next_url: scrapling_fetch(next_url, timeout_seconds, user_agent, stealth=False)
    if probe_mode == "scrapling-stealth":
        return lambda next_url: scrapling_fetch(next_url, timeout_seconds, user_agent, stealth=True)
    raise ValueError(f"Unknown probe mode: {probe_mode}")


def parse_count_value(value: Any) -> Optional[int]:
    if value is None or isinstance(value, bool):
        return None
    if isinstance(value, int):
        return value if value >= 0 else None
    text = str(value).strip().replace(",", "")
    if not text or not text.isdigit():
        return None
    parsed = int(text)
    return parsed if parsed >= 0 else None


def parse_compact_count(value: str) -> Optional[int]:
    text = (value or "").strip().replace(",", "")
    if not text:
        return None
    match = re.fullmatch(r"([0-9]+(?:\.[0-9]+)?)\s*([KMBkmb만천억]?)", text)
    if not match:
        return parse_count_value(text)

    number = float(match.group(1))
    suffix = match.group(2).lower()
    multiplier = {
        "": 1,
        "k": 1_000,
        "m": 1_000_000,
        "b": 1_000_000_000,
        "천": 1_000,
        "만": 10_000,
        "억": 100_000_000,
    }.get(suffix)
    if multiplier is None:
        return None
    return int(number * multiplier)


def first_count_in_tree(value: Any) -> Optional[int]:
    if isinstance(value, dict):
        for key in COUNT_FIELD_NAMES:
            count = parse_count_value(value.get(key))
            if count is not None:
                return count
        for child in value.values():
            count = first_count_in_tree(child)
            if count is not None:
                return count
    elif isinstance(value, list):
        for child in value:
            count = first_count_in_tree(child)
            if count is not None:
                return count
    return None


def dict_mentions_code(value: Mapping[str, Any], code: str) -> bool:
    for key in ("code", "shortcode"):
        if str(value.get(key) or "") == code:
            return True
    for key in ("url", "permalink", "threads_url"):
        if code in str(value.get(key) or ""):
            return True
    return False


def find_count_for_code(value: Any, code: str) -> Optional[int]:
    if isinstance(value, dict):
        if dict_mentions_code(value, code):
            count = first_count_in_tree(value)
            if count is not None:
                return count
        for child in value.values():
            count = find_count_for_code(child, code)
            if count is not None:
                return count
    elif isinstance(value, list):
        for child in value:
            count = find_count_for_code(child, code)
            if count is not None:
                return count
    return None


def iter_script_texts(markup: str) -> Iterable[str]:
    for match in _SCRIPT_RE.finditer(markup):
        text = html.unescape(match.group(1) or "").strip()
        if text:
            yield text


def count_from_json_script(script_text: str, code: str) -> Optional[int]:
    text = script_text.strip()
    if not text or text[0] not in "[{":
        return None
    try:
        value = json.loads(text)
    except json.JSONDecodeError:
        return None
    return find_count_for_code(value, code)


def count_from_text_window(markup: str, code: str) -> Optional[int]:
    text = html.unescape(markup)
    for match in re.finditer(re.escape(code), text):
        start = max(0, match.start() - 12000)
        end = min(len(text), match.end() + 12000)
        window = text[start:end]
        for count_match in _COUNT_FIELD_RE.finditer(window):
            count = parse_count_value(count_match.group(1))
            if count is not None:
                return count
    return None


def count_from_visible_text(markup: str) -> Optional[int]:
    text = re.sub(r"\s+", " ", html.unescape(markup))
    for pattern in _VISIBLE_VIEW_COUNT_RES:
        match = pattern.search(text)
        if not match:
            continue
        count = parse_compact_count(match.group(1))
        if count is not None:
            return count
    return None


def extract_view_count_from_html(markup: str, url: str) -> Optional[int]:
    post_key = extract_post_key_from_url(url)
    if post_key is None:
        return None
    _, code = post_key

    for script_text in iter_script_texts(markup):
        count = count_from_json_script(script_text, code)
        if count is not None:
            return count

    return count_from_text_window(markup, code) or count_from_visible_text(markup)


def normalized_markup_text(markup: str) -> str:
    return re.sub(r"\s+", " ", html.unescape(markup or "")).lower()


def classify_unavailable_html(markup: str) -> str:
    text = normalized_markup_text(markup)
    if any(marker in text for marker in _ACCOUNT_UNAVAILABLE_MARKERS):
        return "account_unavailable"
    if any(marker in text for marker in _POST_UNAVAILABLE_MARKERS):
        return "post_unavailable"
    if any(marker in text for marker in _LOGIN_REQUIRED_MARKERS):
        return "login_required"
    return "view_count_not_found"


def state_row(
    row: Mapping[str, Any],
    *,
    status: str,
    view_count: Optional[int] = None,
    http_status: Any = "",
    error: str = "",
    internal_fetch_statuses: str = "",
    redirect_observed: Any = "",
    fetch_warning_count: Any = "",
    fetch_warning_codes: str = "",
    diagnostic_event: str = "",
    missing_count_attempt: Any = "",
    resolved_at: Optional[str] = None,
) -> Dict[str, Any]:
    return {
        "idx": row.get("idx", ""),
        "url": row.get("url", ""),
        "view_counts_value": "" if view_count is None else view_count,
        "view_lookup_status": status,
        "http_status": http_status,
        "error": error,
        "internal_fetch_statuses": internal_fetch_statuses,
        "redirect_observed": redirect_observed,
        "fetch_warning_count": fetch_warning_count,
        "fetch_warning_codes": fetch_warning_codes,
        "resolved_at": resolved_at or utc_timestamp(),
        "_diagnostic_event": diagnostic_event,
        "_missing_count_attempt": missing_count_attempt,
    }


def response_diagnostic_fields(url: str, response: HttpResponse) -> Dict[str, Any]:
    return combined_response_diagnostic_fields(url, [response])


def combined_response_diagnostic_fields(url: str, responses: Sequence[HttpResponse]) -> Dict[str, Any]:
    internal_statuses: List[int] = []
    warning_codes: List[str] = []
    warning_count = 0
    redirect_observed = False

    for response in responses:
        internal_statuses.extend(response.internal_fetch_statuses)
        warning_count += response.fetch_warning_count
        warning_codes.extend(response.fetch_warning_codes)
        if has_redirect_signal(url, response):
            redirect_observed = True

    return {
        "internal_fetch_statuses": summarize_internal_fetch_statuses(internal_statuses),
        "redirect_observed": "true" if redirect_observed else "false",
        "fetch_warning_count": warning_count,
        "fetch_warning_codes": "|".join(unique_preserving_order(warning_codes)),
    }


def probe_lookup_row(
    row: Mapping[str, Any],
    *,
    timeout_seconds: float = 10.0,
    user_agent: str = "Mozilla/5.0",
    probe_mode: str = "static",
    fetch_func: Optional[Callable[[str], HttpResponse]] = None,
    fetch_attempts: int = 1,
    retry_sleep_seconds: float = 0.0,
    missing_count_attempts: int = 1,
    missing_count_sleep_seconds: float = 0.0,
    resolved_at: Optional[str] = None,
) -> Dict[str, Any]:
    url = str(row.get("url") or "").strip()
    if not is_allowed_threads_url(url):
        return state_row(row, status="invalid_url", error="invalid_url", resolved_at=resolved_at)

    fetch = fetch_func or fetch_for_mode(probe_mode, timeout_seconds, user_agent)
    attempts = max(1, fetch_attempts)
    count_attempts = max(1, missing_count_attempts)
    last_error = "view_count_not_found"
    last_http_status: Any = ""
    diagnostic_responses: List[HttpResponse] = []

    for count_attempt in range(1, count_attempts + 1):
        response: Optional[HttpResponse] = None
        for attempt in range(1, attempts + 1):
            try:
                response = fetch(url)
            except Exception as exc:  # Keep one malformed row from aborting a runner shard.
                response = HttpResponse(
                    status=0,
                    body="",
                    final_url=url,
                    error=format_exception_error("fetch_exception", exc),
                )
            diagnostic_responses.append(response)
            if response.status != 0:
                break
            if attempt < attempts and retry_sleep_seconds > 0:
                time.sleep(retry_sleep_seconds)

        if response is None:
            return state_row(row, status="request_error", error="request_error", resolved_at=resolved_at)

        if response.status in RATE_LIMIT_HTTP_STATUSES:
            return state_row(
                row,
                status="rate_limited",
                http_status=response.status,
                error=response.error or "rate_limited",
                **combined_response_diagnostic_fields(url, diagnostic_responses),
                resolved_at=resolved_at,
            )
        if response.status == 0:
            return state_row(
                row,
                status="request_error",
                http_status=response.status,
                error=response.error or "request_error",
                **combined_response_diagnostic_fields(url, diagnostic_responses),
                diagnostic_event="request_error_after_fetch_retries" if attempts > 1 else "request_error",
                resolved_at=resolved_at,
            )
        if response.status < 200 or response.status >= 400:
            return state_row(
                row,
                status="http_error",
                http_status=response.status,
                error=response.error or f"http_{response.status}",
                **combined_response_diagnostic_fields(url, diagnostic_responses),
                resolved_at=resolved_at,
            )

        count = extract_view_count_from_html(response.body, url)
        last_http_status = response.status
        if count is not None:
            return state_row(
                row,
                status="resolved",
                view_count=count,
                http_status=response.status,
                **combined_response_diagnostic_fields(url, diagnostic_responses),
                diagnostic_event="resolved_after_missing_count_retry" if count_attempt > 1 else "",
                missing_count_attempt=count_attempt if count_attempt > 1 else "",
                resolved_at=resolved_at,
            )

        last_error = classify_unavailable_html(response.body)
        if last_error != "view_count_not_found":
            return state_row(
                row,
                status="unavailable",
                http_status=response.status,
                error=last_error,
                **combined_response_diagnostic_fields(url, diagnostic_responses),
                diagnostic_event="classified_unavailable",
                missing_count_attempt=count_attempt,
                resolved_at=resolved_at,
            )
        if count_attempt < count_attempts and missing_count_sleep_seconds > 0:
            time.sleep(missing_count_sleep_seconds)

    if count_attempts > 1 and last_error == "view_count_not_found":
        last_error = "view_count_not_found_after_retries"
    return state_row(
        row,
        status="unavailable",
        http_status=last_http_status,
        error=last_error,
        **combined_response_diagnostic_fields(url, diagnostic_responses),
        diagnostic_event="unavailable_after_missing_count_retries" if count_attempts > 1 else "unavailable_count_missing",
        missing_count_attempt=count_attempts,
        resolved_at=resolved_at,
    )


def probe_lookup_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    timeout_seconds: float = 10.0,
    sleep_seconds: float = 0.0,
    user_agent: str = "Mozilla/5.0",
    probe_mode: str = "static",
    fetch_func: Optional[Callable[[str], HttpResponse]] = None,
    fetch_attempts: int = 1,
    retry_sleep_seconds: float = 0.0,
    missing_count_attempts: int = 1,
    missing_count_sleep_seconds: float = 0.0,
    resolved_at: Optional[str] = None,
    stop_on_rate_limit: bool = True,
    progress_interval: int = 0,
    diagnostic_log: bool = False,
    progress_stream: Optional[TextIO] = None,
) -> List[Dict[str, Any]]:
    source_rows = list(rows)
    results: List[Dict[str, Any]] = []

    for index, row in enumerate(source_rows):
        if index > 0 and sleep_seconds > 0:
            time.sleep(sleep_seconds)
        result = probe_lookup_row(
            row,
            timeout_seconds=timeout_seconds,
            user_agent=user_agent,
            probe_mode=probe_mode,
            fetch_func=fetch_func,
            fetch_attempts=fetch_attempts,
            retry_sleep_seconds=retry_sleep_seconds,
            missing_count_attempts=missing_count_attempts,
            missing_count_sleep_seconds=missing_count_sleep_seconds,
            resolved_at=resolved_at,
        )
        results.append(result)

        if diagnostic_log and progress_stream:
            progress_stream.write(diagnostic_log_line(result) + "\n")
            progress_stream.flush()

        if result.get("view_lookup_status") == "rate_limited" and stop_on_rate_limit:
            for remaining in source_rows[index + 1:]:
                remaining_result = state_row(
                    remaining,
                    status="unexecuted_after_rate_limit",
                    error="unexecuted_after_rate_limit",
                    diagnostic_event="unexecuted_after_rate_limit",
                    resolved_at=resolved_at,
                )
                results.append(remaining_result)
                if diagnostic_log and progress_stream:
                    progress_stream.write(diagnostic_log_line(remaining_result) + "\n")
                    progress_stream.flush()
            break

        if progress_interval > 0 and progress_stream and (
            len(results) == len(source_rows) or len(results) % progress_interval == 0
        ):
            counts = status_counts(results)
            progress_stream.write(
                "view lookup progress: "
                f"processed={len(results)}/{len(source_rows)} "
                f"resolved={counts.get('resolved', 0)} "
                f"failed={len(results) - counts.get('resolved', 0)} "
                f"rate_limited={counts.get('rate_limited', 0)}\n"
            )
            progress_stream.flush()

    return results


def status_counts(rows: Iterable[Mapping[str, Any]]) -> Dict[str, int]:
    counts: Dict[str, int] = {}
    for row in rows:
        status = str(row.get("view_lookup_status") or "unknown")
        counts[status] = counts.get(status, 0) + 1
    return counts


def expand_internal_fetch_status_summary(value: str) -> List[int]:
    statuses: List[int] = []
    for part in str(value or "").split("|"):
        if not part:
            continue
        if "x" in part:
            status_text, count_text = part.split("x", 1)
            try:
                status = int(status_text)
                count = int(count_text)
            except ValueError:
                continue
            statuses.extend([status] * max(0, count))
            continue
        try:
            statuses.append(int(part))
        except ValueError:
            continue
    return statuses


def network_diagnostic_summary(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Any]:
    status_counts_by_code: Dict[str, int] = {}
    rows_with_internal_statuses = 0
    redirect_rows = 0
    warning_rows = 0

    for row in rows:
        statuses = expand_internal_fetch_status_summary(str(row.get("internal_fetch_statuses") or ""))
        if statuses:
            rows_with_internal_statuses += 1
        for status in statuses:
            status_text = str(status)
            status_counts_by_code[status_text] = status_counts_by_code.get(status_text, 0) + 1

        if str(row.get("redirect_observed") or "").lower() == "true":
            redirect_rows += 1

        try:
            warning_count = int(str(row.get("fetch_warning_count") or "0"))
        except ValueError:
            warning_count = 0
        if warning_count > 0:
            warning_rows += 1

    return {
        "rows_with_internal_fetch_statuses": rows_with_internal_statuses,
        "redirect_observed_rows": redirect_rows,
        "fetch_warning_rows": warning_rows,
        "internal_fetch_status_counts": dict(sorted(status_counts_by_code.items())),
    }


def diagnostic_log_line(row: Mapping[str, Any]) -> str:
    parts = [
        "view lookup diagnostic:",
        f"idx={row.get('idx', '')}",
        f"status={row.get('view_lookup_status', '')}",
        f"view_counts_value={row.get('view_counts_value', '') or '-'}",
        f"error={row.get('error', '') or '-'}",
        f"http_status={row.get('http_status', '') or '-'}",
        f"internal_fetch_statuses={row.get('internal_fetch_statuses', '') or '-'}",
        f"redirect_observed={row.get('redirect_observed', '') or '-'}",
        f"fetch_warning_count={row.get('fetch_warning_count', '') if row.get('fetch_warning_count', '') != '' else '-'}",
    ]
    warning_codes = str(row.get("fetch_warning_codes") or "")
    if warning_codes:
        parts.append(f"fetch_warning_codes={warning_codes}")
    event = str(row.get("_diagnostic_event") or "")
    if event:
        parts.append(f"event={event}")
    missing_count_attempt = str(row.get("_missing_count_attempt") or "")
    if missing_count_attempt:
        parts.append(f"missing_count_attempt={missing_count_attempt}")
    return " ".join(parts)


def read_map_by_idx(map_path: Path) -> Dict[str, Dict[str, Any]]:
    rows_by_idx: Dict[str, Dict[str, Any]] = {}
    for row in read_csv_rows(map_path):
        idx = str(row.get("idx") or "").strip()
        if idx and idx not in rows_by_idx:
            rows_by_idx[idx] = dict(row)
    return rows_by_idx


def state_by_idx(rows: Iterable[Mapping[str, Any]]) -> Dict[str, Dict[str, Any]]:
    values: Dict[str, Dict[str, Any]] = {}
    for row in rows:
        idx = str(row.get("idx") or "").strip()
        if idx:
            values[idx] = {field: row.get(field, "") for field in VIEW_LOOKUP_STATE_FIELDS}
    return values


def build_full_results(
    all_input_rows: Sequence[Mapping[str, Any]],
    map_by_idx: Mapping[str, Mapping[str, Any]],
    state_rows: Sequence[Mapping[str, Any]],
) -> List[Dict[str, Any]]:
    states = state_by_idx(state_rows)
    results: List[Dict[str, Any]] = []
    for input_row in all_input_rows:
        idx = str(input_row.get("idx") or "").strip()
        url = str(input_row.get("url") or "").strip()
        state = states.get(
            idx,
            state_row(
                {"idx": idx, "url": url},
                status="unexecuted_missing_result",
                error="unexecuted_missing_result",
            ),
        )
        map_row = map_by_idx.get(idx, {})
        results.append(
            {
                "idx": idx,
                "url": url,
                "lookup_id": map_row.get("lookup_id", ""),
                "export_key": map_row.get("export_key", ""),
                "match_id": map_row.get("match_id", ""),
                "role": map_row.get("role", ""),
                "target_view_field": map_row.get("target_view_field", ""),
                "threads_post_key": map_row.get("threads_post_key", ""),
                "view_counts_value": state.get("view_counts_value", ""),
                "view_lookup_status": state.get("view_lookup_status", ""),
                "http_status": state.get("http_status", ""),
                "error": state.get("error", ""),
                "internal_fetch_statuses": state.get("internal_fetch_statuses", ""),
                "redirect_observed": state.get("redirect_observed", ""),
                "fetch_warning_count": state.get("fetch_warning_count", ""),
                "fetch_warning_codes": state.get("fetch_warning_codes", ""),
                "resolved_at": state.get("resolved_at", ""),
            }
        )
    return results


def output_paths(output_dir: Path, export_key: str) -> Dict[str, Path]:
    return {
        "view_lookup_results_csv": output_dir / f"view-lookup-results-{export_key}.csv",
        "threads_viewcount_csv": output_dir / f"threads-viewcount-{export_key}.csv",
        "view_lookup_manifest_json": output_dir / f"{VIEW_LOOKUP_MANIFEST_NAME}-{export_key}.json",
    }


def merge_view_lookup_outputs(
    *,
    export_key: str,
    all_input_path: Path,
    map_path: Path,
    output_dir: Path,
    previous_final_path: Optional[Path] = None,
    shard_result_paths: Sequence[Path] = (),
    extra_manifest: Optional[Mapping[str, Any]] = None,
) -> Dict[str, Any]:
    all_input_rows = read_allowed_lookup_rows(all_input_path)
    map_rows = read_map_by_idx(map_path)
    state_rows: List[Dict[str, Any]] = []
    if previous_final_path is not None and previous_final_path.exists():
        state_rows.extend(read_state_rows(previous_final_path))
    for path in shard_result_paths:
        state_rows.extend(read_state_rows(path))

    results = build_full_results(all_input_rows, map_rows, state_rows)
    final_rows = [
        {field: row.get(field, "") for field in VIEW_LOOKUP_STATE_FIELDS}
        for row in results
        if row.get("view_lookup_status") not in DEFERRED_STATUSES
    ]
    retry_rows = [
        {field: row.get(field, "") for field in VIEW_LOOKUP_STATE_FIELDS}
        for row in results
        if row.get("view_lookup_status") in DEFERRED_STATUSES
    ]
    view_count_rows = [
        {
            "idx": row.get("idx", ""),
            "url": row.get("url", ""),
            "view_counts_value": row.get("view_counts_value", "")
            if row.get("view_lookup_status") == "resolved"
            else "",
        }
        for row in results
    ]

    paths = output_paths(output_dir, export_key)
    write_csv_rows(paths["view_lookup_results_csv"], results, VIEW_LOOKUP_RESULT_FIELDS)
    write_csv_rows(paths["threads_viewcount_csv"], view_count_rows, THREADS_VIEWCOUNT_FIELDS)

    retry_state_dir = output_dir / "retry-state"
    write_csv_rows(retry_state_dir / "all_input.csv", all_input_rows, ("idx", "url"))
    write_csv_rows(retry_state_dir / "final.csv", final_rows, VIEW_LOOKUP_STATE_FIELDS)
    write_csv_rows(
        retry_state_dir / "rate_limited_and_unexecuted.csv",
        retry_rows,
        VIEW_LOOKUP_STATE_FIELDS,
    )

    counts = status_counts(results)
    merge_result = ViewLookupMergeResult(
        export_key=export_key,
        total_rows=len(results),
        resolved_rows=counts.get("resolved", 0),
        retry_rows=len(retry_rows),
        status_counts=counts,
        results=results,
        final_rows=final_rows,
        retry_state_rows=retry_rows,
    )
    manifest: Dict[str, Any] = {
        "workflow_stage": "resolve_view_lookup",
        **merge_result.summary(),
        "network_diagnostics": network_diagnostic_summary(results),
        "source_all_input": str(all_input_path),
        "source_map": str(map_path),
        "output_files": {name: str(path) for name, path in paths.items()},
        "retry_state_dir": str(retry_state_dir),
    }
    if extra_manifest:
        manifest.update(dict(extra_manifest))
    write_json(paths["view_lookup_manifest_json"], manifest)
    return manifest


__all__ = [
    "ALLOWED_THREADS_HOSTS",
    "PROBE_MODES",
    "THREADS_VIEWCOUNT_FIELDS",
    "VIEW_LOOKUP_RESULT_FIELDS",
    "VIEW_LOOKUP_STATE_FIELDS",
    "HttpResponse",
    "build_full_results",
    "extract_view_count_from_html",
    "fetch_for_mode",
    "is_allowed_threads_url",
    "merge_view_lookup_outputs",
    "network_diagnostic_summary",
    "parse_fetch_diagnostics",
    "parse_compact_count",
    "prepare_initial_state",
    "prepare_retry_state",
    "probe_lookup_rows",
    "probe_lookup_row",
    "read_allowed_lookup_rows",
    "read_numbered_rows",
    "split_numbered_rows",
    "summarize_internal_fetch_statuses",
    "write_numbered_rows",
]
