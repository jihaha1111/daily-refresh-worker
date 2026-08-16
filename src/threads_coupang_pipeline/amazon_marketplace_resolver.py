"""Resolve approved Amazon short links into private marketplace evidence."""

from __future__ import annotations

import csv
import hashlib
import json
import os
import re
import tempfile
import time
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Sequence, TextIO, Tuple
from urllib.error import HTTPError, URLError
from urllib.parse import parse_qsl, urljoin, urlsplit, urlunsplit
from urllib.request import HTTPRedirectHandler, Request, build_opener


AMAZON_MARKETPLACE_QUEUE_VERSION = "amazon_marketplace_lookup_queue_v1"
AMAZON_MARKETPLACE_RESULT_VERSION = "amazon_marketplace_lookup_result_v1"
AMAZON_MARKETPLACE_MANIFEST_VERSION = "amazon_marketplace_lookup_manifest_v1"
AMAZON_MARKETPLACE_REPORT_VERSION = "amazon_marketplace_lookup_report_v1"
AMAZON_MARKETPLACE_RESOLVER_VERSION = "amazon_marketplace_redirect_resolver_v1"
AMAZON_SHORTLINK_INPUT_RULE_VERSION = "amazon_shortlink_exact_https_host_v1"
AMAZON_DESTINATION_RULE_VERSION = "amazon_marketplace_domain_allowlist_v1"
AMAZON_AFFILIATE_RULE_VERSION = "amazon_associates_tag_query_v1"
APPROVED_PUBLIC_RUNNER_REPOSITORY = "jihaha1111/daily-refresh-worker"

ALLOWED_AMAZON_SHORTLINK_HOSTS: Tuple[str, ...] = ("amzn.to", "link.amazon")

# Amazon's official locale reference lists these marketplace roots. Only JP and
# US receive a locale here; the remaining roots are classified but not enabled
# for collection, performance, or dashboard use by this resolver.
AMAZON_MARKETPLACE_DOMAINS: Mapping[str, Tuple[str, str]] = {
    "amazon.com.au": ("amazon_au", ""),
    "amazon.com.be": ("amazon_be", ""),
    "amazon.com.br": ("amazon_br", ""),
    "amazon.ca": ("amazon_ca", ""),
    "amazon.eg": ("amazon_eg", ""),
    "amazon.fr": ("amazon_fr", ""),
    "amazon.de": ("amazon_de", ""),
    "amazon.in": ("amazon_in", ""),
    "amazon.ie": ("amazon_ie", ""),
    "amazon.it": ("amazon_it", ""),
    "amazon.co.jp": ("amazon_jp", "ja-JP"),
    "amazon.com.mx": ("amazon_mx", ""),
    "amazon.nl": ("amazon_nl", ""),
    "amazon.pl": ("amazon_pl", ""),
    "amazon.sg": ("amazon_sg", ""),
    "amazon.sa": ("amazon_sa", ""),
    "amazon.es": ("amazon_es", ""),
    "amazon.se": ("amazon_se", ""),
    "amazon.com.tr": ("amazon_tr", ""),
    "amazon.ae": ("amazon_ae", ""),
    "amazon.co.uk": ("amazon_uk", ""),
    "amazon.com": ("amazon_us", "en-US"),
}

AMAZON_MARKETPLACE_QUEUE_FIELDS: Tuple[str, ...] = (
    "contract_version",
    "lookup_url_id",
    "short_url",
    "short_host",
    "evidence_count",
)

AMAZON_MARKETPLACE_RESULT_FIELDS: Tuple[str, ...] = (
    "contract_version",
    "resolver_version",
    "lookup_url_id",
    "resolution_status",
    "marketplace",
    "locale",
    "affiliate_program",
    "affiliate_status",
    "affiliate_id",
    "affiliate_id_param",
    "normalized_destination_url",
    "resolved_host",
    "resolved_path",
    "http_status",
    "redirect_count",
    "request_attempt_count",
    "error_code",
    "resolved_at",
)

RESOLUTION_STATUSES: Tuple[str, ...] = (
    "amazon_affiliate_id_resolved",
    "amazon_marketplace_confirmed",
    "disallowed_destination",
    "unresolved",
    "http_error",
    "network_error",
    "redirect_limit_exceeded",
    "invalid_input",
)

_AFFILIATE_ID_PATTERN = re.compile(r"^[A-Za-z0-9_-]{1,128}$")
_RUN_KEY_PATTERN = re.compile(r"^[a-z0-9][a-z0-9._-]{0,63}$")
_FORBIDDEN_PUBLIC_KEYS = {
    "run_key",
    "lookup_url_id",
    "short_url",
    "affiliate_id",
    "affiliate_id_param",
    "normalized_destination_url",
    "resolved_host",
    "resolved_path",
    "source_input",
    "github_run_id",
    "github_sha",
}


class AmazonMarketplaceResolverError(ValueError):
    """Raised when a private lookup contract fails validation."""


@dataclass(frozen=True)
class HttpHop:
    status: int
    location: str = ""
    error_code: str = ""


@dataclass(frozen=True)
class MarketplaceDestination:
    marketplace: str
    locale: str
    affiliate_id: str
    affiliate_id_param: str
    affiliate_status: str
    normalized_destination_url: str
    resolved_host: str
    resolved_path: str
    error_code: str = ""


@dataclass(frozen=True)
class ResolutionOutcome:
    resolution_status: str
    marketplace: str = ""
    locale: str = ""
    affiliate_program: str = ""
    affiliate_status: str = "not_applicable"
    affiliate_id: str = ""
    affiliate_id_param: str = ""
    normalized_destination_url: str = ""
    resolved_host: str = ""
    resolved_path: str = ""
    http_status: int = 0
    redirect_count: int = 0
    request_attempt_count: int = 0
    error_code: str = ""


@dataclass
class AmazonMarketplaceLookupResult:
    run_key: str
    rows: List[Dict[str, Any]]
    status_counts: Dict[str, int]
    marketplace_counts: Dict[str, int]
    request_attempt_count: int
    redirect_count: int

    def public_report(self) -> Dict[str, Any]:
        confirmed = sum(
            count
            for status, count in self.status_counts.items()
            if status in {
                "amazon_affiliate_id_resolved",
                "amazon_marketplace_confirmed",
            }
        )
        report: Dict[str, Any] = {
            "contract_version": AMAZON_MARKETPLACE_REPORT_VERSION,
            "mode": "resolve",
            "rules": {
                "resolver_version": AMAZON_MARKETPLACE_RESOLVER_VERSION,
                "input_host_rule_version": AMAZON_SHORTLINK_INPUT_RULE_VERSION,
                "destination_rule_version": AMAZON_DESTINATION_RULE_VERSION,
                "affiliate_rule_version": AMAZON_AFFILIATE_RULE_VERSION,
                "approved_shortlink_host_count": len(ALLOWED_AMAZON_SHORTLINK_HOSTS),
                "approved_marketplace_root_count": len(AMAZON_MARKETPLACE_DOMAINS),
            },
            "counts": {
                "input_rows": len(self.rows),
                "marketplace_confirmed_rows": confirmed,
                "japan_marketplace_rows": self.marketplace_counts.get("amazon_jp", 0),
                "united_states_marketplace_rows": self.marketplace_counts.get(
                    "amazon_us", 0
                ),
                "other_approved_marketplace_rows": sum(
                    count
                    for marketplace, count in self.marketplace_counts.items()
                    if marketplace not in {"amazon_jp", "amazon_us"}
                ),
                "affiliate_identity_resolved_rows": self.status_counts.get(
                    "amazon_affiliate_id_resolved", 0
                ),
                "status_counts": dict(sorted(self.status_counts.items())),
            },
            "network": {
                "request_attempts_performed": self.request_attempt_count,
                "redirects_observed": self.redirect_count,
                "runner_only": True,
            },
            "privacy": {
                "row_level_evidence_written_privately": True,
                "stdout_is_aggregate_only": True,
                "query_and_fragment_removed_from_destination": True,
            },
            "interpretation": {
                "marketplace_confirmation_requires_destination_allowlist": True,
                "marketplace_confirmation_does_not_require_affiliate_id": True,
                "japanese_text_alone_does_not_confirm_marketplace": True,
            },
        }
        validate_public_report(report, (), self.rows, self.run_key)
        return report


class NoRedirectHandler(HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):  # type: ignore[no-untyped-def]
        return None


def validate_run_key(value: str) -> str:
    normalized = (value or "").strip().lower()
    if not _RUN_KEY_PATTERN.fullmatch(normalized):
        raise AmazonMarketplaceResolverError(
            "Run key must use 1-64 lowercase letters, digits, dot, underscore, or hyphen"
        )
    return normalized


def validate_approved_runner_environment(environment: Mapping[str, str]) -> None:
    if environment.get("GITHUB_ACTIONS") != "true":
        raise AmazonMarketplaceResolverError(
            "Amazon network resolution is restricted to the approved GitHub Actions runner"
        )
    if environment.get("RUNNER_ENVIRONMENT") != "github-hosted":
        raise AmazonMarketplaceResolverError(
            "Amazon network resolution requires a GitHub-hosted runner"
        )
    if environment.get("GITHUB_REPOSITORY") != APPROVED_PUBLIC_RUNNER_REPOSITORY:
        raise AmazonMarketplaceResolverError(
            "Amazon network resolution is restricted to the approved public repository"
        )


def _contains_unsafe_url_characters(value: str) -> bool:
    return any(ord(character) <= 32 or character == "\ufffc" for character in value)


def _safe_parsed_url(value: str, *, allow_missing_scheme: bool) -> Optional[Any]:
    text = (value or "").strip()
    if not text or _contains_unsafe_url_characters(text):
        return None
    parsed = urlsplit(text)
    if not parsed.scheme and allow_missing_scheme:
        parsed = urlsplit("https://" + text.lstrip("/"))
    if parsed.scheme.lower() != "https":
        return None
    if parsed.username is not None or parsed.password is not None:
        return None
    try:
        port = parsed.port
    except ValueError:
        return None
    if port not in {None, 443}:
        return None
    host = (parsed.hostname or "").lower()
    if not host or host.endswith(".") or not host.isascii():
        return None
    return parsed


def normalize_amazon_short_url(value: str) -> Optional[Tuple[str, str]]:
    parsed = _safe_parsed_url(value, allow_missing_scheme=True)
    if parsed is None:
        return None
    host = (parsed.hostname or "").lower()
    if host not in ALLOWED_AMAZON_SHORTLINK_HOSTS:
        return None
    if not parsed.path or parsed.path == "/":
        return None
    normalized = urlunsplit(
        ("https", host, parsed.path, parsed.query, "")
    )
    return normalized, host


def amazon_lookup_url_id(value: str) -> str:
    normalized = normalize_amazon_short_url(value)
    if normalized is None:
        raise AmazonMarketplaceResolverError("Lookup queue contains an invalid short URL")
    digest = hashlib.sha256(normalized[0].encode("utf-8")).hexdigest()
    return f"amazon-url-sha256:{digest}"


def _marketplace_for_host(host: str) -> Optional[Tuple[str, str]]:
    for root in sorted(AMAZON_MARKETPLACE_DOMAINS, key=len, reverse=True):
        if host == root or host.endswith("." + root):
            return AMAZON_MARKETPLACE_DOMAINS[root]
    return None


def _affiliate_evidence(parsed: Any) -> Tuple[str, str, str, str]:
    values = [
        value.strip()
        for name, value in parse_qsl(parsed.query, keep_blank_values=True)
        if name.lower() == "tag" and value.strip()
    ]
    if not values:
        return "", "", "missing", ""
    unique_values = list(dict.fromkeys(values))
    if len(unique_values) != 1:
        return "", "", "invalid", "ambiguous_affiliate_tag"
    affiliate_id = unique_values[0]
    if not _AFFILIATE_ID_PATTERN.fullmatch(affiliate_id):
        return "", "", "invalid", "invalid_affiliate_tag"
    return affiliate_id, "tag", "resolved", ""


def classify_amazon_destination(value: str) -> Optional[MarketplaceDestination]:
    parsed = _safe_parsed_url(value, allow_missing_scheme=False)
    if parsed is None:
        return None
    host = (parsed.hostname or "").lower()
    marketplace = _marketplace_for_host(host)
    if marketplace is None:
        return None
    marketplace_name, locale = marketplace
    affiliate_id, affiliate_id_param, affiliate_status, error_code = (
        _affiliate_evidence(parsed)
    )
    return MarketplaceDestination(
        marketplace=marketplace_name,
        locale=locale,
        affiliate_id=affiliate_id,
        affiliate_id_param=affiliate_id_param,
        affiliate_status=affiliate_status,
        normalized_destination_url=urlunsplit(
            ("https", host, parsed.path or "/", "", "")
        ),
        resolved_host=host,
        resolved_path=parsed.path or "/",
        error_code=error_code,
    )


def http_get_no_redirect(url: str, timeout_seconds: float, user_agent: str) -> HttpHop:
    opener = build_opener(NoRedirectHandler)
    request = Request(
        url,
        headers={
            "User-Agent": user_agent,
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        },
        method="GET",
    )
    try:
        with opener.open(request, timeout=timeout_seconds) as response:
            return HttpHop(status=int(getattr(response, "status", 200)))
    except HTTPError as exc:
        if 300 <= exc.code < 400:
            return HttpHop(
                status=int(exc.code),
                location=exc.headers.get("Location", ""),
            )
        return HttpHop(status=int(exc.code), error_code=f"http_{int(exc.code)}")
    except URLError:
        return HttpHop(status=0, error_code="network_error")
    except (OSError, TimeoutError, UnicodeError, ValueError):
        return HttpHop(status=0, error_code="request_error")


def _resolved_outcome(
    destination: MarketplaceDestination,
    *,
    http_status: int,
    redirect_count: int,
    request_attempt_count: int,
) -> ResolutionOutcome:
    status = (
        "amazon_affiliate_id_resolved"
        if destination.affiliate_status == "resolved"
        else "amazon_marketplace_confirmed"
    )
    return ResolutionOutcome(
        resolution_status=status,
        marketplace=destination.marketplace,
        locale=destination.locale,
        affiliate_program="amazon_associates",
        affiliate_status=destination.affiliate_status,
        affiliate_id=destination.affiliate_id,
        affiliate_id_param=destination.affiliate_id_param,
        normalized_destination_url=destination.normalized_destination_url,
        resolved_host=destination.resolved_host,
        resolved_path=destination.resolved_path,
        http_status=http_status,
        redirect_count=redirect_count,
        request_attempt_count=request_attempt_count,
        error_code=destination.error_code,
    )


def resolve_amazon_short_url(
    value: str,
    *,
    max_redirects: int = 5,
    timeout_seconds: float = 10.0,
    fetch_attempts: int = 2,
    retry_sleep_seconds: float = 0.0,
    user_agent: str = "Mozilla/5.0",
    request_func: Optional[Callable[[str], HttpHop]] = None,
    sleep_func: Callable[[float], None] = time.sleep,
) -> ResolutionOutcome:
    normalized = normalize_amazon_short_url(value)
    if normalized is None:
        return ResolutionOutcome(
            resolution_status="invalid_input",
            error_code="invalid_short_url",
        )
    if max_redirects < 0 or fetch_attempts <= 0:
        raise AmazonMarketplaceResolverError("Resolver limits are invalid")
    if timeout_seconds <= 0 or retry_sleep_seconds < 0:
        raise AmazonMarketplaceResolverError("Resolver timing is invalid")
    if max_redirects == 0:
        return ResolutionOutcome(
            resolution_status="redirect_limit_exceeded",
            error_code="redirect_limit_exceeded",
        )

    fetch = request_func or (
        lambda next_url: http_get_no_redirect(next_url, timeout_seconds, user_agent)
    )
    current_url = normalized[0]
    total_attempts = 0
    last_status = 0

    for redirect_count in range(1, max_redirects + 1):
        hop: Optional[HttpHop] = None
        for attempt in range(1, fetch_attempts + 1):
            total_attempts += 1
            try:
                hop = fetch(current_url)
            except Exception:  # Defensive isolation for runner-only network calls.
                hop = HttpHop(status=0, error_code="request_exception")
            retryable = hop.status == 0 or 500 <= hop.status <= 599
            if not retryable or attempt == fetch_attempts:
                break
            if retry_sleep_seconds > 0:
                sleep_func(retry_sleep_seconds)

        assert hop is not None
        last_status = hop.status
        if hop.status == 0:
            return ResolutionOutcome(
                resolution_status="network_error",
                http_status=0,
                redirect_count=redirect_count - 1,
                request_attempt_count=total_attempts,
                error_code=hop.error_code or "network_error",
            )
        if hop.status >= 400:
            return ResolutionOutcome(
                resolution_status="http_error",
                http_status=hop.status,
                redirect_count=redirect_count - 1,
                request_attempt_count=total_attempts,
                error_code=hop.error_code or f"http_{hop.status}",
            )
        if not 300 <= hop.status <= 399:
            return ResolutionOutcome(
                resolution_status="unresolved",
                http_status=hop.status,
                redirect_count=redirect_count - 1,
                request_attempt_count=total_attempts,
                error_code="no_redirect_location",
            )
        location = (hop.location or "").strip()
        if not location:
            return ResolutionOutcome(
                resolution_status="unresolved",
                http_status=hop.status,
                redirect_count=redirect_count - 1,
                request_attempt_count=total_attempts,
                error_code="missing_redirect_location",
            )

        next_url = urljoin(current_url, location)
        destination = classify_amazon_destination(next_url)
        if destination is not None:
            return _resolved_outcome(
                destination,
                http_status=hop.status,
                redirect_count=redirect_count,
                request_attempt_count=total_attempts,
            )
        next_short = normalize_amazon_short_url(next_url)
        if next_short is None:
            return ResolutionOutcome(
                resolution_status="disallowed_destination",
                http_status=hop.status,
                redirect_count=redirect_count,
                request_attempt_count=total_attempts,
                error_code="destination_not_allowlisted",
            )
        current_url = next_short[0]

    return ResolutionOutcome(
        resolution_status="redirect_limit_exceeded",
        http_status=last_status,
        redirect_count=max_redirects,
        request_attempt_count=total_attempts,
        error_code="redirect_limit_exceeded",
    )


def _positive_int(value: Any, *, label: str) -> int:
    try:
        parsed = int(str(value))
    except (TypeError, ValueError) as exc:
        raise AmazonMarketplaceResolverError(f"{label} must be a positive integer") from exc
    if parsed <= 0:
        raise AmazonMarketplaceResolverError(f"{label} must be a positive integer")
    return parsed


def validate_lookup_rows(rows: Iterable[Mapping[str, Any]]) -> List[Dict[str, Any]]:
    validated: List[Dict[str, Any]] = []
    seen_ids: set[str] = set()
    for row in rows:
        if str(row.get("contract_version") or "") != AMAZON_MARKETPLACE_QUEUE_VERSION:
            raise AmazonMarketplaceResolverError("Lookup queue contract version is unsupported")
        short_url = str(row.get("short_url") or "").strip()
        normalized = normalize_amazon_short_url(short_url)
        if normalized is None:
            raise AmazonMarketplaceResolverError("Lookup queue contains an invalid short URL")
        lookup_url_id = str(row.get("lookup_url_id") or "").strip()
        if lookup_url_id != amazon_lookup_url_id(normalized[0]):
            raise AmazonMarketplaceResolverError("Lookup queue URL identity is inconsistent")
        if lookup_url_id in seen_ids:
            raise AmazonMarketplaceResolverError("Lookup queue contains a duplicate URL identity")
        if str(row.get("short_host") or "").strip().lower() != normalized[1]:
            raise AmazonMarketplaceResolverError("Lookup queue short host is inconsistent")
        evidence_count = _positive_int(row.get("evidence_count"), label="evidence_count")
        seen_ids.add(lookup_url_id)
        validated.append(
            {
                "contract_version": AMAZON_MARKETPLACE_QUEUE_VERSION,
                "lookup_url_id": lookup_url_id,
                "short_url": normalized[0],
                "short_host": normalized[1],
                "evidence_count": evidence_count,
            }
        )
    if not validated:
        raise AmazonMarketplaceResolverError("Lookup queue must contain at least one row")
    return validated


def _result_row(
    row: Mapping[str, Any],
    outcome: ResolutionOutcome,
    *,
    resolved_at: str,
) -> Dict[str, Any]:
    return {
        "contract_version": AMAZON_MARKETPLACE_RESULT_VERSION,
        "resolver_version": AMAZON_MARKETPLACE_RESOLVER_VERSION,
        "lookup_url_id": row["lookup_url_id"],
        "resolution_status": outcome.resolution_status,
        "marketplace": outcome.marketplace,
        "locale": outcome.locale,
        "affiliate_program": outcome.affiliate_program,
        "affiliate_status": outcome.affiliate_status,
        "affiliate_id": outcome.affiliate_id,
        "affiliate_id_param": outcome.affiliate_id_param,
        "normalized_destination_url": outcome.normalized_destination_url,
        "resolved_host": outcome.resolved_host,
        "resolved_path": outcome.resolved_path,
        "http_status": outcome.http_status or "",
        "redirect_count": outcome.redirect_count,
        "request_attempt_count": outcome.request_attempt_count,
        "error_code": outcome.error_code,
        "resolved_at": resolved_at,
    }


def resolve_amazon_marketplace_rows(
    rows: Iterable[Mapping[str, Any]],
    *,
    run_key: str,
    max_redirects: int = 5,
    timeout_seconds: float = 10.0,
    fetch_attempts: int = 2,
    retry_sleep_seconds: float = 0.0,
    sleep_seconds: float = 0.0,
    max_workers: int = 8,
    progress_interval: int = 25,
    progress_stream: Optional[TextIO] = None,
    user_agent: str = "Mozilla/5.0",
    request_func: Optional[Callable[[str], HttpHop]] = None,
    resolved_at: Optional[str] = None,
    sleep_func: Callable[[float], None] = time.sleep,
) -> AmazonMarketplaceLookupResult:
    normalized_run_key = validate_run_key(run_key)
    source_rows = validate_lookup_rows(rows)
    if max_workers <= 0 or progress_interval < 0:
        raise AmazonMarketplaceResolverError("Resolver concurrency settings are invalid")
    if sleep_seconds < 0:
        raise AmazonMarketplaceResolverError("Resolver sleep setting is invalid")
    timestamp = resolved_at or datetime.now(timezone.utc).isoformat()
    result_rows: List[Dict[str, Any]] = [{} for _ in source_rows]

    def resolve_indexed(indexed: Tuple[int, Mapping[str, Any]]) -> Tuple[int, Dict[str, Any]]:
        index, row = indexed
        outcome = resolve_amazon_short_url(
            str(row["short_url"]),
            max_redirects=max_redirects,
            timeout_seconds=timeout_seconds,
            fetch_attempts=fetch_attempts,
            retry_sleep_seconds=retry_sleep_seconds,
            user_agent=user_agent,
            request_func=request_func,
            sleep_func=sleep_func,
        )
        return index, _result_row(row, outcome, resolved_at=timestamp)

    completed = 0

    def record(result: Tuple[int, Dict[str, Any]]) -> None:
        nonlocal completed
        index, row = result
        result_rows[index] = row
        completed += 1
        if progress_stream and progress_interval > 0 and (
            completed == len(source_rows) or completed % progress_interval == 0
        ):
            confirmed = sum(
                1
                for current in result_rows
                if current.get("resolution_status")
                in {
                    "amazon_affiliate_id_resolved",
                    "amazon_marketplace_confirmed",
                }
            )
            progress_stream.write(
                "amazon marketplace progress: "
                f"processed={completed}/{len(source_rows)} "
                f"confirmed={confirmed} unresolved={completed - confirmed}\n"
            )
            progress_stream.flush()

    if max_workers == 1:
        for index, row in enumerate(source_rows):
            if index > 0 and sleep_seconds > 0:
                sleep_func(sleep_seconds)
            record(resolve_indexed((index, row)))
    else:
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futures = []
            for index, row in enumerate(source_rows):
                if index > 0 and sleep_seconds > 0:
                    sleep_func(sleep_seconds)
                futures.append(executor.submit(resolve_indexed, (index, row)))
            for future in as_completed(futures):
                record(future.result())

    status_counts = Counter(str(row["resolution_status"]) for row in result_rows)
    marketplace_counts = Counter(
        str(row["marketplace"]) for row in result_rows if row["marketplace"]
    )
    result = AmazonMarketplaceLookupResult(
        run_key=normalized_run_key,
        rows=result_rows,
        status_counts=dict(status_counts),
        marketplace_counts=dict(marketplace_counts),
        request_attempt_count=sum(int(row["request_attempt_count"]) for row in result_rows),
        redirect_count=sum(int(row["redirect_count"]) for row in result_rows),
    )
    validate_public_report(result.public_report(), source_rows, result_rows, normalized_run_key)
    return result


def read_amazon_marketplace_lookup_input(path: Path) -> List[Dict[str, Any]]:
    try:
        with path.open(encoding="utf-8", newline="") as handle:
            reader = csv.DictReader(handle)
            if tuple(reader.fieldnames or ()) != AMAZON_MARKETPLACE_QUEUE_FIELDS:
                raise AmazonMarketplaceResolverError("Lookup queue CSV header is invalid")
            rows = [dict(row) for row in reader]
    except UnicodeError as exc:
        raise AmazonMarketplaceResolverError("Lookup queue must be UTF-8 CSV") from exc
    except OSError as exc:
        raise AmazonMarketplaceResolverError("Lookup queue could not be opened") from exc
    return validate_lookup_rows(rows)


def _atomic_write_csv(path: Path, rows: Sequence[Mapping[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            newline="",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            writer = csv.DictWriter(handle, fieldnames=AMAZON_MARKETPLACE_RESULT_FIELDS)
            writer.writeheader()
            writer.writerows(rows)
            temp_path = Path(handle.name)
        os.chmod(temp_path, 0o600)
        os.replace(temp_path, path)
    except OSError as exc:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
        raise AmazonMarketplaceResolverError("Resolver result could not be written") from exc


def _atomic_write_json(path: Path, payload: Mapping[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    temp_path: Optional[Path] = None
    try:
        with tempfile.NamedTemporaryFile(
            mode="w",
            encoding="utf-8",
            dir=path.parent,
            prefix=f".{path.name}.",
            suffix=".tmp",
            delete=False,
        ) as handle:
            json.dump(payload, handle, ensure_ascii=False, indent=2, sort_keys=True)
            handle.write("\n")
            temp_path = Path(handle.name)
        os.chmod(temp_path, 0o600)
        os.replace(temp_path, path)
    except OSError as exc:
        if temp_path is not None:
            temp_path.unlink(missing_ok=True)
        raise AmazonMarketplaceResolverError("Resolver manifest could not be written") from exc


def validate_private_output_dir(path: Path, repo_root: Path) -> Path:
    resolved = path.expanduser().resolve()
    root = repo_root.expanduser().resolve()
    try:
        relative = resolved.relative_to(root)
    except ValueError:
        return resolved
    if not relative.parts or relative.parts[0] not in {"private", "tmp"}:
        raise AmazonMarketplaceResolverError(
            "Resolver outputs inside the repository must remain under private/ or tmp/"
        )
    return resolved


def write_amazon_marketplace_outputs(
    result: AmazonMarketplaceLookupResult,
    output_dir: Path,
    *,
    repo_root: Path,
    source_input: Path,
    max_redirects: int,
    timeout_seconds: float,
    fetch_attempts: int,
    retry_sleep_seconds: float,
    sleep_seconds: float,
    max_workers: int,
    github_run_id: Optional[str] = None,
    github_sha: Optional[str] = None,
) -> Dict[str, Any]:
    resolved_output_dir = validate_private_output_dir(output_dir, repo_root)
    result_path = resolved_output_dir / (
        f"amazon-marketplace-lookup-results-{result.run_key}.csv"
    )
    manifest_path = resolved_output_dir / (
        f"amazon-marketplace-lookup-manifest-{result.run_key}.json"
    )
    if source_input.expanduser().resolve() in {result_path, manifest_path}:
        raise AmazonMarketplaceResolverError("Resolver output must not replace its input")

    manifest: Dict[str, Any] = {
        "contract_version": AMAZON_MARKETPLACE_MANIFEST_VERSION,
        "resolver_version": AMAZON_MARKETPLACE_RESOLVER_VERSION,
        "run_key": result.run_key,
        "source_input": source_input.name,
        "resolved_at": result.rows[0]["resolved_at"] if result.rows else None,
        "settings": {
            "max_redirects": max_redirects,
            "timeout_seconds": timeout_seconds,
            "fetch_attempts": fetch_attempts,
            "retry_sleep_seconds": retry_sleep_seconds,
            "sleep_seconds": sleep_seconds,
            "max_workers": max_workers,
        },
        "counts": {
            "rows": len(result.rows),
            "status": dict(sorted(result.status_counts.items())),
            "marketplace": dict(sorted(result.marketplace_counts.items())),
            "request_attempts": result.request_attempt_count,
            "redirects": result.redirect_count,
        },
        "github": {
            "run_id": (github_run_id or "").strip() or None,
            "sha": (github_sha or "").strip() or None,
        },
        "output_files": {"results_csv": result_path.name},
    }
    _atomic_write_csv(result_path, result.rows)
    _atomic_write_json(manifest_path, manifest)
    return {
        "results_csv": result_path,
        "manifest_json": manifest_path,
        "manifest": manifest,
    }


def _has_forbidden_public_key(value: Any) -> bool:
    if isinstance(value, Mapping):
        return any(
            key in _FORBIDDEN_PUBLIC_KEYS or _has_forbidden_public_key(child)
            for key, child in value.items()
        )
    if isinstance(value, list):
        return any(_has_forbidden_public_key(child) for child in value)
    return False


def _public_scalar_strings(value: Any) -> set[str]:
    if isinstance(value, Mapping):
        result: set[str] = set()
        for child in value.values():
            result.update(_public_scalar_strings(child))
        return result
    if isinstance(value, list):
        result = set()
        for child in value:
            result.update(_public_scalar_strings(child))
        return result
    if isinstance(value, str) and value:
        return {value}
    return set()


def validate_public_report(
    report: Mapping[str, Any],
    source_rows: Iterable[Mapping[str, Any]],
    result_rows: Iterable[Mapping[str, Any]],
    run_key: str,
) -> None:
    if _has_forbidden_public_key(report):
        raise AmazonMarketplaceResolverError("Public resolver report contains a private field")
    forbidden_values: List[str] = [run_key]
    for row in source_rows:
        forbidden_values.extend(
            str(row.get(key) or "") for key in ("lookup_url_id", "short_url")
        )
    for row in result_rows:
        forbidden_values.extend(
            str(row.get(key) or "")
            for key in (
                "lookup_url_id",
                "affiliate_id",
                "normalized_destination_url",
                "resolved_host",
                "resolved_path",
            )
        )
    report_values = _public_scalar_strings(report)
    if any(value and value in report_values for value in forbidden_values):
        raise AmazonMarketplaceResolverError(
            "Public resolver report contains private evidence"
        )


__all__ = [
    "ALLOWED_AMAZON_SHORTLINK_HOSTS",
    "APPROVED_PUBLIC_RUNNER_REPOSITORY",
    "AMAZON_MARKETPLACE_DOMAINS",
    "AMAZON_MARKETPLACE_MANIFEST_VERSION",
    "AMAZON_MARKETPLACE_QUEUE_FIELDS",
    "AMAZON_MARKETPLACE_QUEUE_VERSION",
    "AMAZON_MARKETPLACE_REPORT_VERSION",
    "AMAZON_MARKETPLACE_RESOLVER_VERSION",
    "AMAZON_MARKETPLACE_RESULT_FIELDS",
    "AMAZON_MARKETPLACE_RESULT_VERSION",
    "AmazonMarketplaceLookupResult",
    "AmazonMarketplaceResolverError",
    "HttpHop",
    "amazon_lookup_url_id",
    "classify_amazon_destination",
    "normalize_amazon_short_url",
    "read_amazon_marketplace_lookup_input",
    "resolve_amazon_marketplace_rows",
    "resolve_amazon_short_url",
    "validate_lookup_rows",
    "validate_approved_runner_environment",
    "validate_private_output_dir",
    "validate_public_report",
    "validate_run_key",
    "write_amazon_marketplace_outputs",
]
