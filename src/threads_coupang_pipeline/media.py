"""Deterministic media selection helpers."""

from __future__ import annotations

from typing import Any, Dict


def get_best_image_candidate(candidates: Any) -> Dict[str, Any]:
    if isinstance(candidates, list) and candidates:
        best = None
        best_score = -1
        for idx, candidate in enumerate(candidates):
            if not isinstance(candidate, dict):
                continue
            width = candidate.get("width")
            height = candidate.get("height")
            width = width if isinstance(width, int) else 0
            height = height if isinstance(height, int) else 0
            score = width * height
            if candidate.get("url") and score > best_score:
                best = dict(candidate)
                best["_candidate_index"] = idx
                best_score = score
        if best:
            return best
    return {}


def get_best_video_url(video_versions: Any) -> str:
    if isinstance(video_versions, list) and video_versions:
        best_url = ""
        best_score = -1
        for version in video_versions:
            if not isinstance(version, dict) or not version.get("url"):
                continue
            width = version.get("width")
            height = version.get("height")
            width = width if isinstance(width, int) else 0
            height = height if isinstance(height, int) else 0
            score = width * height
            if score > best_score:
                best_url = str(version.get("url") or "")
                best_score = score
        if best_url:
            return best_url
    return ""
