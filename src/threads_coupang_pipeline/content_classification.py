"""Deterministic content category helpers for matched body/link records."""

from __future__ import annotations

import re
from dataclasses import dataclass
from typing import Any, Dict, Mapping


CONTENT_CATEGORY_RECIPE = "recipe"
CONTENT_CATEGORY_GENERAL = "general"

RECIPE_CONFIDENCE_HIGH = "high"
RECIPE_CONFIDENCE_MEDIUM = "medium"
RECIPE_CONFIDENCE_NONE = "none"

CONTENT_SCOPE_ALL = "all"
CONTENT_SCOPE_NON_RECIPE = "non_recipe"
CONTENT_SCOPE_RECIPE_ONLY = "recipe_only"
CONTENT_SCOPE_CHOICES = (
    CONTENT_SCOPE_ALL,
    CONTENT_SCOPE_NON_RECIPE,
    CONTENT_SCOPE_RECIPE_ONLY,
)

_MANUAL_RECIPE_VALUES = {"recipe", "recipes", "레시피", "요리", "식단/레시피"}
_MANUAL_GENERAL_VALUES = {"general", "non_recipe", "none", "일반", "비레시피"}

_EXPLICIT_RECIPE_RE = re.compile(
    r"레시피|조리\s*순서|조리법|조리\s*방법|요리\s*법|만드는\s*법|만드는법|"
    r"준비물|재료\s*[:：]|\b재료\b|양념\s*[:：]|소스\s*[:：]",
    re.IGNORECASE,
)

_COOKING_SIGNAL_RE = re.compile(
    r"큰술|작은술|스푼|티스푼|밥숟갈|밥숟가락|에어프라이어|에프\b|전자레인지|"
    r"냄비|프라이팬|팬에|중불|약불|강불|구워|굽고|볶아|볶기|삶아|끓여|"
    r"쪄|섞어|섞고|썰어|썰고|담아|뿌려|데우|익히|완성|"
    r"\d+\s*(?:g|그램|ml|미리|분|초|큰술|작은술|스푼)",
    re.IGNORECASE,
)

_FOOD_TERM_RE = re.compile(
    "|".join(
        re.escape(term)
        for term in [
            "요거트",
            "계란",
            "달걀",
            "소고기",
            "돼지고기",
            "닭가슴살",
            "닭",
            "배추",
            "참외",
            "단호박",
            "고구마",
            "두부",
            "김치",
            "밥",
            "면",
            "파스타",
            "샐러드",
            "소스",
            "양념",
            "마늘",
            "후추",
            "알룰로스",
            "올리브유",
            "바게트",
            "치즈",
            "라면",
            "만두",
            "떡",
            "오이",
            "토마토",
            "아보카도",
            "감자",
            "양배추",
            "현미",
            "곤약",
            "두유",
            "프로틴",
            "그릭",
        ]
    ),
    re.IGNORECASE,
)

_PROCEDURE_RE = re.compile(
    r"\d+\s*[.)️⃣]|먼저|다음|마지막|넣고|넣어|섞고|섞어|자른|썰|굽|볶|끓|삶|"
    r"데우|익히|올려|뿌려|완성",
    re.IGNORECASE,
)


@dataclass(frozen=True)
class ContentClassification:
    content_category: str
    is_recipe: bool
    recipe_confidence: str


def _manual_category(row: Mapping[str, Any]) -> str:
    for key in ("manual_content_category", "content_category_override", "category_tag"):
        value = str(row.get(key, "") or "").strip().lower()
        if value:
            return value
    return ""


def _combined_text(row: Mapping[str, Any]) -> str:
    return " ".join(
        str(row.get(field, "") or "")
        for field in ("body_text", "body_text_lines", "link_text", "link_text_lines")
    )


def classify_match_content(row: Mapping[str, Any]) -> ContentClassification:
    """Classify a matched post as recipe/general using a deterministic heuristic."""

    manual = _manual_category(row)
    if manual in _MANUAL_RECIPE_VALUES:
        return ContentClassification(
            content_category=CONTENT_CATEGORY_RECIPE,
            is_recipe=True,
            recipe_confidence=RECIPE_CONFIDENCE_HIGH,
        )
    if manual in _MANUAL_GENERAL_VALUES:
        return ContentClassification(
            content_category=CONTENT_CATEGORY_GENERAL,
            is_recipe=False,
            recipe_confidence=RECIPE_CONFIDENCE_NONE,
        )

    text = _combined_text(row)
    if _EXPLICIT_RECIPE_RE.search(text):
        return ContentClassification(
            content_category=CONTENT_CATEGORY_RECIPE,
            is_recipe=True,
            recipe_confidence=RECIPE_CONFIDENCE_HIGH,
        )

    cooking_signals = _COOKING_SIGNAL_RE.findall(text)
    if len(cooking_signals) >= 3:
        return ContentClassification(
            content_category=CONTENT_CATEGORY_RECIPE,
            is_recipe=True,
            recipe_confidence=RECIPE_CONFIDENCE_HIGH,
        )

    if _FOOD_TERM_RE.search(text) and _PROCEDURE_RE.search(text):
        return ContentClassification(
            content_category=CONTENT_CATEGORY_RECIPE,
            is_recipe=True,
            recipe_confidence=RECIPE_CONFIDENCE_MEDIUM,
        )

    return ContentClassification(
        content_category=CONTENT_CATEGORY_GENERAL,
        is_recipe=False,
        recipe_confidence=RECIPE_CONFIDENCE_NONE,
    )


def add_content_classification(row: Mapping[str, Any]) -> Dict[str, Any]:
    classified = classify_match_content(row)
    output = dict(row)
    output.update(
        {
            "content_category": classified.content_category,
            "is_recipe": classified.is_recipe,
            "recipe_confidence": classified.recipe_confidence,
        }
    )
    return output


def row_matches_content_scope(row: Mapping[str, Any], content_scope: str) -> bool:
    if content_scope == CONTENT_SCOPE_ALL:
        return True

    is_recipe = row.get("is_recipe")
    if is_recipe is None or is_recipe == "":
        is_recipe = classify_match_content(row).is_recipe
    elif isinstance(is_recipe, str):
        is_recipe = is_recipe.strip().lower() in {"true", "t", "1", "yes", "y"}
    else:
        is_recipe = bool(is_recipe)

    if content_scope == CONTENT_SCOPE_NON_RECIPE:
        return not is_recipe
    if content_scope == CONTENT_SCOPE_RECIPE_ONLY:
        return bool(is_recipe)
    raise ValueError(f"Unsupported content_scope: {content_scope}")
