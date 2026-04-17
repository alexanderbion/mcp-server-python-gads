"""
Validation for Google Ads entities before API submission.

Each validator returns a list of error strings. Empty list = valid.
All errors are collected so Claude can fix everything in one pass.
"""

import re
from urllib.parse import urlparse

VALID_MATCH_TYPES = {"BROAD", "PHRASE", "EXACT"}

VALID_BIDDING_STRATEGIES = {
    "MANUAL_CPC",
    "MAXIMIZE_CLICKS",
    "MAXIMIZE_CONVERSIONS",
    "MAXIMIZE_CONVERSION_VALUE",
    "ENHANCED_CPC",
    "TARGET_CPA",
    "TARGET_ROAS",
}

VALID_CAMPAIGN_STATUSES = {"ENABLED", "PAUSED", "REMOVED"}

VALID_AD_GROUP_STATUSES = {"ENABLED", "PAUSED", "REMOVED"}

VALID_AD_STATUSES = {"ENABLED", "PAUSED", "REMOVED"}

VALID_NEGATIVE_SCOPES = {"CAMPAIGN", "AD_GROUP"}

VALID_SNIPPET_HEADERS = {
    "Amenities",
    "Brands",
    "Courses",
    "Degree programs",
    "Destinations",
    "Featured hotels",
    "Insurance coverage",
    "Models",
    "Neighborhoods",
    "Service catalog",
    "Shows",
    "Styles",
    "Types",
}


# ─── Primitives ─────────────────────────────────────────────────────────


def validate_url(url: str, field: str = "url") -> list[str]:
    if not url or not url.strip():
        return [f"{field}: cannot be empty"]
    u = url.strip()
    if " " in u:
        return [f"{field}: cannot contain spaces"]
    if not u.startswith(("http://", "https://")):
        u = "https://" + u
    parsed = urlparse(u)
    if not parsed.netloc or "." not in parsed.netloc:
        return [f"{field}: '{url}' is not a valid URL"]
    return []


def validate_match_type(match_type: str, field: str = "match_type") -> list[str]:
    if match_type.upper() not in VALID_MATCH_TYPES:
        return [f"{field}: '{match_type}' invalid — must be BROAD, PHRASE, or EXACT"]
    return []


def validate_bidding_strategy(strategy: str) -> list[str]:
    if strategy.upper() not in VALID_BIDDING_STRATEGIES:
        opts = ", ".join(sorted(VALID_BIDDING_STRATEGIES))
        return [f"bidding_strategy: '{strategy}' invalid — must be one of: {opts}"]
    return []


def validate_campaign_status(status: str, field: str = "status") -> list[str]:
    if status.upper() not in VALID_CAMPAIGN_STATUSES:
        opts = ", ".join(sorted(VALID_CAMPAIGN_STATUSES))
        return [f"{field}: '{status}' invalid — must be one of: {opts}"]
    return []


def validate_ad_group_status(status: str, field: str = "status") -> list[str]:
    if status.upper() not in VALID_AD_GROUP_STATUSES:
        opts = ", ".join(sorted(VALID_AD_GROUP_STATUSES))
        return [f"{field}: '{status}' invalid — must be one of: {opts}"]
    return []


def validate_ad_status(status: str, field: str = "status") -> list[str]:
    if status.upper() not in VALID_AD_STATUSES:
        opts = ", ".join(sorted(VALID_AD_STATUSES))
        return [f"{field}: '{status}' invalid — must be one of: {opts}"]
    return []


def validate_scope(scope: str, field: str = "scope") -> list[str]:
    if scope.upper() not in VALID_NEGATIVE_SCOPES:
        opts = ", ".join(sorted(VALID_NEGATIVE_SCOPES))
        return [f"{field}: '{scope}' invalid — must be one of: {opts}"]
    return []


# ─── RSA Components ─────────────────────────────────────────────────────


def validate_headlines(
    headlines: list, min_count: int = 3, max_count: int = 15
) -> list[str]:
    errors = []
    if not isinstance(headlines, list):
        return ["headlines: must be a list of strings"]
    if len(headlines) < min_count:
        errors.append(f"headlines: need at least {min_count}, got {len(headlines)}")
    if len(headlines) > max_count:
        errors.append(f"headlines: max {max_count} allowed, got {len(headlines)}")
    for i, h in enumerate(headlines):
        if not isinstance(h, str) or not h.strip():
            errors.append(f"headlines[{i}]: cannot be empty")
        elif len(h) > 30:
            errors.append(f"headlines[{i}]: \"{h}\" is {len(h)} chars (max 30)")
    return errors


def validate_descriptions(
    descriptions: list, min_count: int = 2, max_count: int = 4
) -> list[str]:
    errors = []
    if not isinstance(descriptions, list):
        return ["descriptions: must be a list of strings"]
    if len(descriptions) < min_count:
        errors.append(
            f"descriptions: need at least {min_count}, got {len(descriptions)}"
        )
    if len(descriptions) > max_count:
        errors.append(
            f"descriptions: max {max_count} allowed, got {len(descriptions)}"
        )
    for i, d in enumerate(descriptions):
        if not isinstance(d, str) or not d.strip():
            errors.append(f"descriptions[{i}]: cannot be empty")
        elif len(d) > 90:
            errors.append(
                f"descriptions[{i}]: \"{d[:50]}...\" is {len(d)} chars (max 90)"
            )
    return errors


# ─── Keywords ────────────────────────────────────────────────────────────


def validate_keywords(keywords: list, field: str = "keywords") -> list[str]:
    """Validate a list of {"text": "...", "match_type": "BROAD|PHRASE|EXACT"} objects."""
    errors = []
    if not isinstance(keywords, list):
        return [f"{field}: must be a list"]
    if len(keywords) == 0:
        return [f"{field}: must contain at least one keyword"]
    for i, kw in enumerate(keywords):
        if not isinstance(kw, dict):
            errors.append(
                f"{field}[{i}]: must be an object with 'text' and 'match_type'"
            )
            continue
        text = kw.get("text", "")
        if not text or not str(text).strip():
            errors.append(f"{field}[{i}]: 'text' is required")
        elif len(str(text)) > 80:
            errors.append(
                f"{field}[{i}]: \"{str(text)[:30]}...\" is {len(str(text))} chars (max 80)"
            )
        mt = kw.get("match_type", "")
        if not mt:
            errors.append(f"{field}[{i}]: 'match_type' is required")
        elif str(mt).upper() not in VALID_MATCH_TYPES:
            errors.append(
                f"{field}[{i}]: match_type '{mt}' invalid — must be BROAD, PHRASE, or EXACT"
            )
    return errors


def validate_negative_keyword_texts(
    keyword_texts: list, field: str = "keyword_texts"
) -> list[str]:
    """Validate a plain list of keyword text strings."""
    errors = []
    if not isinstance(keyword_texts, list) or len(keyword_texts) == 0:
        return [f"{field}: must be a non-empty list of strings"]
    for i, t in enumerate(keyword_texts):
        if not isinstance(t, str) or not t.strip():
            errors.append(f"{field}[{i}]: cannot be empty")
        elif len(t) > 80:
            errors.append(
                f"{field}[{i}]: \"{t[:30]}...\" is {len(t)} chars (max 80)"
            )
    return errors


# ─── Extensions ──────────────────────────────────────────────────────────


def validate_sitelinks(sitelinks: list, field: str = "sitelinks") -> list[str]:
    errors = []
    if not isinstance(sitelinks, list):
        return [f"{field}: must be a list"]
    for i, sl in enumerate(sitelinks):
        if not isinstance(sl, dict):
            errors.append(f"{field}[{i}]: must be an object")
            continue
        lt = sl.get("link_text", "")
        if not lt or not str(lt).strip():
            errors.append(f"{field}[{i}]: 'link_text' is required")
        elif len(lt) > 25:
            errors.append(
                f"{field}[{i}].link_text: \"{lt}\" is {len(lt)} chars (max 25)"
            )
        d1 = sl.get("description1", "")
        if d1 and len(d1) > 35:
            errors.append(
                f"{field}[{i}].description1: {len(d1)} chars (max 35)"
            )
        d2 = sl.get("description2", "")
        if d2 and len(d2) > 35:
            errors.append(
                f"{field}[{i}].description2: {len(d2)} chars (max 35)"
            )
        url = sl.get("final_url", "")
        if not url or not str(url).strip():
            errors.append(f"{field}[{i}]: 'final_url' is required")
        else:
            errors.extend(validate_url(url, f"{field}[{i}].final_url"))
    return errors


def validate_callouts(callouts: list, field: str = "callouts") -> list[str]:
    """Validate callouts — accepts both list[str] and list[{"text": "..."}]."""
    errors = []
    if not isinstance(callouts, list):
        return [f"{field}: must be a list"]
    for i, co in enumerate(callouts):
        text = co.get("text", "") if isinstance(co, dict) else co
        if not isinstance(text, str) or not text.strip():
            errors.append(f"{field}[{i}]: cannot be empty")
        elif len(text) > 25:
            errors.append(
                f"{field}[{i}]: \"{text}\" is {len(text)} chars (max 25)"
            )
    return errors


def validate_snippets(snippets: list, field: str = "structured_snippets") -> list[str]:
    errors = []
    if not isinstance(snippets, list):
        return [f"{field}: must be a list"]
    for i, sn in enumerate(snippets):
        if not isinstance(sn, dict):
            errors.append(f"{field}[{i}]: must be an object")
            continue
        header = sn.get("header", "")
        if not header:
            errors.append(f"{field}[{i}]: 'header' is required")
        elif header not in VALID_SNIPPET_HEADERS:
            errors.append(
                f"{field}[{i}]: header \"{header}\" invalid — must be one of: "
                + ", ".join(sorted(VALID_SNIPPET_HEADERS))
            )
        values = sn.get("values", [])
        if not isinstance(values, list) or len(values) < 3:
            n = len(values) if isinstance(values, list) else 0
            errors.append(f"{field}[{i}]: need at least 3 values, got {n}")
        else:
            for j, v in enumerate(values):
                if not isinstance(v, str) or not v.strip():
                    errors.append(f"{field}[{i}].values[{j}]: cannot be empty")
                elif len(v) > 25:
                    errors.append(
                        f"{field}[{i}].values[{j}]: \"{v}\" is {len(v)} chars (max 25)"
                    )
    return errors


# ─── Keyword Actions ────────────────────────────────────────────────────


VALID_KEYWORD_ACTIONS = {"PAUSED", "REMOVED"}


def validate_keyword_actions(actions: list, field: str = "actions") -> list[str]:
    errors = []
    if not isinstance(actions, list):
        return [f"{field}: must be a list"]
    if len(actions) == 0:
        return [f"{field}: must contain at least one action"]
    for i, a in enumerate(actions):
        if not isinstance(a, dict):
            errors.append(f"{field}[{i}]: must be an object")
            continue
        ag = a.get("ad_group_id", "")
        if not ag:
            errors.append(f"{field}[{i}]: 'ad_group_id' is required")
        elif not re.fullmatch(r"\d+", str(ag).replace("-", "")):
            errors.append(f"{field}[{i}]: 'ad_group_id' must be numeric")
        crit = a.get("criterion_id", "")
        if not crit:
            errors.append(f"{field}[{i}]: 'criterion_id' is required")
        elif not re.fullmatch(r"\d+", str(crit).replace("-", "")):
            errors.append(f"{field}[{i}]: 'criterion_id' must be numeric")
        action_val = str(a.get("action", "")).upper()
        if action_val not in VALID_KEYWORD_ACTIONS:
            errors.append(
                f"{field}[{i}]: action '{a.get('action')}' invalid — must be PAUSED or REMOVED"
            )
    return errors


# ─── Compound Validators ────────────────────────────────────────────────


def validate_campaign(
    *,
    campaign_name: str,
    daily_budget: float,
    url: str,
    headlines: list,
    descriptions: list,
    keywords: list,
    campaign_negative_keywords: list,
    ad_group_negative_keywords: list,
    bidding_strategy: str,
    sitelinks: list,
    callouts: list,
    snippets: list,
) -> list[str]:
    """Validate all fields for campaign creation. Returns all errors at once."""
    errors = []
    if not campaign_name or not campaign_name.strip():
        errors.append("campaign_name: cannot be empty")
    if daily_budget <= 0:
        errors.append(
            f"daily_budget: must be greater than 0, got {daily_budget}"
        )
    errors.extend(validate_url(url))
    errors.extend(validate_headlines(headlines))
    errors.extend(validate_descriptions(descriptions))
    errors.extend(validate_keywords(keywords, "keywords"))
    errors.extend(validate_bidding_strategy(bidding_strategy))
    if campaign_negative_keywords:
        errors.extend(validate_keywords(campaign_negative_keywords, "campaign_negative_keywords"))
    if ad_group_negative_keywords:
        errors.extend(validate_keywords(ad_group_negative_keywords, "ad_group_negative_keywords"))
    if sitelinks:
        errors.extend(validate_sitelinks(sitelinks))
    if callouts:
        errors.extend(validate_callouts(callouts))
    if snippets:
        errors.extend(validate_snippets(snippets))
    return errors
