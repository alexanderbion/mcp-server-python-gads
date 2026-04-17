"""
MCP Server for Google Ads campaign management.
Exposes tools for Claude to create, read, and edit Google Search campaigns
using the Google Ads API. Deployable on Render with bearer token auth.
"""

import json
import re
import asyncio
import logging
import functools
import os

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.applications import Starlette
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.routing import Mount, Route
from google.ads.googleads.errors import GoogleAdsException

from validators import (
    validate_campaign,
    validate_headlines,
    validate_descriptions,
    validate_keywords,
    validate_match_type,
    validate_bidding_strategy,
    validate_sitelinks,
    validate_callouts,
    validate_snippets,
    validate_negative_keyword_texts,
    validate_keyword_actions,
    validate_campaign_status,
    validate_ad_group_status,
    validate_ad_status,
    validate_scope,
)

from services.google_ads import (
    get_google_ads_client,
    list_accessible_customers,
    list_campaigns as gads_list_campaigns,
    list_ad_groups as gads_list_ad_groups,
    get_ad_group_ads,
    get_ad_group_keywords,
    get_campaign_extensions as gads_get_extensions,
    get_campaign_targeting as gads_get_targeting,
    create_paused_campaign,
    add_keywords_to_ad_group as gads_add_keywords,
    update_rsa_ad as gads_update_rsa,
    pause_keywords as gads_pause_keywords,
    get_top_keywords_by_cost,
    get_top_search_terms_by_cost,
    get_campaign_negative_keywords,
    get_ad_group_negative_keywords as gads_get_ag_negatives,
    add_ad_group as gads_add_ad_group,
    create_rsa_ad as gads_create_rsa,
    clone_campaign as gads_clone_campaign,
    update_campaign as gads_update_campaign,
    update_targeting as gads_update_targeting,
    add_extensions_to_campaign as gads_add_extensions,
    add_negatives as gads_add_negatives,
)

# ─── Environment ───────────────────────────────────────────────────────

MCP_API_TOKEN = os.environ.get("MCP_API_TOKEN")
RENDER_EXTERNAL_HOSTNAME = os.environ.get("RENDER_EXTERNAL_HOSTNAME")

# ─── FastMCP instance ──────────────────────────────────────────────────

mcp = FastMCP(
    "prime-ads",
    transport_security=TransportSecuritySettings(
        enable_dns_rebinding_protection=bool(RENDER_EXTERNAL_HOSTNAME),
        allowed_hosts=[RENDER_EXTERNAL_HOSTNAME] if RENDER_EXTERNAL_HOSTNAME else [],
    ),
    instructions="""Google Ads campaign management server for Search campaigns.

## Entity hierarchy
Account → Campaign → Ad Group → Ads & Keywords.
Every write tool needs a customer_id (account). Most also need a campaign_id or ad_group_id.

## Typical workflows
- **Browse**: list_accounts → list_campaigns → list_ad_groups → get_ads / get_keywords
- **Create**: create_campaign (everything created PAUSED) → add_ad_group / create_ad for more
- **Clone**: clone_campaign copies an existing campaign with optional overrides (name, budget, geos, languages, bidding strategy)
- **Edit**: update_campaign (name/budget/status/bidding), update_targeting (geos/languages), update_ad (RSA copy)
- **Add to existing**: add_extensions (sitelinks/callouts/snippets), add_negatives (campaign or ad-group), add_keywords
- **Reduce waste**: get_keyword_performance / get_search_term_performance → add_negatives / modify_keyword_status

## RSA limits
Headlines: min 3, max 15, each ≤30 chars. Descriptions: min 2, max 4, each ≤90 chars.
Sitelink text ≤25 chars, sitelink descriptions ≤35 chars. Callouts ≤25 chars.

## Important
- create_campaign / clone_campaign / add_ad_group default to PAUSED — nothing goes live automatically.
- update_ad REPLACES the ad: RSAs are immutable, so a new ad is created with the merged content and the old one is removed. The returned new_ad_id replaces the old ad_id. Always fetch the current ad first and merge.
- create_ad ADDS a new RSA alongside existing ones — use this for A/B ad testing.
- customer_id accepts dashes (e.g. '123-456-7890') — they are stripped automatically.
- daily_budget is in the account's local currency (not always USD). EUR for most EU accounts.
""",
)

logger = logging.getLogger("prime-ads")


# ─── Health check ──────────────────────────────────────────────────────


async def health(request: Request) -> Response:
    return JSONResponse({"status": "ok"})


# ─── ASGI app factory ─────────────────────────────────────────────────


def create_app():
    # Use sse_app() — Claude Web connectors talk to MCP over SSE.
    # We must explicitly disable FastMCP's environment-based bearer token middleware
    # because it causes 401 Unauthorized for Claude Web. We authenticate via secret path instead.
    original_token = os.environ.pop("MCP_API_TOKEN", None)
    mcp_app = mcp.sse_app()
    if original_token is not None:
        os.environ["MCP_API_TOKEN"] = original_token

    # Mount the MCP server behind a hard-to-guess secret path.
    # Claude Web connectors drop query params and don't send static bearer
    # tokens on subsequent POSTs, so the secret URL path is the auth mechanism.
    secret_path = "/mcp-primeads-secure-proxy-829xyz"

    return Starlette(routes=[
        Route("/health", health, methods=["GET"]),
        Mount(secret_path, app=mcp_app),
    ])


# ─── Helpers ────────────────────────────────────────────────────────────


def _clean(value: str, name: str) -> str:
    """Validate and clean a numeric ID (strips dashes)."""
    cleaned = value.replace("-", "")
    if not re.fullmatch(r"\d+", cleaned):
        raise ValueError(f"Invalid {name}: must be numeric")
    return cleaned


def _client():
    return get_google_ads_client()


def _json(obj) -> str:
    """Serialize to indented JSON, handling Pydantic models."""
    if hasattr(obj, "model_dump"):
        obj = obj.model_dump()
    return json.dumps(obj, indent=2, default=str)


def _parse_json(raw: str, param_name: str) -> list | dict:
    """Parse a JSON string with a clear error on failure."""
    try:
        return json.loads(raw)
    except json.JSONDecodeError as e:
        raise ValueError(f"Invalid JSON in {param_name}: {e.msg} at position {e.pos}")


def _check(errors: list[str]) -> str | None:
    """If there are validation errors, return them as JSON. Otherwise None."""
    if errors:
        return _json({"validation_errors": errors})
    return None


def _format_google_ads_error(exc: GoogleAdsException) -> str:
    """Extract a readable error message from a GoogleAdsException."""
    errors = []
    for error in exc.failure.errors:
        msg = error.message
        if hasattr(error, "details") and error.details:
            if hasattr(error.details, "policy_violation_details") and error.details.policy_violation_details:
                v = error.details.policy_violation_details
                msg = f"Policy violation: {v.external_policy_name}"
                if hasattr(v, "key") and v.key and v.key.violating_text:
                    msg += f' (violating text: "{v.key.violating_text}")'
        errors.append(msg)
    return "; ".join(errors) if errors else str(exc)


def _handle_errors(fn):
    """Decorator that catches Google Ads and validation errors and returns structured error JSON."""
    @functools.wraps(fn)
    async def wrapper(*args, **kwargs):
        try:
            return await fn(*args, **kwargs)
        except GoogleAdsException as exc:
            detail = _format_google_ads_error(exc)
            logger.error("Google Ads API error in %s: %s", fn.__name__, detail)
            return _json({"error": detail, "request_id": exc.request_id})
        except (ValueError, json.JSONDecodeError) as exc:
            return _json({"error": str(exc)})
        except Exception as exc:
            logger.exception("Unexpected error in %s", fn.__name__)
            return _json({"error": f"Unexpected error: {exc}"})
    return wrapper


# ═════════════════════════════════════════════════════════════════════════
# READ TOOLS
# ═════════════════════════════════════════════════════════════════════════


@mcp.tool()
@_handle_errors
async def list_accounts() -> str:
    """List all Google Ads accounts accessible under the configured MCC manager account.
    Returns account IDs and names. Call this first to find the customer_id needed by other tools."""
    client = _client()
    accounts = await asyncio.to_thread(list_accessible_customers, client)
    if not accounts:
        return "No accessible accounts found. Check GOOGLE_ADS_LOGIN_CUSTOMER_ID in .env."
    return _json(accounts)


@mcp.tool()
@_handle_errors
async def list_campaigns(customer_id: str) -> str:
    """List all non-removed SEARCH campaigns for an account.

    Args:
        customer_id: Google Ads customer ID (dashes optional, e.g. '123-456-7890')
    """
    cid = _clean(customer_id, "customer_id")
    campaigns = await asyncio.to_thread(gads_list_campaigns, _client(), cid)
    if not campaigns:
        return "No search campaigns found for this account."
    return _json(campaigns)


@mcp.tool()
@_handle_errors
async def list_ad_groups(customer_id: str, campaign_id: str) -> str:
    """List all ad groups in a campaign.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: The campaign to list ad groups for
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")
    groups = await asyncio.to_thread(gads_list_ad_groups, _client(), cid, camp)
    return _json(groups)


@mcp.tool()
@_handle_errors
async def get_ads(customer_id: str, ad_group_id: str) -> str:
    """Get all RSA (Responsive Search Ad) ads in an ad group.
    Returns headlines, descriptions, final URLs, and status for each ad.

    Args:
        customer_id: Google Ads customer ID
        ad_group_id: The ad group to get ads from
    """
    cid = _clean(customer_id, "customer_id")
    ag = _clean(ad_group_id, "ad_group_id")
    ads = await asyncio.to_thread(get_ad_group_ads, _client(), cid, ag)
    return _json(ads)


@mcp.tool()
@_handle_errors
async def get_keywords(customer_id: str, ad_group_id: str) -> str:
    """Get all keywords (positive and negative) for an ad group.

    Args:
        customer_id: Google Ads customer ID
        ad_group_id: The ad group to get keywords from
    """
    cid = _clean(customer_id, "customer_id")
    ag = _clean(ad_group_id, "ad_group_id")
    keywords = await asyncio.to_thread(get_ad_group_keywords, _client(), cid, ag)
    return _json(keywords)


@mcp.tool()
@_handle_errors
async def get_extensions(customer_id: str, campaign_id: str) -> str:
    """Get sitelink, callout, and structured snippet extensions for a campaign.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: The campaign to get extensions for
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")
    ext = await asyncio.to_thread(gads_get_extensions, _client(), cid, camp)
    return _json(ext)


@mcp.tool()
@_handle_errors
async def get_targeting(customer_id: str, campaign_id: str) -> str:
    """Get geo-location and language targeting settings for a campaign.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: The campaign to get targeting for
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")
    targeting = await asyncio.to_thread(gads_get_targeting, _client(), cid, camp)
    return _json(targeting)


@mcp.tool()
@_handle_errors
async def get_keyword_performance(customer_id: str, campaign_id: str) -> str:
    """Get the top 50 keywords by cost for a campaign over the last 30 days.
    Returns impressions, clicks, CTR, conversions, and cost for each keyword.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to analyze
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")
    data = await asyncio.to_thread(get_top_keywords_by_cost, _client(), cid, camp)
    return _json(data)


@mcp.tool()
@_handle_errors
async def get_search_term_performance(customer_id: str, campaign_id: str) -> str:
    """Get the top 50 search terms by cost for a campaign over the last 30 days.
    Shows what users actually searched for when your ads appeared.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to analyze
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")
    data = await asyncio.to_thread(get_top_search_terms_by_cost, _client(), cid, camp)
    return _json(data)


@mcp.tool()
@_handle_errors
async def get_negative_keywords(customer_id: str, campaign_id: str) -> str:
    """Get all negative keywords for a campaign, at both campaign and ad-group level.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to get negatives for
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")
    client = _client()
    camp_negs, ag_negs = await asyncio.gather(
        asyncio.to_thread(get_campaign_negative_keywords, client, cid, camp),
        asyncio.to_thread(gads_get_ag_negatives, client, cid, camp),
    )
    return _json({"campaign_level": camp_negs, "ad_group_level": ag_negs})


@mcp.tool()
@_handle_errors
async def load_full_campaign(customer_id: str, campaign_id: str, ad_group_id: str) -> str:
    """Load all data for a campaign in one call: ads, keywords, extensions, and targeting.
    Useful for getting a complete picture before making changes.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign ID
        ad_group_id: Ad group ID to load ads and keywords from
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")
    ag = _clean(ad_group_id, "ad_group_id")
    client = _client()

    ads, keywords, extensions, targeting = await asyncio.gather(
        asyncio.to_thread(get_ad_group_ads, client, cid, ag),
        asyncio.to_thread(get_ad_group_keywords, client, cid, ag),
        asyncio.to_thread(gads_get_extensions, client, cid, camp),
        asyncio.to_thread(gads_get_targeting, client, cid, camp),
    )
    return _json({
        "ads": ads,
        "keywords": keywords,
        "extensions": extensions,
        "targeting": targeting,
    })


# ═════════════════════════════════════════════════════════════════════════
# WRITE TOOLS — these modify your Google Ads account
# ═════════════════════════════════════════════════════════════════════════


def _normalize_id_list(value, field: str) -> list[int]:
    """Accept either an int or a list[int] and return list[int]. Raises ValueError otherwise."""
    if value is None:
        return []
    if isinstance(value, int):
        return [value]
    if isinstance(value, list):
        out = []
        for v in value:
            try:
                out.append(int(v))
            except (TypeError, ValueError):
                raise ValueError(f"{field}: '{v}' is not a valid integer ID")
        return out
    raise ValueError(f"{field}: must be an int or a list of ints")


@mcp.tool()
@_handle_errors
async def create_campaign(
    customer_id: str,
    campaign_name: str,
    daily_budget: float,
    url: str,
    headlines: list[str],
    descriptions: list[str],
    keywords_json: str,
    campaign_negative_keywords_json: str = "[]",
    ad_group_negative_keywords_json: str = "[]",
    negative_keywords_json: str = "[]",
    bidding_strategy: str = "MANUAL_CPC",
    target_cpa: float | None = None,
    target_roas: float | None = None,
    language_ids: list[int] | int = 1000,
    geo_target_ids: list[int] | int = 2840,
    sitelinks_json: str = "[]",
    callouts_json: str = "[]",
    snippets_json: str = "[]",
) -> str:
    """Create a complete paused Google Search campaign with ad group, RSA ad, keywords, and extensions.
    The campaign and all components are created in PAUSED status.

    Args:
        customer_id: Google Ads customer ID
        campaign_name: Name for the new campaign
        daily_budget: Daily budget in the account's local currency (USD/EUR/etc.) e.g. 10.0
        url: Final URL for the ads (e.g. 'https://example.com')
        headlines: List of ad headlines (3-15 headlines, each max 30 chars)
        descriptions: List of ad descriptions (2-4 descriptions, each max 90 chars)
        keywords_json: JSON array of keywords, each {"text": "...", "match_type": "BROAD|PHRASE|EXACT"}
        campaign_negative_keywords_json: JSON array of negatives written at CAMPAIGN level (apply to all ad groups). Same format as keywords_json.
        ad_group_negative_keywords_json: JSON array of negatives written at AD GROUP level (apply only to the main ad group created here). Same format as keywords_json.
        negative_keywords_json: DEPRECATED — kept for backwards compatibility. Maps to ad_group_negative_keywords_json.
        bidding_strategy: One of MANUAL_CPC, MAXIMIZE_CLICKS, MAXIMIZE_CONVERSIONS, MAXIMIZE_CONVERSION_VALUE, TARGET_CPA, TARGET_ROAS
        target_cpa: Required when bidding_strategy=TARGET_CPA. Local currency.
        target_roas: Required when bidding_strategy=TARGET_ROAS. Decimal e.g. 4.0 = 400% ROAS.
        language_ids: Single int or list of language constants (1000=English, 1001=French, 1002=German, 1003=Spanish, 1004=Italian, 1005=Russian, 1009=Portuguese, 1015=Dutch, 1030=Polish, 1037=Turkish)
        geo_target_ids: Single int or list of geo constants (2840=US, 2826=UK, 2124=Canada, 2276=Germany, 2250=France, 2724=Spain, 2380=Italy, 2528=Netherlands, 2616=Poland, 2484=Mexico, 2076=Brazil, 2792=Turkey, 2040=Austria, 2203=Czechia, 2642=Romania, 2056=Belgium, 2756=Switzerland, 2032=Argentina, 2152=Chile, 2170=Colombia, 2604=Peru)
        sitelinks_json: JSON array of sitelinks, each {"link_text": "...", "description1": "...", "description2": "...", "final_url": "..."}
        callouts_json: JSON array of callouts, each {"text": "..."}
        snippets_json: JSON array of structured snippets, each {"header": "Types|Brands|...", "values": ["a","b","c"]}
    """
    cid = _clean(customer_id, "customer_id")

    keywords = _parse_json(keywords_json, "keywords_json")
    campaign_negs = _parse_json(campaign_negative_keywords_json, "campaign_negative_keywords_json")
    ad_group_negs = _parse_json(ad_group_negative_keywords_json, "ad_group_negative_keywords_json")
    legacy_negs = _parse_json(negative_keywords_json, "negative_keywords_json")
    if legacy_negs and not ad_group_negs:
        ad_group_negs = legacy_negs  # backwards compat

    sitelinks = _parse_json(sitelinks_json, "sitelinks_json")
    callouts = _parse_json(callouts_json, "callouts_json")
    snippets = _parse_json(snippets_json, "snippets_json")

    geos = _normalize_id_list(geo_target_ids, "geo_target_ids")
    langs = _normalize_id_list(language_ids, "language_ids")

    errors = validate_campaign(
        campaign_name=campaign_name,
        daily_budget=daily_budget,
        url=url,
        headlines=headlines,
        descriptions=descriptions,
        keywords=keywords,
        campaign_negative_keywords=campaign_negs,
        ad_group_negative_keywords=ad_group_negs,
        bidding_strategy=bidding_strategy,
        sitelinks=sitelinks,
        callouts=callouts,
        snippets=snippets,
    )
    if not geos:
        errors.append("geo_target_ids: must contain at least one ID")
    if not langs:
        errors.append("language_ids: must contain at least one ID")
    if bidding_strategy.upper() == "TARGET_CPA" and target_cpa is None:
        errors.append("target_cpa: required when bidding_strategy=TARGET_CPA")
    if bidding_strategy.upper() == "TARGET_ROAS" and target_roas is None:
        errors.append("target_roas: required when bidding_strategy=TARGET_ROAS")
    failed = _check(errors)
    if failed:
        return failed

    budget_micros = int(round(daily_budget * 1_000_000))

    resource = await asyncio.to_thread(
        create_paused_campaign,
        _client(),
        cid,
        campaign_name,
        budget_micros,
        headlines,
        descriptions,
        keywords,
        campaign_negs,
        ad_group_negs,
        langs,
        geos,
        url,
        bidding_strategy,
        target_cpa,
        target_roas,
        sitelinks,
        callouts,
        snippets,
    )
    return _json({
        "status": "success",
        "resource_name": resource,
        "message": (
            f"Campaign '{campaign_name}' created in PAUSED status with "
            f"{len(keywords)} keywords, {len(headlines)} headlines, "
            f"{len(descriptions)} descriptions, "
            f"{len(campaign_negs)} campaign-level + {len(ad_group_negs)} ad-group-level negatives, "
            f"targeting {len(geos)} geo(s) and {len(langs)} language(s)."
        ),
    })


@mcp.tool()
@_handle_errors
async def add_keywords(
    customer_id: str,
    ad_group_id: str,
    keywords_json: str,
) -> str:
    """Add keywords to an existing ad group.

    Args:
        customer_id: Google Ads customer ID
        ad_group_id: Ad group to add keywords to
        keywords_json: JSON array of keywords, each {"text": "...", "match_type": "BROAD|PHRASE|EXACT"}. Add "negative": true for negative keywords.
    """
    cid = _clean(customer_id, "customer_id")
    ag = _clean(ad_group_id, "ad_group_id")
    keywords = _parse_json(keywords_json, "keywords_json")

    failed = _check(validate_keywords(keywords))
    if failed:
        return failed

    ag_resource = f"customers/{cid}/adGroups/{ag}"
    await asyncio.to_thread(gads_add_keywords, _client(), cid, ag_resource, keywords)
    return _json({"status": "success", "message": f"Added {len(keywords)} keywords to ad group {ag}"})


@mcp.tool()
@_handle_errors
async def add_negatives(
    customer_id: str,
    scope: str,
    parent_id: str,
    keyword_texts: list[str],
    match_type: str = "PHRASE",
) -> str:
    """Add negative keywords at either CAMPAIGN or AD_GROUP scope.

    Args:
        customer_id: Google Ads customer ID
        scope: 'CAMPAIGN' (negatives apply to all ad groups in the campaign) or 'AD_GROUP' (only this ad group)
        parent_id: campaign_id when scope=CAMPAIGN, ad_group_id when scope=AD_GROUP
        keyword_texts: List of keyword texts to exclude (e.g. ['free', 'cheap', 'diy'])
        match_type: BROAD, PHRASE, or EXACT
    """
    cid = _clean(customer_id, "customer_id")
    pid = _clean(parent_id, "parent_id")

    errors = validate_scope(scope)
    errors.extend(validate_negative_keyword_texts(keyword_texts))
    errors.extend(validate_match_type(match_type))
    failed = _check(errors)
    if failed:
        return failed

    result = await asyncio.to_thread(
        gads_add_negatives, _client(), cid, scope.upper(), pid,
        keyword_texts, match_type.upper(),
    )
    return _json({"status": "success", **result})


@mcp.tool()
@_handle_errors
async def add_extensions(
    customer_id: str,
    campaign_id: str,
    callouts_json: str = "[]",
    sitelinks_json: str = "[]",
    snippets_json: str = "[]",
) -> str:
    """Add any combination of callouts, sitelinks, and structured snippets to a campaign in one call.

    At least one of the three lists must be non-empty.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to add extensions to
        callouts_json: JSON array of callouts, each {"text": "..." (max 25 chars)} OR a JSON array of plain strings
        sitelinks_json: JSON array of sitelinks, each {"link_text": "..." (max 25), "description1": "..." (max 35), "description2": "..." (max 35), "final_url": "https://..."}
        snippets_json: JSON array of structured snippets, each {"header": "Types|Brands|...", "values": ["v1","v2","v3"]}. Valid headers: Amenities, Brands, Courses, Degree programs, Destinations, Featured hotels, Insurance coverage, Models, Neighborhoods, Service catalog, Shows, Styles, Types.
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")

    callouts_raw = _parse_json(callouts_json, "callouts_json")
    sitelinks = _parse_json(sitelinks_json, "sitelinks_json")
    snippets = _parse_json(snippets_json, "snippets_json")

    callouts = [{"text": c} if isinstance(c, str) else c for c in callouts_raw]

    if not callouts and not sitelinks and not snippets:
        return _json({"error": "Provide at least one of callouts_json, sitelinks_json, or snippets_json"})

    errors: list[str] = []
    if callouts:
        errors.extend(validate_callouts(callouts))
    if sitelinks:
        errors.extend(validate_sitelinks(sitelinks))
    if snippets:
        errors.extend(validate_snippets(snippets))
    failed = _check(errors)
    if failed:
        return failed

    counts = await asyncio.to_thread(
        gads_add_extensions, _client(), cid, camp,
        sitelinks, callouts, snippets,
    )
    return _json({"status": "success", "added": counts, "campaign_id": camp})


@mcp.tool()
@_handle_errors
async def update_campaign(
    customer_id: str,
    campaign_id: str,
    name: str | None = None,
    status: str | None = None,
    daily_budget: float | None = None,
    bidding_strategy: str | None = None,
    target_cpa: float | None = None,
    target_roas: float | None = None,
) -> str:
    """Update one or more fields on an existing campaign.

    Pass only the fields you want to change. At least one of
    name/status/daily_budget/bidding_strategy must be provided.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to update
        name: New campaign name
        status: 'ENABLED' | 'PAUSED' | 'REMOVED'
        daily_budget: New daily budget in account local currency (e.g. 5.0)
        bidding_strategy: One of MANUAL_CPC, MAXIMIZE_CLICKS, MAXIMIZE_CONVERSIONS, MAXIMIZE_CONVERSION_VALUE, TARGET_CPA, TARGET_ROAS
        target_cpa: Required when bidding_strategy=TARGET_CPA. Local currency.
        target_roas: Required when bidding_strategy=TARGET_ROAS. Decimal e.g. 4.0 = 400% ROAS.
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")

    if name is None and status is None and daily_budget is None and bidding_strategy is None:
        return _json({"error": "Provide at least one of: name, status, daily_budget, bidding_strategy"})

    errors: list[str] = []
    if status is not None:
        errors.extend(validate_campaign_status(status))
    if bidding_strategy is not None:
        errors.extend(validate_bidding_strategy(bidding_strategy))
        if bidding_strategy.upper() == "TARGET_CPA" and target_cpa is None:
            errors.append("target_cpa: required when bidding_strategy=TARGET_CPA")
        if bidding_strategy.upper() == "TARGET_ROAS" and target_roas is None:
            errors.append("target_roas: required when bidding_strategy=TARGET_ROAS")
    if daily_budget is not None and daily_budget <= 0:
        errors.append(f"daily_budget: must be greater than 0, got {daily_budget}")
    if name is not None and not name.strip():
        errors.append("name: cannot be empty")
    failed = _check(errors)
    if failed:
        return failed

    result = await asyncio.to_thread(
        gads_update_campaign, _client(), cid, camp,
        name, status.upper() if status else None,
        daily_budget,
        bidding_strategy.upper() if bidding_strategy else None,
        target_cpa, target_roas,
    )
    return _json({"status": "success", **result})


@mcp.tool()
@_handle_errors
async def update_targeting(
    customer_id: str,
    campaign_id: str,
    add_geo_target_ids: list[int] | None = None,
    remove_geo_target_ids: list[int] | None = None,
    exclude_geo_target_ids: list[int] | None = None,
    add_language_ids: list[int] | None = None,
    remove_language_ids: list[int] | None = None,
) -> str:
    """Edit a campaign's geo and language targeting.

    Excluded geos are added as negative LocationInfo criteria — equivalent to
    'Location → Exclude' in the UI. At least one list must be non-empty.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to update
        add_geo_target_ids: Geo constants to add (e.g. [2826, 2124] = UK + Canada)
        remove_geo_target_ids: Geo constants to remove (only matches existing positive criteria)
        exclude_geo_target_ids: Geo constants to add as exclusions (negative criteria)
        add_language_ids: Language constants to add
        remove_language_ids: Language constants to remove
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")

    if not any([add_geo_target_ids, remove_geo_target_ids, exclude_geo_target_ids,
                add_language_ids, remove_language_ids]):
        return _json({"error": "Provide at least one of add/remove/exclude_geo_target_ids or add/remove_language_ids"})

    result = await asyncio.to_thread(
        gads_update_targeting, _client(), cid, camp,
        add_geo_target_ids, remove_geo_target_ids, exclude_geo_target_ids,
        add_language_ids, remove_language_ids,
    )
    return _json({"status": "success", "campaign_id": camp, **result})


@mcp.tool()
@_handle_errors
async def clone_campaign(
    customer_id: str,
    source_campaign_id: str,
    new_campaign_name: str,
    daily_budget: float | None = None,
    geo_target_ids: list[int] | int | None = None,
    language_ids: list[int] | int | None = None,
    bidding_strategy: str | None = None,
    target_cpa: float | None = None,
    target_roas: float | None = None,
    status: str = "PAUSED",
) -> str:
    """Clone a campaign atomically with optional overrides.

    Copies all ad groups, RSAs, keywords, negatives (both levels), extensions,
    IP exclusions, and targeting. Does NOT copy performance data, experiments,
    drafts, or A/B tests.

    Args:
        customer_id: Google Ads customer ID
        source_campaign_id: Campaign to copy from
        new_campaign_name: Name for the new campaign
        daily_budget: Optional override; defaults to source budget
        geo_target_ids: Optional override (int or list); defaults to source geo targets
        language_ids: Optional override (int or list); defaults to source languages
        bidding_strategy: Optional override; defaults to source strategy
        target_cpa: Required when bidding_strategy override = TARGET_CPA
        target_roas: Required when bidding_strategy override = TARGET_ROAS
        status: 'ENABLED' | 'PAUSED' | 'REMOVED'. Defaults to PAUSED for safety.
    """
    cid = _clean(customer_id, "customer_id")
    source = _clean(source_campaign_id, "source_campaign_id")

    errors = validate_campaign_status(status)
    if bidding_strategy is not None:
        errors.extend(validate_bidding_strategy(bidding_strategy))
        if bidding_strategy.upper() == "TARGET_CPA" and target_cpa is None:
            errors.append("target_cpa: required when bidding_strategy=TARGET_CPA")
        if bidding_strategy.upper() == "TARGET_ROAS" and target_roas is None:
            errors.append("target_roas: required when bidding_strategy=TARGET_ROAS")
    if daily_budget is not None and daily_budget <= 0:
        errors.append(f"daily_budget: must be greater than 0, got {daily_budget}")
    if not new_campaign_name.strip():
        errors.append("new_campaign_name: cannot be empty")
    failed = _check(errors)
    if failed:
        return failed

    geos = _normalize_id_list(geo_target_ids, "geo_target_ids") if geo_target_ids is not None else None
    langs = _normalize_id_list(language_ids, "language_ids") if language_ids is not None else None
    budget_micros = int(round(daily_budget * 1_000_000)) if daily_budget is not None else None

    result = await asyncio.to_thread(
        gads_clone_campaign, _client(), cid, source, new_campaign_name,
        budget_micros, geos, langs,
        bidding_strategy.upper() if bidding_strategy else None,
        target_cpa, target_roas,
        status.upper(),
    )
    return _json({"status": "success", **result})


@mcp.tool()
@_handle_errors
async def add_ad_group(
    customer_id: str,
    campaign_id: str,
    ad_group_name: str,
    cpc_bid_micros: int = 1_000_000,
    status: str = "PAUSED",
    headlines: list[str] | None = None,
    descriptions: list[str] | None = None,
    final_url: str | None = None,
    keywords_json: str = "[]",
    negative_keywords_json: str = "[]",
) -> str:
    """Create a new ad group inside an existing campaign, optionally with an RSA and keywords in one call.

    If headlines + descriptions + final_url are all present, an RSA is created in
    the new ad group. Otherwise just the ad group + keywords. Returns
    ad_group_id and ad_id (if an ad was created).

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to add the ad group to
        ad_group_name: Name for the new ad group
        cpc_bid_micros: Default CPC bid in micros (1_000_000 = $1.00). Default $1.00.
        status: 'ENABLED' | 'PAUSED' | 'REMOVED'. Default PAUSED.
        headlines: Optional RSA headlines (3-15, each ≤30 chars). If provided with descriptions+final_url, creates an RSA.
        descriptions: Optional RSA descriptions (2-4, each ≤90 chars).
        final_url: Optional RSA final URL.
        keywords_json: JSON array of positive keywords, each {"text": "...", "match_type": "BROAD|PHRASE|EXACT"}
        negative_keywords_json: JSON array of ad-group-level negatives, same format
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")
    keywords = _parse_json(keywords_json, "keywords_json") if keywords_json else []
    negative_keywords = _parse_json(negative_keywords_json, "negative_keywords_json") if negative_keywords_json else []

    errors = validate_ad_group_status(status)
    if not ad_group_name.strip():
        errors.append("ad_group_name: cannot be empty")

    create_rsa = bool(headlines and descriptions and final_url)
    if create_rsa:
        errors.extend(validate_headlines(headlines))
        errors.extend(validate_descriptions(descriptions))
    elif headlines or descriptions or final_url:
        errors.append("To create an RSA in this call, provide all of headlines, descriptions, and final_url")

    if keywords:
        errors.extend(validate_keywords(keywords, "keywords_json"))
    if negative_keywords:
        errors.extend(validate_keywords(negative_keywords, "negative_keywords_json"))
    failed = _check(errors)
    if failed:
        return failed

    result = await asyncio.to_thread(
        gads_add_ad_group, _client(), cid, camp,
        ad_group_name, cpc_bid_micros, status.upper(),
        headlines if create_rsa else None,
        descriptions if create_rsa else None,
        final_url if create_rsa else None,
        keywords, negative_keywords,
    )
    return _json({
        "status": "success",
        "ad_group_id": result["ad_group_id"],
        "ad_id": result["ad_id"],
        "message": (
            f"Ad group '{ad_group_name}' created in {status.upper()} status with "
            f"{len(keywords)} keywords, {len(negative_keywords)} negatives"
            f"{', and 1 RSA' if create_rsa else ''}."
        ),
    })


@mcp.tool()
@_handle_errors
async def create_ad(
    customer_id: str,
    ad_group_id: str,
    headlines: list[str],
    descriptions: list[str],
    final_url: str,
    status: str = "PAUSED",
) -> str:
    """Create a new RSA in an existing ad group (alongside any existing ads).

    For replacing an ad's content, use update_ad instead. This tool is for A/B
    testing where you want 2+ RSAs in the same ad group.

    Args:
        customer_id: Google Ads customer ID
        ad_group_id: Ad group to add the RSA to
        headlines: 3-15 headlines, each ≤30 chars
        descriptions: 2-4 descriptions, each ≤90 chars
        final_url: Final URL for the ad
        status: 'ENABLED' | 'PAUSED' | 'REMOVED'. Default PAUSED.
    """
    cid = _clean(customer_id, "customer_id")
    ag = _clean(ad_group_id, "ad_group_id")

    errors = validate_ad_status(status)
    errors.extend(validate_headlines(headlines))
    errors.extend(validate_descriptions(descriptions))
    if not final_url.strip():
        errors.append("final_url: cannot be empty")
    failed = _check(errors)
    if failed:
        return failed

    result = await asyncio.to_thread(
        gads_create_rsa, _client(), cid, ag,
        headlines, descriptions, final_url, status.upper(),
    )
    return _json({"status": "success", **result})


@mcp.tool()
@_handle_errors
async def update_ad(
    customer_id: str,
    ad_group_id: str,
    ad_id: str,
    headlines: list[str],
    descriptions: list[str],
) -> str:
    """Replace an RSA ad's headlines and descriptions.

    RSA ad content is immutable in Google Ads, so this creates a new ad with the
    updated headlines/descriptions (preserving final_urls and status) and removes
    the old ad atomically. The returned new_ad_id replaces the passed-in ad_id.
    Fetch the current ad first with get_ads, merge your changes, then call this.

    Args:
        customer_id: Google Ads customer ID
        ad_group_id: Ad group containing the ad
        ad_id: The ad ID to replace
        headlines: Complete list of headlines (max 15, each max 30 chars)
        descriptions: Complete list of descriptions (max 4, each max 90 chars)
    """
    cid = _clean(customer_id, "customer_id")
    ag = _clean(ad_group_id, "ad_group_id")
    aid = _clean(ad_id, "ad_id")

    errors = validate_headlines(headlines)
    errors.extend(validate_descriptions(descriptions))
    failed = _check(errors)
    if failed:
        return failed

    result = await asyncio.to_thread(gads_update_rsa, _client(), cid, ag, aid, headlines, descriptions)
    return _json({
        "status": "success",
        "new_ad_id": result["new_ad_id"],
        "removed_ad_id": result["removed_ad_id"],
        "message": (
            f"Replaced ad {aid} with new ad {result['new_ad_id']} "
            f"({len(headlines)} headlines, {len(descriptions)} descriptions). "
            f"RSA content is immutable, so a new ad was created and the old one removed."
        ),
    })


@mcp.tool()
@_handle_errors
async def modify_keyword_status(
    customer_id: str,
    actions_json: str,
) -> str:
    """Pause or remove keywords from ad groups.

    Args:
        customer_id: Google Ads customer ID
        actions_json: JSON array of actions, each {"ad_group_id": "...", "criterion_id": "...", "action": "PAUSED|REMOVED"}
    """
    cid = _clean(customer_id, "customer_id")
    actions = _parse_json(actions_json, "actions_json")

    failed = _check(validate_keyword_actions(actions))
    if failed:
        return failed

    await asyncio.to_thread(gads_pause_keywords, _client(), cid, actions)
    return _json({"status": "success", "message": f"Applied {len(actions)} keyword status changes"})


# ═════════════════════════════════════════════════════════════════════════

if __name__ == "__main__":
    import uvicorn

    if not MCP_API_TOKEN:
        print(
            "WARNING: MCP_API_TOKEN is not set."
            " The server is running without authentication."
        )

    port = int(os.environ.get("PORT", 10000))
    uvicorn.run(create_app(), host="0.0.0.0", port=port)
