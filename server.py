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
import hmac
import os

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from starlette.types import ASGIApp, Receive, Scope, Send
from google.ads.googleads.errors import GoogleAdsException

from validators import (
    validate_campaign,
    validate_headlines,
    validate_descriptions,
    validate_keywords,
    validate_url,
    validate_match_type,
    validate_bidding_strategy,
    validate_sitelinks,
    validate_callouts,
    validate_snippets,
    validate_negative_keyword_texts,
    validate_keyword_actions,
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
    add_negative_keywords as gads_add_negatives,
    add_sitelinks_to_campaign,
    add_callouts_to_campaign,
    add_snippets_to_campaign,
    add_keywords_to_ad_group as gads_add_keywords,
    update_rsa_ad as gads_update_rsa,
    add_ad_group_negative_keywords as gads_add_ag_negatives,
    pause_keywords as gads_pause_keywords,
    get_keyword_forecast_metrics,
    get_top_keywords_by_cost,
    get_top_search_terms_by_cost,
    get_campaign_negative_keywords,
    get_ad_group_negative_keywords as gads_get_ag_negatives,
)

# ─── Environment ───────────────────────────────────────────────────────

MCP_API_TOKEN = os.environ.get("MCP_API_TOKEN")
RENDER_EXTERNAL_HOSTNAME = os.environ.get("RENDER_EXTERNAL_HOSTNAME")

# ─── FastMCP instance ──────────────────────────────────────────────────

mcp = FastMCP(
    "prime-ads",
    stateless_http=True,
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
- **Create**: create_campaign (everything created PAUSED)
- **Optimize**: load_full_campaign → apply changes with add_keywords, update_ad, add_sitelinks, etc.
- **Reduce waste**: get_keyword_performance / get_search_term_performance → add_campaign_negatives / modify_keyword_status

## RSA limits
Headlines: min 3, max 15, each ≤30 chars. Descriptions: min 2, max 4, each ≤90 chars.
Sitelink text ≤25 chars, sitelink descriptions ≤35 chars. Callouts ≤25 chars.

## Important
- create_campaign creates everything in PAUSED status — nothing goes live automatically.
- update_ad REPLACES all headlines/descriptions — always fetch current ad first and merge.
- customer_id accepts dashes (e.g. '123-456-7890') — they are stripped automatically.
""",
)

logger = logging.getLogger("prime-ads")


# ─── Health check (bypasses auth) ──────────────────────────────────────


@mcp.custom_route("/health", methods=["GET"])
async def health(request: Request) -> Response:
    return JSONResponse({"status": "ok"})


# ─── Bearer token auth middleware ──────────────────────────────────────


class BearerAuthMiddleware:
    def __init__(self, app: ASGIApp) -> None:
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send):
        if scope["type"] != "http" or scope["path"] == "/health":
            await self.app(scope, receive, send)
            return

        request = Request(scope)
        auth = request.headers.get("authorization", "")
        token = request.query_params.get("token", "")

        # Encode strings to bytes to handle URL-decoded/non-ASCII chars safely
        valid_bearer = f"Bearer {MCP_API_TOKEN}".encode("utf-8")
        auth_bytes = auth.encode("utf-8")
        
        valid_token = str(MCP_API_TOKEN).encode("utf-8")
        token_bytes = token.encode("utf-8")

        # constant-time comparison to prevent timing side-channel attacks
        if hmac.compare_digest(auth_bytes, valid_bearer) or hmac.compare_digest(token_bytes, valid_token):
            await self.app(scope, receive, send)
            return

        response = JSONResponse(
            {
                "jsonrpc": "2.0",
                "error": {"code": -32001, "message": "Unauthorized"},
                "id": None,
            },
            status_code=401,
        )
        await response(scope, receive, send)


# ─── ASGI app factory ─────────────────────────────────────────────────


def create_app():
    app = mcp.streamable_http_app()
    # When no token is set (local dev), auth is disabled entirely
    if MCP_API_TOKEN:
        app.add_middleware(BearerAuthMiddleware)
    return app


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


@mcp.tool()
@_handle_errors
async def create_campaign(
    customer_id: str,
    campaign_name: str,
    daily_budget_usd: float,
    url: str,
    headlines: list[str],
    descriptions: list[str],
    keywords_json: str,
    negative_keywords_json: str = "[]",
    bidding_strategy: str = "MANUAL_CPC",
    language_id: int = 1000,
    geo_target_id: int = 2840,
    sitelinks_json: str = "[]",
    callouts_json: str = "[]",
    snippets_json: str = "[]",
) -> str:
    """Create a complete paused Google Search campaign with ad group, RSA ad, keywords, and extensions.
    The campaign and all components are created in PAUSED status.

    Args:
        customer_id: Google Ads customer ID
        campaign_name: Name for the new campaign
        daily_budget_usd: Daily budget in USD (e.g. 10.0 for $10/day)
        url: Final URL for the ads (e.g. 'https://example.com')
        headlines: List of ad headlines (3-15 headlines, each max 30 chars)
        descriptions: List of ad descriptions (2-4 descriptions, each max 90 chars)
        keywords_json: JSON array of keywords, each {"text": "...", "match_type": "BROAD|PHRASE|EXACT"}
        negative_keywords_json: JSON array of negative keywords, same format as keywords_json
        bidding_strategy: One of MANUAL_CPC, MAXIMIZE_CLICKS, MAXIMIZE_CONVERSIONS, ENHANCED_CPC
        language_id: Language targeting constant (1000=English, 1003=Spanish, 1001=French)
        geo_target_id: Geo targeting constant (2840=United States, 2826=United Kingdom, 2124=Canada)
        sitelinks_json: JSON array of sitelinks, each {"link_text": "...", "description1": "...", "description2": "...", "final_url": "..."}
        callouts_json: JSON array of callouts, each {"text": "..."}
        snippets_json: JSON array of structured snippets, each {"header": "Types|Brands|...", "values": ["a","b","c"]}
    """
    cid = _clean(customer_id, "customer_id")

    keywords = _parse_json(keywords_json, "keywords_json")
    negative_keywords = _parse_json(negative_keywords_json, "negative_keywords_json")
    sitelinks = _parse_json(sitelinks_json, "sitelinks_json")
    callouts = _parse_json(callouts_json, "callouts_json")
    snippets = _parse_json(snippets_json, "snippets_json")

    failed = _check(validate_campaign(
        campaign_name=campaign_name,
        daily_budget_usd=daily_budget_usd,
        url=url,
        headlines=headlines,
        descriptions=descriptions,
        keywords=keywords,
        negative_keywords=negative_keywords,
        bidding_strategy=bidding_strategy,
        sitelinks=sitelinks,
        callouts=callouts,
        snippets=snippets,
    ))
    if failed:
        return failed

    budget_micros = int(round(daily_budget_usd * 1_000_000))

    resource = await asyncio.to_thread(
        create_paused_campaign,
        _client(),
        cid,
        campaign_name,
        budget_micros,
        headlines,
        descriptions,
        keywords,
        negative_keywords,
        language_id,
        geo_target_id,
        url,
        bidding_strategy,
        sitelinks,
        callouts,
        snippets,
    )
    return _json({
        "status": "success",
        "resource_name": resource,
        "message": f"Campaign '{campaign_name}' created in PAUSED status with {len(keywords)} keywords, {len(headlines)} headlines, {len(descriptions)} descriptions.",
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
async def add_campaign_negatives(
    customer_id: str,
    campaign_id: str,
    keyword_texts: list[str],
    match_type: str = "PHRASE",
) -> str:
    """Add negative keywords to a campaign to prevent ads from showing for irrelevant searches.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to add negatives to
        keyword_texts: List of keyword texts to exclude (e.g. ["free", "cheap", "diy"])
        match_type: Match type for all keywords: BROAD, PHRASE, or EXACT
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")

    errors = validate_negative_keyword_texts(keyword_texts)
    errors.extend(validate_match_type(match_type))
    failed = _check(errors)
    if failed:
        return failed

    await asyncio.to_thread(gads_add_negatives, _client(), cid, camp, keyword_texts, match_type.upper())
    return _json({"status": "success", "message": f"Added {len(keyword_texts)} {match_type} negative keywords to campaign {camp}"})


@mcp.tool()
@_handle_errors
async def add_ad_group_negatives(
    customer_id: str,
    ad_group_id: str,
    keyword_texts: list[str],
    match_type: str = "PHRASE",
) -> str:
    """Add negative keywords to a specific ad group.

    Args:
        customer_id: Google Ads customer ID
        ad_group_id: Ad group to add negatives to
        keyword_texts: List of keyword texts to exclude
        match_type: BROAD, PHRASE, or EXACT
    """
    cid = _clean(customer_id, "customer_id")
    ag = _clean(ad_group_id, "ad_group_id")

    errors = validate_negative_keyword_texts(keyword_texts)
    errors.extend(validate_match_type(match_type))
    failed = _check(errors)
    if failed:
        return failed

    await asyncio.to_thread(gads_add_ag_negatives, _client(), cid, ag, keyword_texts, match_type.upper())
    return _json({"status": "success", "message": f"Added {len(keyword_texts)} {match_type} negatives to ad group {ag}"})


@mcp.tool()
@_handle_errors
async def add_sitelinks(
    customer_id: str,
    campaign_id: str,
    sitelinks_json: str,
) -> str:
    """Add sitelink extensions to a campaign. Sitelinks appear as additional links below your ad.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to add sitelinks to
        sitelinks_json: JSON array of sitelinks, each {"link_text": "..." (max 25 chars), "description1": "..." (max 35 chars), "description2": "..." (max 35 chars), "final_url": "https://..."}
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")
    sitelinks = _parse_json(sitelinks_json, "sitelinks_json")

    failed = _check(validate_sitelinks(sitelinks))
    if failed:
        return failed

    campaign_resource = f"customers/{cid}/campaigns/{camp}"
    await asyncio.to_thread(add_sitelinks_to_campaign, _client(), cid, campaign_resource, sitelinks)
    return _json({"status": "success", "message": f"Added {len(sitelinks)} sitelinks to campaign {camp}"})


@mcp.tool()
@_handle_errors
async def add_callouts(
    customer_id: str,
    campaign_id: str,
    callout_texts: list[str],
) -> str:
    """Add callout extensions to a campaign. Callouts are short highlights like 'Free Shipping' or '24/7 Support'.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to add callouts to
        callout_texts: List of callout texts (each max 25 chars), e.g. ["Free Shipping", "24/7 Support"]
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")

    failed = _check(validate_callouts(callout_texts))
    if failed:
        return failed

    callouts = [{"text": t} for t in callout_texts]
    campaign_resource = f"customers/{cid}/campaigns/{camp}"
    await asyncio.to_thread(add_callouts_to_campaign, _client(), cid, campaign_resource, callouts)
    return _json({"status": "success", "message": f"Added {len(callouts)} callouts to campaign {camp}"})


@mcp.tool()
@_handle_errors
async def add_structured_snippets(
    customer_id: str,
    campaign_id: str,
    snippets_json: str,
) -> str:
    """Add structured snippet extensions to a campaign. Snippets highlight specific aspects of your products/services.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to add snippets to
        snippets_json: JSON array of snippets, each {"header": "Types|Brands|Services|...", "values": ["val1", "val2", "val3"]}. Valid headers: Amenities, Brands, Courses, Degree programs, Destinations, Featured hotels, Insurance coverage, Models, Neighborhoods, Service catalog, Shows, Styles, Types.
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")
    snippets = _parse_json(snippets_json, "snippets_json")

    failed = _check(validate_snippets(snippets))
    if failed:
        return failed

    campaign_resource = f"customers/{cid}/campaigns/{camp}"
    await asyncio.to_thread(add_snippets_to_campaign, _client(), cid, campaign_resource, snippets)
    return _json({"status": "success", "message": f"Added {len(snippets)} structured snippets to campaign {camp}"})


@mcp.tool()
@_handle_errors
async def update_ad(
    customer_id: str,
    ad_group_id: str,
    ad_id: str,
    headlines: list[str],
    descriptions: list[str],
) -> str:
    """Update an RSA ad's headlines and descriptions. This REPLACES all existing headlines and descriptions.
    Fetch the current ad first with get_ads, merge your changes, then call this.

    Args:
        customer_id: Google Ads customer ID
        ad_group_id: Ad group containing the ad
        ad_id: The ad ID to update
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

    await asyncio.to_thread(gads_update_rsa, _client(), cid, ag, aid, headlines, descriptions)
    return _json({
        "status": "success",
        "message": f"Updated ad {aid} with {len(headlines)} headlines and {len(descriptions)} descriptions",
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
# FORECAST TOOL
# ═════════════════════════════════════════════════════════════════════════


@mcp.tool()
@_handle_errors
async def forecast_budget(
    customer_id: str,
    keywords: list[str],
    target_ctr: float = 3.0,
    geo_target_id: int = 2840,
    language_id: int = 1000,
) -> str:
    """Forecast monthly budget for a set of keywords based on historical search volume and bid estimates.

    Args:
        customer_id: Google Ads customer ID
        keywords: List of keyword texts to forecast (e.g. ["coffee beans", "buy coffee online"])
        target_ctr: Expected click-through rate as percentage (default 3.0 = 3%)
        geo_target_id: Geo target (2840=US, 2826=UK, 2124=Canada)
        language_id: Language (1000=English)
    """
    if not keywords:
        return _json({"total_monthly_budget": 0, "recommended_daily_budget": 0, "keyword_metrics": []})

    cid = _clean(customer_id, "customer_id")
    metrics_data = await asyncio.to_thread(
        get_keyword_forecast_metrics, _client(), cid, keywords, geo_target_id, language_id
    )

    keyword_breakdown = []
    total_monthly_cost = 0.0

    for m in metrics_data:
        low_bid = m["low_top_of_page_bid_micros"] / 1_000_000
        high_bid = m["high_top_of_page_bid_micros"] / 1_000_000
        avg_cpc = (low_bid + high_bid) / 2 if (low_bid + high_bid) > 0 else 0
        estimated_clicks = m["avg_monthly_searches"] * (target_ctr / 100)
        monthly_cost = estimated_clicks * avg_cpc
        total_monthly_cost += monthly_cost

        keyword_breakdown.append({
            "keyword": m["keyword"],
            "monthly_volume": m["avg_monthly_searches"],
            "avg_cpc": round(avg_cpc, 2),
            "estimated_clicks": round(estimated_clicks, 2),
            "monthly_cost": round(monthly_cost, 2),
        })

    return _json({
        "total_monthly_budget": round(total_monthly_cost, 2),
        "recommended_daily_budget": round(total_monthly_cost / 30.4, 2),
        "keyword_metrics": keyword_breakdown,
    })


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
