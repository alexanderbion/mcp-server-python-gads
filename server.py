"""
MCP Server for Google Ads campaign management.

15-tool architecture (v2):
- 10 core tools (read + add only)
- 5 restricted tools (update/remove) gated by GOOGLE_ADS_ALLOW_DESTRUCTIVE env var
  and annotated with destructiveHint=True so MCP clients can warn the user.
"""

import json
import re
import asyncio
import logging
import functools
import os

from mcp.server.fastmcp import FastMCP
from mcp.server.transport_security import TransportSecuritySettings
from mcp.types import ToolAnnotations
from starlette.requests import Request
from starlette.responses import JSONResponse, Response
from google.ads.googleads.errors import GoogleAdsException

from validators import (
    validate_campaign,
    validate_headlines,
    validate_descriptions,
    validate_keywords,
    validate_url,
    validate_sitelinks,
    validate_callouts,
    validate_snippets,
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
    create_rsa_ad as gads_create_rsa,
    remove_campaign_by_id as gads_remove_campaign,
    remove_ad_by_id as gads_remove_ad,
    remove_campaign_extensions as gads_remove_extensions,
    pause_keywords as gads_pause_keywords,
    get_keyword_forecast_metrics,
    get_top_keywords_by_cost,
    get_top_search_terms_by_cost,
    get_campaign_performance as gads_campaign_performance,
    get_campaign_negative_keywords,
    get_ad_group_negative_keywords as gads_get_ag_negatives,
)

# ─── Environment ───────────────────────────────────────────────────────

MCP_API_TOKEN = os.environ.get("MCP_API_TOKEN")
RENDER_EXTERNAL_HOSTNAME = os.environ.get("RENDER_EXTERNAL_HOSTNAME")
ALLOW_DESTRUCTIVE = os.environ.get("GOOGLE_ADS_ALLOW_DESTRUCTIVE", "").lower() in {"1", "true", "yes"}

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

## Tool tiers
- **Core (10 tools)** — read and add-only. Always available.
- **Restricted (5 tools)** — update_ad, remove_keywords, remove_extensions, remove_campaign, remove_ad.
  These are marked with destructiveHint=True and only enabled when GOOGLE_ADS_ALLOW_DESTRUCTIVE=true.

## Typical workflows
- **Browse**: list_accounts → list_campaigns → list_ad_groups → load_campaign
- **Create**: create_campaign (everything created PAUSED)
- **Optimize**: load_campaign → manage_keywords / manage_extensions / create_ad
- **Performance**: get_performance with report_type="keywords" | "search_terms" | "campaign"
- **Reduce waste**: get_performance → manage_keywords (add_negative) or remove_keywords (restricted)

## RSA limits
Headlines: min 3, max 15, each ≤30 chars. Descriptions: min 2, max 4, each ≤90 chars.
Sitelink text ≤25 chars, sitelink descriptions ≤35 chars. Callouts ≤25 chars.

## Important
- create_campaign and create_ad create everything in PAUSED status — nothing goes live automatically.
- update_ad REPLACES all headlines/descriptions — always fetch current ad first with load_campaign and merge.
- customer_id accepts dashes (e.g. '123-456-7890') — they are stripped automatically.
- All write paths use validate-then-push: a dry run is always performed before the real mutation.
""",
)

logger = logging.getLogger("prime-ads")


from starlette.applications import Starlette
from starlette.routing import Mount, Route

# ─── Health check ──────────────────────────────────────────────────────

async def health(request: Request) -> Response:
    return JSONResponse({"status": "ok"})


# ─── ASGI app factory ─────────────────────────────────────────────────

def create_app():
    # Use sse_app() which natively supports Claude's SSE connection flow
    mcp_app = mcp.sse_app()

    # Mount the MCP server behind a hard-to-guess secret path.
    # Claude Web connectors drop query params (like ?token=...) on subsequent
    # POST requests, so using a secret URL path guarantees secure connection without auth failures.
    secret_path = "/mcp-primeads-secure-proxy-829xyz"

    app = Starlette(routes=[
        Route("/health", health, methods=["GET"]),
        Mount(secret_path, app=mcp_app),
    ])
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


def _check_destructive_allowed() -> str | None:
    """Return an error JSON string if destructive tools are disabled, else None."""
    if not ALLOW_DESTRUCTIVE:
        return _json({
            "error": (
                "Destructive operation refused. Set GOOGLE_ADS_ALLOW_DESTRUCTIVE=true "
                "on the server to enable update/remove tools."
            )
        })
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


READ_ONLY = ToolAnnotations(readOnlyHint=True)
DESTRUCTIVE = ToolAnnotations(destructiveHint=True)


# ═════════════════════════════════════════════════════════════════════════
# CORE TOOLS — read + add only (always available)
# ═════════════════════════════════════════════════════════════════════════


# 1. list_accounts ───────────────────────────────────────────────────────

@mcp.tool(annotations=READ_ONLY)
@_handle_errors
async def list_accounts() -> str:
    """List all Google Ads accounts accessible under the configured MCC manager account.
    Returns account IDs and names. Call this first to find the customer_id needed by other tools."""
    client = _client()
    accounts = await asyncio.to_thread(list_accessible_customers, client)
    if not accounts:
        return "No accessible accounts found. Check GOOGLE_ADS_LOGIN_CUSTOMER_ID in .env."
    return _json(accounts)


# 2. list_campaigns ──────────────────────────────────────────────────────

@mcp.tool(annotations=READ_ONLY)
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


# 3. list_ad_groups ──────────────────────────────────────────────────────

@mcp.tool(annotations=READ_ONLY)
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


# 4. load_campaign ──────────────────────────────────────────────────────

@mcp.tool(annotations=READ_ONLY)
@_handle_errors
async def load_campaign(customer_id: str, campaign_id: str, ad_group_id: str = "") -> str:
    """Load all data for a campaign in a single call.
    Returns targeting, extensions, and negative keywords (campaign + ad group level).
    If ad_group_id is provided, also returns ads and positive/negative keywords for that ad group.
    Use this before making changes to get a complete picture of the campaign.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to load
        ad_group_id: Optional ad group ID. If provided, ads and ad-group keywords are included.
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")
    ag = _clean(ad_group_id, "ad_group_id") if ad_group_id else ""
    client = _client()

    targeting, extensions, camp_negs, ag_negs = await asyncio.gather(
        asyncio.to_thread(gads_get_targeting, client, cid, camp),
        asyncio.to_thread(gads_get_extensions, client, cid, camp),
        asyncio.to_thread(get_campaign_negative_keywords, client, cid, camp),
        asyncio.to_thread(gads_get_ag_negatives, client, cid, camp),
    )

    result = {
        "targeting": targeting,
        "extensions": extensions,
        "negative_keywords": {
            "campaign_level": camp_negs,
            "ad_group_level": ag_negs,
        },
    }

    if ag:
        ads, keywords = await asyncio.gather(
            asyncio.to_thread(get_ad_group_ads, client, cid, ag),
            asyncio.to_thread(get_ad_group_keywords, client, cid, ag),
        )
        result["ads"] = ads
        result["keywords"] = keywords

    return _json(result)


# 5. get_performance ─────────────────────────────────────────────────────

@mcp.tool(annotations=READ_ONLY)
@_handle_errors
async def get_performance(
    customer_id: str,
    campaign_id: str,
    report_type: str = "keywords",
    days: int = 30,
) -> str:
    """Get performance metrics for a campaign.
    Returns impressions, clicks, CTR, conversions, and cost over the lookback window.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to analyze
        report_type: One of "keywords", "search_terms", or "campaign".
            - "keywords": top 50 keywords by cost, with keyword_text, match_type, ad_group_id.
            - "search_terms": top 50 search terms (what users actually typed) by cost.
            - "campaign": single row of aggregate metrics for the whole campaign.
        days: Lookback window in days (default 30).
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")

    if days <= 0:
        return _json({"error": "days must be a positive integer"})

    rtype = report_type.lower()
    if rtype == "keywords":
        data = await asyncio.to_thread(get_top_keywords_by_cost, _client(), cid, camp, days)
    elif rtype == "search_terms":
        data = await asyncio.to_thread(get_top_search_terms_by_cost, _client(), cid, camp, days)
    elif rtype == "campaign":
        data = await asyncio.to_thread(gads_campaign_performance, _client(), cid, camp, days)
    else:
        return _json({
            "error": f"report_type '{report_type}' invalid — must be 'keywords', 'search_terms', or 'campaign'"
        })

    return _json(data)


# 6. create_campaign ─────────────────────────────────────────────────────

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


# 7. manage_keywords ─────────────────────────────────────────────────────

@mcp.tool()
@_handle_errors
async def manage_keywords(
    customer_id: str,
    campaign_id: str,
    action: str,
    level: str,
    keywords_json: str,
    ad_group_id: str = "",
) -> str:
    """Add positive or negative keywords at either campaign or ad group level.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign the keywords belong to
        action: "add" (positive keywords) or "add_negative" (negative keywords)
        level: "campaign" or "ad_group"
        keywords_json: JSON array of {"text": "...", "match_type": "BROAD|PHRASE|EXACT"}
        ad_group_id: Required when level="ad_group"

    Valid combinations:
        - action="add",          level="ad_group"  → add positive keywords to the ad group
        - action="add_negative", level="campaign"  → add campaign-level negative keywords
        - action="add_negative", level="ad_group"  → add ad-group-level negative keywords
        (Positive keywords can only be added at ad-group level.)
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")

    action_n = action.lower()
    level_n = level.lower()

    if action_n not in {"add", "add_negative"}:
        return _json({"error": f"action '{action}' invalid — must be 'add' or 'add_negative'"})
    if level_n not in {"campaign", "ad_group"}:
        return _json({"error": f"level '{level}' invalid — must be 'campaign' or 'ad_group'"})
    if action_n == "add" and level_n == "campaign":
        return _json({"error": "Positive keywords can only be added at ad_group level. Use level='ad_group'."})
    if level_n == "ad_group" and not ad_group_id:
        return _json({"error": "ad_group_id is required when level='ad_group'"})

    keywords = _parse_json(keywords_json, "keywords_json")
    failed = _check(validate_keywords(keywords))
    if failed:
        return failed

    client = _client()

    if level_n == "campaign":
        await asyncio.to_thread(gads_add_negatives, client, cid, camp, keywords)
        return _json({
            "status": "success",
            "message": f"Added {len(keywords)} campaign-level negative keywords to campaign {camp}",
        })

    # ad_group level — positive or negative
    ag = _clean(ad_group_id, "ad_group_id")
    ag_resource = f"customers/{cid}/adGroups/{ag}"
    # Mark negatives
    if action_n == "add_negative":
        for kw in keywords:
            kw["negative"] = True

    await asyncio.to_thread(gads_add_keywords, client, cid, ag_resource, keywords)
    kind = "negative " if action_n == "add_negative" else ""
    return _json({
        "status": "success",
        "message": f"Added {len(keywords)} {kind}keywords to ad group {ag}",
    })


# 8. manage_extensions ───────────────────────────────────────────────────

@mcp.tool()
@_handle_errors
async def manage_extensions(
    customer_id: str,
    campaign_id: str,
    type: str,
    data_json: str,
) -> str:
    """Add sitelinks, callouts, or structured snippets to a campaign.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to add extensions to
        type: One of "sitelinks", "callouts", "snippets"
        data_json: JSON array; format depends on type:
            - "sitelinks": [{"link_text": "..." (≤25), "description1": "..." (≤35), "description2": "..." (≤35), "final_url": "..."}]
            - "callouts":  [{"text": "..." (≤25)}]
            - "snippets":  [{"header": "Types|Brands|Services|...", "values": ["a","b","c"]}]
    """
    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")
    ext_type = type.lower()

    if ext_type not in {"sitelinks", "callouts", "snippets"}:
        return _json({"error": f"type '{type}' invalid — must be 'sitelinks', 'callouts', or 'snippets'"})

    data = _parse_json(data_json, "data_json")
    if not isinstance(data, list) or not data:
        return _json({"error": "data_json must be a non-empty JSON array"})

    client = _client()
    campaign_resource = f"customers/{cid}/campaigns/{camp}"

    if ext_type == "sitelinks":
        failed = _check(validate_sitelinks(data))
        if failed:
            return failed
        await asyncio.to_thread(add_sitelinks_to_campaign, client, cid, campaign_resource, data)
        return _json({"status": "success", "message": f"Added {len(data)} sitelinks to campaign {camp}"})

    if ext_type == "callouts":
        failed = _check(validate_callouts(data))
        if failed:
            return failed
        # Normalize list[str] → list[{text}] if needed
        callouts = [c if isinstance(c, dict) else {"text": c} for c in data]
        await asyncio.to_thread(add_callouts_to_campaign, client, cid, campaign_resource, callouts)
        return _json({"status": "success", "message": f"Added {len(callouts)} callouts to campaign {camp}"})

    # snippets
    failed = _check(validate_snippets(data))
    if failed:
        return failed
    await asyncio.to_thread(add_snippets_to_campaign, client, cid, campaign_resource, data)
    return _json({"status": "success", "message": f"Added {len(data)} structured snippets to campaign {camp}"})


# 9. create_ad ──────────────────────────────────────────────────────────

@mcp.tool()
@_handle_errors
async def create_ad(
    customer_id: str,
    ad_group_id: str,
    headlines: list[str],
    descriptions: list[str],
    final_url: str,
) -> str:
    """Create a new RSA (Responsive Search Ad) in an existing ad group. Created in PAUSED status.

    Args:
        customer_id: Google Ads customer ID
        ad_group_id: Ad group to create the ad in
        headlines: List of headlines (3-15, each max 30 chars)
        descriptions: List of descriptions (2-4, each max 90 chars)
        final_url: Landing page URL (e.g. 'https://example.com')
    """
    cid = _clean(customer_id, "customer_id")
    ag = _clean(ad_group_id, "ad_group_id")

    errors = validate_headlines(headlines)
    errors.extend(validate_descriptions(descriptions))
    errors.extend(validate_url(final_url, "final_url"))
    failed = _check(errors)
    if failed:
        return failed

    resource = await asyncio.to_thread(
        gads_create_rsa, _client(), cid, ag, headlines, descriptions, final_url
    )
    # resource_name format: customers/{cid}/adGroupAds/{ad_group_id}~{ad_id}
    ad_id = resource.rsplit("~", 1)[-1] if "~" in resource else ""
    return _json({
        "status": "success",
        "id": ad_id,
        "resource_name": resource,
        "message": f"Created paused RSA ad {ad_id} in ad group {ag} with {len(headlines)} headlines and {len(descriptions)} descriptions",
    })


# 10. forecast_budget ───────────────────────────────────────────────────

@mcp.tool(annotations=READ_ONLY)
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
# RESTRICTED TOOLS — update / remove (gated by GOOGLE_ADS_ALLOW_DESTRUCTIVE)
# ═════════════════════════════════════════════════════════════════════════


# 11. update_ad ⚠️ RESTRICTED ──────────────────────────────────────────

@mcp.tool(annotations=DESTRUCTIVE)
@_handle_errors
async def update_ad(
    customer_id: str,
    ad_group_id: str,
    ad_id: str,
    headlines: list[str],
    descriptions: list[str],
) -> str:
    """⚠️ RESTRICTED. Update an RSA ad's headlines and descriptions. REPLACES all existing headlines and descriptions.
    Always fetch the current ad first with load_campaign, merge your changes, then call this.

    Args:
        customer_id: Google Ads customer ID
        ad_group_id: Ad group containing the ad
        ad_id: The ad ID to update
        headlines: Complete list of headlines (max 15, each max 30 chars)
        descriptions: Complete list of descriptions (max 4, each max 90 chars)
    """
    blocked = _check_destructive_allowed()
    if blocked:
        return blocked

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


# 12. remove_keywords ⚠️ RESTRICTED ────────────────────────────────────

@mcp.tool(annotations=DESTRUCTIVE)
@_handle_errors
async def remove_keywords(
    customer_id: str,
    actions_json: str,
) -> str:
    """⚠️ RESTRICTED. Pause or remove keywords from ad groups. REMOVED is permanent; PAUSED can be re-enabled.
    Use load_campaign first to get criterion IDs.

    Args:
        customer_id: Google Ads customer ID
        actions_json: JSON array of {"ad_group_id": "...", "criterion_id": "...", "action": "PAUSED|REMOVED"}
    """
    blocked = _check_destructive_allowed()
    if blocked:
        return blocked

    cid = _clean(customer_id, "customer_id")
    actions = _parse_json(actions_json, "actions_json")

    failed = _check(validate_keyword_actions(actions))
    if failed:
        return failed

    await asyncio.to_thread(gads_pause_keywords, _client(), cid, actions)
    return _json({"status": "success", "message": f"Applied {len(actions)} keyword status changes"})


# 13. remove_extensions ⚠️ RESTRICTED ──────────────────────────────────

@mcp.tool(annotations=DESTRUCTIVE)
@_handle_errors
async def remove_extensions(
    customer_id: str,
    campaign_id: str,
    type: str,
    asset_ids: list[str],
) -> str:
    """⚠️ RESTRICTED. Unlink sitelinks, callouts, or structured snippets from a campaign.
    Removes the campaign→asset link; does not delete the underlying asset.
    Get the IDs from load_campaign — each extension item has an `asset_id` field.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to remove extensions from
        type: One of "sitelinks", "callouts", "snippets"
        asset_ids: List of asset IDs (the `asset_id` values returned by load_campaign) to unlink
    """
    blocked = _check_destructive_allowed()
    if blocked:
        return blocked

    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")

    if type.lower() not in {"sitelinks", "callouts", "snippets"}:
        return _json({"error": f"type '{type}' invalid — must be 'sitelinks', 'callouts', or 'snippets'"})
    if not asset_ids:
        return _json({"error": "asset_ids must be a non-empty list"})

    cleaned_ids = [_clean(str(a), "asset_id") for a in asset_ids]

    removed = await asyncio.to_thread(
        gads_remove_extensions, _client(), cid, camp, type.lower(), cleaned_ids
    )
    return _json({
        "status": "success",
        "removed": removed,
        "message": f"Removed {removed} {type.lower()} link(s) from campaign {camp}",
    })


# 14. remove_campaign ⚠️ RESTRICTED ────────────────────────────────────

@mcp.tool(annotations=DESTRUCTIVE)
@_handle_errors
async def remove_campaign(
    customer_id: str,
    campaign_id: str,
) -> str:
    """⚠️ RESTRICTED. Remove (delete) a campaign. IRREVERSIBLE.

    Args:
        customer_id: Google Ads customer ID
        campaign_id: Campaign to remove
    """
    blocked = _check_destructive_allowed()
    if blocked:
        return blocked

    cid = _clean(customer_id, "customer_id")
    camp = _clean(campaign_id, "campaign_id")

    await asyncio.to_thread(gads_remove_campaign, _client(), cid, camp)
    return _json({"status": "success", "message": f"Campaign {camp} removed"})


# 15. remove_ad ⚠️ RESTRICTED ──────────────────────────────────────────

@mcp.tool(annotations=DESTRUCTIVE)
@_handle_errors
async def remove_ad(
    customer_id: str,
    ad_group_id: str,
    ad_id: str,
) -> str:
    """⚠️ RESTRICTED. Remove an ad from an ad group. Sets ad status to REMOVED.

    Args:
        customer_id: Google Ads customer ID
        ad_group_id: Ad group containing the ad
        ad_id: The ad to remove
    """
    blocked = _check_destructive_allowed()
    if blocked:
        return blocked

    cid = _clean(customer_id, "customer_id")
    ag = _clean(ad_group_id, "ad_group_id")
    aid = _clean(ad_id, "ad_id")

    await asyncio.to_thread(gads_remove_ad, _client(), cid, ag, aid)
    return _json({"status": "success", "message": f"Ad {aid} removed from ad group {ag}"})


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
