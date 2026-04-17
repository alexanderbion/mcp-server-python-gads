import os
from typing import List, Dict, Any
from google.ads.googleads.client import GoogleAdsClient
from .client import execute_with_retry

def list_accessible_customers(client: GoogleAdsClient) -> List[Dict[str, Any]]:
    """Lists all customer accounts accessible to the provided credentials."""
    customer_service = client.get_service("CustomerService")

    googleads_service = client.get_service("GoogleAdsService")

    query = """
        SELECT
          customer_client.client_customer,
          customer_client.descriptive_name,
          customer_client.level,
          customer_client.id
        FROM customer_client
        WHERE customer_client.status = 'ENABLED'
          AND customer_client.manager = FALSE
          AND customer_client.level > 0
    """

    login_customer_id = os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").replace("-", "")
    if not login_customer_id:
        return []

    search_request = client.get_type("SearchGoogleAdsRequest")
    search_request.customer_id = login_customer_id
    search_request.query = query

    results = googleads_service.search(request=search_request)

    accounts = []
    for row in results:
        accounts.append({
            "id": str(row.customer_client.id),
            "name": row.customer_client.descriptive_name or f"Account {row.customer_client.id}",
            "resource_name": row.customer_client.client_customer
        })

    return accounts


# ── Read functions for Campaign Optimizer ──────────────────────────────

def list_campaigns(client: GoogleAdsClient, customer_id: str) -> List[Dict[str, Any]]:
    """Lists all non-removed SEARCH campaigns for a customer."""
    query = """
        SELECT campaign.id, campaign.name, campaign.status,
               campaign.advertising_channel_type,
               campaign_budget.amount_micros, campaign.bidding_strategy_type
        FROM campaign
        WHERE campaign.status != 'REMOVED'
          AND campaign.advertising_channel_type = 'SEARCH'
        ORDER BY campaign.name
    """
    service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query

    results = execute_with_retry(service.search, request=request)
    campaigns = []
    for row in results:
        campaigns.append({
            "id": str(row.campaign.id),
            "name": row.campaign.name,
            "status": row.campaign.status.name,
            "budget_micros": row.campaign_budget.amount_micros,
            "bidding_strategy": row.campaign.bidding_strategy_type.name,
        })
    return campaigns


def list_ad_groups(client: GoogleAdsClient, customer_id: str, campaign_id: str) -> List[Dict[str, Any]]:
    """Lists all non-removed ad groups for a campaign."""
    query = f"""
        SELECT ad_group.id, ad_group.name, ad_group.status, ad_group.cpc_bid_micros
        FROM ad_group
        WHERE ad_group.campaign = 'customers/{customer_id}/campaigns/{campaign_id}'
          AND ad_group.status != 'REMOVED'
        ORDER BY ad_group.name
    """
    service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query

    results = execute_with_retry(service.search, request=request)
    ad_groups = []
    for row in results:
        ad_groups.append({
            "id": str(row.ad_group.id),
            "name": row.ad_group.name,
            "status": row.ad_group.status.name,
            "cpc_bid_micros": row.ad_group.cpc_bid_micros,
        })
    return ad_groups


def get_ad_group_ads(client: GoogleAdsClient, customer_id: str, ad_group_id: str) -> List[Dict[str, Any]]:
    """Gets RSA ads for an ad group."""
    query = f"""
        SELECT ad_group_ad.ad.id, ad_group_ad.ad.responsive_search_ad.headlines,
               ad_group_ad.ad.responsive_search_ad.descriptions, ad_group_ad.ad.final_urls,
               ad_group_ad.status
        FROM ad_group_ad
        WHERE ad_group_ad.ad_group = 'customers/{customer_id}/adGroups/{ad_group_id}'
          AND ad_group_ad.status != 'REMOVED'
          AND ad_group_ad.ad.type = 'RESPONSIVE_SEARCH_AD'
    """
    service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query

    results = execute_with_retry(service.search, request=request)
    ads = []
    for row in results:
        rsa = row.ad_group_ad.ad.responsive_search_ad
        ads.append({
            "id": str(row.ad_group_ad.ad.id),
            "headlines": [asset.text for asset in rsa.headlines],
            "descriptions": [asset.text for asset in rsa.descriptions],
            "final_urls": list(row.ad_group_ad.ad.final_urls),
            "status": row.ad_group_ad.status.name,
        })
    return ads


def get_ad_group_keywords(client: GoogleAdsClient, customer_id: str, ad_group_id: str) -> Dict[str, List[Dict[str, Any]]]:
    """Gets keywords for an ad group, split into positive and negative."""
    query = f"""
        SELECT ad_group_criterion.criterion_id, ad_group_criterion.keyword.text,
               ad_group_criterion.keyword.match_type, ad_group_criterion.negative,
               ad_group_criterion.status
        FROM ad_group_criterion
        WHERE ad_group_criterion.ad_group = 'customers/{customer_id}/adGroups/{ad_group_id}'
          AND ad_group_criterion.status != 'REMOVED'
          AND ad_group_criterion.type = 'KEYWORD'
    """
    service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query

    results = execute_with_retry(service.search, request=request)
    positive = []
    negative = []
    for row in results:
        kw = {
            "id": str(row.ad_group_criterion.criterion_id),
            "text": row.ad_group_criterion.keyword.text,
            "match_type": row.ad_group_criterion.keyword.match_type.name,
        }
        if row.ad_group_criterion.negative:
            negative.append(kw)
        else:
            positive.append(kw)
    return {"positive": positive, "negative": negative}


def get_campaign_extensions(client: GoogleAdsClient, customer_id: str, campaign_id: str) -> Dict[str, List[Any]]:
    """Gets sitelink, callout, and structured snippet extensions for a campaign."""
    query = f"""
        SELECT campaign_asset.field_type, asset.sitelink_asset.link_text,
               asset.sitelink_asset.description1, asset.sitelink_asset.description2,
               asset.final_urls, asset.callout_asset.callout_text,
               asset.structured_snippet_asset.header, asset.structured_snippet_asset.values
        FROM campaign_asset
        WHERE campaign_asset.campaign = 'customers/{customer_id}/campaigns/{campaign_id}'
          AND campaign_asset.status != 'REMOVED'
          AND campaign_asset.field_type IN ('SITELINK', 'CALLOUT', 'STRUCTURED_SNIPPET')
    """
    service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query

    results = execute_with_retry(service.search, request=request)
    sitelinks = []
    callouts = []
    structured_snippets = []
    for row in results:
        ft = row.campaign_asset.field_type.name
        if ft == "SITELINK":
            sitelinks.append({
                "link_text": row.asset.sitelink_asset.link_text,
                "description1": row.asset.sitelink_asset.description1,
                "description2": row.asset.sitelink_asset.description2,
                "final_urls": list(row.asset.final_urls),
            })
        elif ft == "CALLOUT":
            callouts.append({"text": row.asset.callout_asset.callout_text})
        elif ft == "STRUCTURED_SNIPPET":
            structured_snippets.append({
                "header": row.asset.structured_snippet_asset.header,
                "values": list(row.asset.structured_snippet_asset.values),
            })
    return {"sitelinks": sitelinks, "callouts": callouts, "structured_snippets": structured_snippets}


def get_campaign_targeting(client: GoogleAdsClient, customer_id: str, campaign_id: str) -> Dict[str, List[str]]:
    """Gets location and language targeting for a campaign."""
    query = f"""
        SELECT campaign_criterion.type, campaign_criterion.location.geo_target_constant,
               campaign_criterion.language.language_constant
        FROM campaign_criterion
        WHERE campaign_criterion.campaign = 'customers/{customer_id}/campaigns/{campaign_id}'
          AND campaign_criterion.status != 'REMOVED'
    """
    service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query

    results = execute_with_retry(service.search, request=request)
    geo_targets = []
    languages = []
    for row in results:
        ctype = row.campaign_criterion.type_.name
        if ctype == "LOCATION":
            geo_targets.append(row.campaign_criterion.location.geo_target_constant)
        elif ctype == "LANGUAGE":
            languages.append(row.campaign_criterion.language.language_constant)
    return {"geo_targets": geo_targets, "languages": languages}


# ── Read functions for Exclusions Tab ─────────────────────────────────

def get_top_keywords_by_cost(client: GoogleAdsClient, customer_id: str, campaign_id: str) -> List[Dict[str, Any]]:
    """Gets top 50 keywords by cost for a campaign over the last 30 days."""
    query = f"""
        SELECT
          ad_group_criterion.criterion_id,
          ad_group_criterion.keyword.text,
          ad_group_criterion.keyword.match_type,
          ad_group_criterion.status,
          ad_group.id,
          ad_group.name,
          metrics.impressions,
          metrics.clicks,
          metrics.ctr,
          metrics.conversions,
          metrics.all_conversions,
          metrics.cost_micros
        FROM keyword_view
        WHERE campaign.id = {campaign_id}
          AND ad_group_criterion.status != 'REMOVED'
          AND ad_group_criterion.negative = FALSE
          AND segments.date DURING LAST_30_DAYS
        ORDER BY metrics.cost_micros DESC
        LIMIT 50
    """
    service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query

    results = execute_with_retry(service.search, request=request)
    keywords = []
    for row in results:
        keywords.append({
            "criterion_id": str(row.ad_group_criterion.criterion_id),
            "keyword_text": row.ad_group_criterion.keyword.text,
            "match_type": row.ad_group_criterion.keyword.match_type.name,
            "status": row.ad_group_criterion.status.name,
            "ad_group_id": str(row.ad_group.id),
            "ad_group_name": row.ad_group.name,
            "impressions": row.metrics.impressions,
            "clicks": row.metrics.clicks,
            "ctr": round(row.metrics.ctr, 4),
            "conversions": round(row.metrics.conversions, 2),
            "all_conversions": round(row.metrics.all_conversions, 2),
            "cost": round(row.metrics.cost_micros / 1_000_000, 2),
        })
    return keywords


def get_top_search_terms_by_cost(client: GoogleAdsClient, customer_id: str, campaign_id: str) -> List[Dict[str, Any]]:
    """Gets top 50 search terms by cost for a campaign over the last 30 days."""
    query = f"""
        SELECT
          search_term_view.search_term,
          search_term_view.status,
          ad_group.id,
          ad_group.name,
          metrics.impressions,
          metrics.clicks,
          metrics.ctr,
          metrics.conversions,
          metrics.all_conversions,
          metrics.cost_micros
        FROM search_term_view
        WHERE campaign.id = {campaign_id}
          AND segments.date DURING LAST_30_DAYS
        ORDER BY metrics.cost_micros DESC
        LIMIT 50
    """
    service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query

    results = execute_with_retry(service.search, request=request)
    search_terms = []
    for row in results:
        search_terms.append({
            "search_term": row.search_term_view.search_term,
            "status": row.search_term_view.status.name,
            "ad_group_id": str(row.ad_group.id),
            "ad_group_name": row.ad_group.name,
            "impressions": row.metrics.impressions,
            "clicks": row.metrics.clicks,
            "ctr": round(row.metrics.ctr, 4),
            "conversions": round(row.metrics.conversions, 2),
            "all_conversions": round(row.metrics.all_conversions, 2),
            "cost": round(row.metrics.cost_micros / 1_000_000, 2),
        })
    return search_terms


def get_campaign_negative_keywords(client: GoogleAdsClient, customer_id: str, campaign_id: str) -> List[Dict[str, Any]]:
    """Gets campaign-level negative keywords."""
    query = f"""
        SELECT
          campaign_criterion.criterion_id,
          campaign_criterion.keyword.text,
          campaign_criterion.keyword.match_type
        FROM campaign_criterion
        WHERE campaign_criterion.campaign = 'customers/{customer_id}/campaigns/{campaign_id}'
          AND campaign_criterion.type = 'KEYWORD'
          AND campaign_criterion.negative = TRUE
          AND campaign_criterion.status != 'REMOVED'
    """
    service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query

    results = execute_with_retry(service.search, request=request)
    negatives = []
    for row in results:
        negatives.append({
            "criterion_id": str(row.campaign_criterion.criterion_id),
            "text": row.campaign_criterion.keyword.text,
            "match_type": row.campaign_criterion.keyword.match_type.name,
        })
    return negatives


def get_ad_group_negative_keywords(client: GoogleAdsClient, customer_id: str, campaign_id: str) -> List[Dict[str, Any]]:
    """Gets ad group-level negative keywords for all ad groups in a campaign."""
    query = f"""
        SELECT
          ad_group_criterion.criterion_id,
          ad_group_criterion.keyword.text,
          ad_group_criterion.keyword.match_type,
          ad_group.id,
          ad_group.name
        FROM ad_group_criterion
        WHERE campaign.id = {campaign_id}
          AND ad_group_criterion.negative = TRUE
          AND ad_group_criterion.status != 'REMOVED'
          AND ad_group_criterion.type = 'KEYWORD'
    """
    service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query

    results = execute_with_retry(service.search, request=request)
    negatives = []
    for row in results:
        negatives.append({
            "criterion_id": str(row.ad_group_criterion.criterion_id),
            "text": row.ad_group_criterion.keyword.text,
            "match_type": row.ad_group_criterion.keyword.match_type.name,
            "ad_group_id": str(row.ad_group.id),
            "ad_group_name": row.ad_group.name,
        })
    return negatives


# ── Helpers for write ops ───────────────────────────────────────────────


def get_campaign_budget_resource(
    client: GoogleAdsClient, customer_id: str, campaign_id: str
) -> str | None:
    """Returns the campaign_budget resource_name for a campaign, or None if not found."""
    query = f"""
        SELECT campaign.campaign_budget
        FROM campaign
        WHERE campaign.id = {campaign_id}
        LIMIT 1
    """
    service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query
    for row in execute_with_retry(service.search, request=request):
        return row.campaign.campaign_budget
    return None


def list_campaign_targeting_criteria(
    client: GoogleAdsClient, customer_id: str, campaign_id: str
) -> List[Dict[str, Any]]:
    """Returns location/language criteria with their resource_names so they can be removed.

    Includes negative=True location exclusions so update_targeting can also remove
    excluded geos by ID if needed in the future.
    """
    query = f"""
        SELECT campaign_criterion.resource_name,
               campaign_criterion.type,
               campaign_criterion.negative,
               campaign_criterion.location.geo_target_constant,
               campaign_criterion.language.language_constant
        FROM campaign_criterion
        WHERE campaign_criterion.campaign = 'customers/{customer_id}/campaigns/{campaign_id}'
          AND campaign_criterion.status != 'REMOVED'
    """
    service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query

    items = []
    for row in execute_with_retry(service.search, request=request):
        c = row.campaign_criterion
        item = {
            "resource_name": c.resource_name,
            "type": c.type_.name,
            "negative": bool(c.negative),
        }
        if c.type_.name == "LOCATION":
            geo = c.location.geo_target_constant
            item["geo_target_id"] = int(geo.split("/")[-1]) if geo else None
        elif c.type_.name == "LANGUAGE":
            lang = c.language.language_constant
            item["language_id"] = int(lang.split("/")[-1]) if lang else None
        items.append(item)
    return items


def get_campaign_basic_info(
    client: GoogleAdsClient, customer_id: str, campaign_id: str
) -> Dict[str, Any] | None:
    """Returns name, status, bidding_strategy_type, target_cpa/roas, and budget_micros."""
    query = f"""
        SELECT campaign.id, campaign.name, campaign.status,
               campaign.bidding_strategy_type,
               campaign.target_cpa.target_cpa_micros,
               campaign.target_roas.target_roas,
               campaign_budget.amount_micros
        FROM campaign
        WHERE campaign.id = {campaign_id}
        LIMIT 1
    """
    service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query
    for row in execute_with_retry(service.search, request=request):
        return {
            "id": str(row.campaign.id),
            "name": row.campaign.name,
            "status": row.campaign.status.name,
            "bidding_strategy": row.campaign.bidding_strategy_type.name,
            "target_cpa_micros": row.campaign.target_cpa.target_cpa_micros,
            "target_roas": row.campaign.target_roas.target_roas,
            "budget_micros": row.campaign_budget.amount_micros,
        }
    return None


def load_campaign_full_config(
    client: GoogleAdsClient, customer_id: str, campaign_id: str
) -> Dict[str, Any] | None:
    """Reads everything needed to clone a campaign.

    Returns a dict with: name, status, bidding_strategy, target_cpa_micros,
    target_roas, budget_micros, geo_target_ids, language_ids,
    excluded_geo_target_ids, ip_blocks, campaign_negatives, ad_groups (each
    with ads, keywords, negative_keywords), extensions.
    """
    info = get_campaign_basic_info(client, customer_id, campaign_id)
    if not info:
        return None

    # Targeting (positive geos/langs + negative location/IP)
    targeting_query = f"""
        SELECT campaign_criterion.type,
               campaign_criterion.negative,
               campaign_criterion.location.geo_target_constant,
               campaign_criterion.language.language_constant,
               campaign_criterion.ip_block.ip_address
        FROM campaign_criterion
        WHERE campaign_criterion.campaign = 'customers/{customer_id}/campaigns/{campaign_id}'
          AND campaign_criterion.status != 'REMOVED'
    """
    service = client.get_service("GoogleAdsService")
    req = client.get_type("SearchGoogleAdsRequest")
    req.customer_id = customer_id
    req.query = targeting_query

    geo_ids: list[int] = []
    lang_ids: list[int] = []
    excluded_geos: list[int] = []
    ip_blocks: list[str] = []
    for row in execute_with_retry(service.search, request=req):
        c = row.campaign_criterion
        t = c.type_.name
        if t == "LOCATION":
            geo = c.location.geo_target_constant
            if not geo:
                continue
            gid = int(geo.split("/")[-1])
            (excluded_geos if c.negative else geo_ids).append(gid)
        elif t == "LANGUAGE":
            lang = c.language.language_constant
            if lang and not c.negative:
                lang_ids.append(int(lang.split("/")[-1]))
        elif t == "IP_BLOCK":
            ip = c.ip_block.ip_address
            if ip and c.negative:
                ip_blocks.append(ip)

    # Campaign-level negative keywords
    campaign_negatives = get_campaign_negative_keywords(client, customer_id, campaign_id)
    campaign_negs_for_clone = [
        {"text": n["text"], "match_type": n["match_type"]} for n in campaign_negatives
    ]

    # Ad groups + each ad group's ads, keywords, negatives
    ag_query = f"""
        SELECT ad_group.id, ad_group.name, ad_group.status, ad_group.cpc_bid_micros
        FROM ad_group
        WHERE ad_group.campaign = 'customers/{customer_id}/campaigns/{campaign_id}'
          AND ad_group.status != 'REMOVED'
        ORDER BY ad_group.name
    """
    req = client.get_type("SearchGoogleAdsRequest")
    req.customer_id = customer_id
    req.query = ag_query

    ad_groups: list[dict] = []
    for row in execute_with_retry(service.search, request=req):
        ag_id = str(row.ad_group.id)
        ad_groups.append({
            "id": ag_id,
            "name": row.ad_group.name,
            "status": row.ad_group.status.name,
            "cpc_bid_micros": row.ad_group.cpc_bid_micros,
            "ads": get_ad_group_ads(client, customer_id, ag_id),
            "keywords": [],
            "negative_keywords": [],
        })

    for ag in ad_groups:
        kws = get_ad_group_keywords(client, customer_id, ag["id"])
        ag["keywords"] = [
            {"text": k["text"], "match_type": k["match_type"]} for k in kws["positive"]
        ]
        ag["negative_keywords"] = [
            {"text": k["text"], "match_type": k["match_type"]} for k in kws["negative"]
        ]

    extensions = get_campaign_extensions(client, customer_id, campaign_id)

    return {
        "name": info["name"],
        "status": info["status"],
        "bidding_strategy": info["bidding_strategy"],
        "target_cpa_micros": info["target_cpa_micros"],
        "target_roas": info["target_roas"],
        "budget_micros": info["budget_micros"],
        "geo_target_ids": geo_ids,
        "language_ids": lang_ids,
        "excluded_geo_target_ids": excluded_geos,
        "ip_blocks": ip_blocks,
        "campaign_negatives": campaign_negs_for_clone,
        "ad_groups": ad_groups,
        "extensions": extensions,
    }
