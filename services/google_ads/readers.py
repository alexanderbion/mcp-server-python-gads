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
