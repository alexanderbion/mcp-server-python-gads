"""
Builder functions for creating Google Ads campaign structure.

All multi-resource creates use a single MutateGoogleAdsRequest so the operation
is atomic — partial failures can't leave orphaned resources behind. Within one
batched mutate, resources reference each other through negative temp-id
resource_names which the API resolves on commit.
"""
import time
from typing import Optional, List, Dict, Any, Tuple
from google.ads.googleads.client import GoogleAdsClient
from .client import execute_with_retry


# ─── Status helpers ─────────────────────────────────────────────────────


def _campaign_status_enum(client: GoogleAdsClient, status: str):
    return getattr(client.enums.CampaignStatusEnum, status.upper())


def _ad_group_status_enum(client: GoogleAdsClient, status: str):
    return getattr(client.enums.AdGroupStatusEnum, status.upper())


def _ad_status_enum(client: GoogleAdsClient, status: str):
    return getattr(client.enums.AdGroupAdStatusEnum, status.upper())


def _criterion_status_enum(client: GoogleAdsClient, status: str):
    return getattr(client.enums.AdGroupCriterionStatusEnum, status.upper())


# ─── Bidding strategy helper ─────────────────────────────────────────────


def _apply_bidding_strategy(
    client: GoogleAdsClient,
    campaign,
    strategy: str,
    target_cpa: Optional[float] = None,
    target_roas: Optional[float] = None,
) -> None:
    """Apply a bidding strategy to a campaign proto by setting the right oneof field.

    TARGET_CPA / TARGET_ROAS are routed through MaximizeConversions /
    MaximizeConversionValue with the optional target subfield populated —
    standalone TargetCpa / TargetRoas were deprecated for Search campaigns
    in September 2021:
    https://ads-developers.googleblog.com/2021/07/updates-to-how-google-ads-api-smart.html
    """
    s = strategy.upper()
    if s == "ENHANCED_CPC":
        raise ValueError(
            "ENHANCED_CPC was deprecated by Google Ads in October 2022. "
            "Use MAXIMIZE_CONVERSIONS or MAXIMIZE_CONVERSION_VALUE instead."
        )
    if s == "MAXIMIZE_CLICKS":
        campaign.target_spend = client.get_type("TargetSpend")
    elif s == "MAXIMIZE_CONVERSIONS":
        campaign.maximize_conversions = client.get_type("MaximizeConversions")
    elif s == "MAXIMIZE_CONVERSION_VALUE":
        campaign.maximize_conversion_value = client.get_type("MaximizeConversionValue")
    elif s == "TARGET_CPA":
        if target_cpa is None:
            raise ValueError("target_cpa is required when bidding_strategy=TARGET_CPA")
        mc = client.get_type("MaximizeConversions")
        mc.target_cpa_micros = int(round(target_cpa * 1_000_000))
        campaign.maximize_conversions = mc
    elif s == "TARGET_ROAS":
        if target_roas is None:
            raise ValueError("target_roas is required when bidding_strategy=TARGET_ROAS")
        mcv = client.get_type("MaximizeConversionValue")
        mcv.target_roas = target_roas
        campaign.maximize_conversion_value = mcv
    else:
        campaign.manual_cpc = client.get_type("ManualCpc")


# ─── URL helper ─────────────────────────────────────────────────────────


def _normalize_url(url: str) -> str:
    if url and not url.startswith(("http://", "https://")):
        return "https://" + url
    return url


# ─── Operation builders (return MutateOperation lists) ──────────────────


def _build_rsa_mutate_operation(
    client: GoogleAdsClient,
    ad_group_resource: str,
    headlines: List[str],
    descriptions: List[str],
    final_url: str,
    status: str,
):
    """Build a MutateOperation that creates one RSA in the given ad group."""
    op = client.get_type("MutateOperation")
    ad_group_ad = op.ad_group_ad_operation.create
    ad_group_ad.ad_group = ad_group_resource
    ad_group_ad.status = _ad_status_enum(client, status)

    ad_group_ad.ad.final_urls.append(_normalize_url(final_url))
    rsa = ad_group_ad.ad.responsive_search_ad
    for h in headlines[:15]:
        asset = client.get_type("AdTextAsset")
        asset.text = h[:30]
        rsa.headlines.append(asset)
    for d in descriptions[:4]:
        asset = client.get_type("AdTextAsset")
        asset.text = d[:90]
        rsa.descriptions.append(asset)
    return op


def _build_keyword_mutate_operations(
    client: GoogleAdsClient,
    ad_group_resource: str,
    keywords: List[Dict[str, Any]],
    status: str = "PAUSED",
) -> list:
    """Build MutateOperations for ad-group-level keywords (positive or negative).

    Each keyword dict: {"text": ..., "match_type": "...", "negative": bool (optional)}.
    """
    ops = []
    for kw in keywords:
        op = client.get_type("MutateOperation")
        criterion = op.ad_group_criterion_operation.create
        criterion.ad_group = ad_group_resource
        criterion.keyword.text = kw["text"]
        criterion.keyword.match_type = getattr(
            client.enums.KeywordMatchTypeEnum, kw["match_type"].upper()
        )
        if kw.get("negative"):
            criterion.negative = True
        else:
            criterion.status = _criterion_status_enum(client, status)
        ops.append(op)
    return ops


def _build_campaign_negative_mutate_operations(
    client: GoogleAdsClient,
    campaign_resource: str,
    keywords: List[Dict[str, Any]],
) -> list:
    """Build MutateOperations for campaign-level negative keywords."""
    ops = []
    for kw in keywords:
        op = client.get_type("MutateOperation")
        criterion = op.campaign_criterion_operation.create
        criterion.campaign = campaign_resource
        criterion.negative = True
        criterion.keyword.text = kw["text"]
        criterion.keyword.match_type = getattr(
            client.enums.KeywordMatchTypeEnum, kw["match_type"].upper()
        )
        ops.append(op)
    return ops


def _build_extension_mutate_operations(
    client: GoogleAdsClient,
    customer_id: str,
    campaign_resource: str,
    sitelinks: List[Dict[str, Any]],
    callouts: List[Dict[str, str]],
    snippets: List[Dict[str, Any]],
    fallback_url: str,
    next_temp_id,
) -> list:
    """Build MutateOperations for sitelinks/callouts/snippets and their links."""
    ops = []
    for sl in sitelinks:
        asset_resource = f"customers/{customer_id}/assets/{next_temp_id()}"

        asset_op = client.get_type("MutateOperation")
        asset = asset_op.asset_operation.create
        asset.resource_name = asset_resource
        asset.sitelink_asset.link_text = sl["link_text"][:25]
        asset.sitelink_asset.description1 = sl.get("description1", "")[:35]
        asset.sitelink_asset.description2 = sl.get("description2", "")[:35]
        sl_url = _normalize_url(sl.get("final_url", fallback_url))
        asset.final_urls.append(sl_url)
        ops.append(asset_op)

        link_op = client.get_type("MutateOperation")
        link_op.campaign_asset_operation.create.campaign = campaign_resource
        link_op.campaign_asset_operation.create.asset = asset_resource
        link_op.campaign_asset_operation.create.field_type = client.enums.AssetFieldTypeEnum.SITELINK
        ops.append(link_op)

    for co in callouts:
        asset_resource = f"customers/{customer_id}/assets/{next_temp_id()}"

        asset_op = client.get_type("MutateOperation")
        asset = asset_op.asset_operation.create
        asset.resource_name = asset_resource
        asset.callout_asset.callout_text = co["text"][:25]
        ops.append(asset_op)

        link_op = client.get_type("MutateOperation")
        link_op.campaign_asset_operation.create.campaign = campaign_resource
        link_op.campaign_asset_operation.create.asset = asset_resource
        link_op.campaign_asset_operation.create.field_type = client.enums.AssetFieldTypeEnum.CALLOUT
        ops.append(link_op)

    for sn in snippets:
        asset_resource = f"customers/{customer_id}/assets/{next_temp_id()}"

        asset_op = client.get_type("MutateOperation")
        asset = asset_op.asset_operation.create
        asset.resource_name = asset_resource
        asset.structured_snippet_asset.header = sn["header"]
        for val in sn.get("values", []):
            asset.structured_snippet_asset.values.append(val[:25])
        ops.append(asset_op)

        link_op = client.get_type("MutateOperation")
        link_op.campaign_asset_operation.create.campaign = campaign_resource
        link_op.campaign_asset_operation.create.asset = asset_resource
        link_op.campaign_asset_operation.create.field_type = client.enums.AssetFieldTypeEnum.STRUCTURED_SNIPPET
        ops.append(link_op)

    return ops


def _make_temp_id_factory():
    """Returns a function that yields decreasing negative integer IDs as strings."""
    counter = [-1]
    def next_temp_id() -> str:
        val = counter[0]
        counter[0] -= 1
        return str(val)
    return next_temp_id


def _execute_mutate_google_ads(
    client: GoogleAdsClient, customer_id: str, mutate_operations: list,
):
    """Run a MutateGoogleAdsRequest with validate-then-execute, returning the response."""
    googleads_service = client.get_service("GoogleAdsService")

    request = client.get_type("MutateGoogleAdsRequest")
    request.customer_id = customer_id
    request.mutate_operations.extend(mutate_operations)
    request.partial_failure = False
    request.validate_only = True
    execute_with_retry(googleads_service.mutate, request=request)

    request.validate_only = False
    return execute_with_retry(googleads_service.mutate, request=request)


# ─── Public builders ────────────────────────────────────────────────────


def create_paused_campaign(
    client: GoogleAdsClient, customer_id: str, campaign_name: str,
    budget_micros: int, headlines: List[str], descriptions: List[str],
    keywords: List[Dict[str, str]],
    campaign_negative_keywords: List[Dict[str, str]],
    ad_group_negative_keywords: List[Dict[str, str]],
    language_ids: List[int],
    geo_target_ids: List[int],
    final_url: str = "",
    bidding_strategy: str = "MANUAL_CPC",
    target_cpa: Optional[float] = None,
    target_roas: Optional[float] = None,
    sitelinks: Optional[List[Dict[str, Any]]] = None,
    callouts: Optional[List[Dict[str, str]]] = None,
    structured_snippets: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Creates a fully formed SEARCH campaign atomically using GoogleAdsService.mutate.

    Everything is created in PAUSED status. Returns the new campaign resource_name.
    """
    sitelinks = sitelinks or []
    callouts = callouts or []
    structured_snippets = structured_snippets or []
    campaign_negative_keywords = campaign_negative_keywords or []
    ad_group_negative_keywords = ad_group_negative_keywords or []
    final_url = _normalize_url(final_url)

    next_temp_id = _make_temp_id_factory()
    mutate_operations = []

    # 1. Budget — explicitly_shared=False so Smart Bidding strategies that require
    # a dedicated budget (MAXIMIZE_CONVERSIONS, MAXIMIZE_CONVERSION_VALUE, TARGET_*)
    # don't get rejected with "incompatible with shared budget".
    budget_resource = f"customers/{customer_id}/campaignBudgets/{next_temp_id()}"
    budget_op = client.get_type("MutateOperation")
    budget = budget_op.campaign_budget_operation.create
    budget.resource_name = budget_resource
    budget.name = f"Budget for {campaign_name} ({int(time.time())})"
    budget.amount_micros = budget_micros
    budget.delivery_method = client.enums.BudgetDeliveryMethodEnum.STANDARD
    budget.explicitly_shared = False
    mutate_operations.append(budget_op)

    # 2. Campaign
    campaign_resource = f"customers/{customer_id}/campaigns/{next_temp_id()}"
    campaign_op = client.get_type("MutateOperation")
    campaign = campaign_op.campaign_operation.create
    campaign.resource_name = campaign_resource
    campaign.name = campaign_name
    campaign.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.SEARCH
    campaign.status = client.enums.CampaignStatusEnum.PAUSED
    campaign.campaign_budget = budget_resource
    _apply_bidding_strategy(client, campaign, bidding_strategy, target_cpa, target_roas)
    campaign.contains_eu_political_advertising = (
        client.enums.EuPoliticalAdvertisingStatusEnum.DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING
    )
    campaign.network_settings.target_google_search = True
    campaign.network_settings.target_search_network = True
    campaign.network_settings.target_content_network = False
    mutate_operations.append(campaign_op)

    # 3. Targeting (multiple geos and languages)
    for lang_id in language_ids:
        lang_op = client.get_type("MutateOperation")
        lang_op.campaign_criterion_operation.create.campaign = campaign_resource
        lang_op.campaign_criterion_operation.create.language.language_constant = (
            f"languageConstants/{lang_id}"
        )
        mutate_operations.append(lang_op)

    for geo_id in geo_target_ids:
        geo_op = client.get_type("MutateOperation")
        geo_op.campaign_criterion_operation.create.campaign = campaign_resource
        geo_op.campaign_criterion_operation.create.location.geo_target_constant = (
            f"geoTargetConstants/{geo_id}"
        )
        mutate_operations.append(geo_op)

    # 4. Ad Group
    ad_group_resource = f"customers/{customer_id}/adGroups/{next_temp_id()}"
    ag_op = client.get_type("MutateOperation")
    ad_group = ag_op.ad_group_operation.create
    ad_group.resource_name = ad_group_resource
    ad_group.name = f"Main Ad Group - {campaign_name}"
    ad_group.campaign = campaign_resource
    ad_group.status = client.enums.AdGroupStatusEnum.PAUSED
    ad_group.type = client.enums.AdGroupTypeEnum.SEARCH_STANDARD
    ad_group.cpc_bid_micros = 1_000_000
    mutate_operations.append(ag_op)

    # 5. RSA
    mutate_operations.append(
        _build_rsa_mutate_operation(
            client, ad_group_resource, headlines, descriptions, final_url, "PAUSED"
        )
    )

    # 6. Positive keywords (created PAUSED so they don't fire when campaign is enabled
    # before review)
    if keywords:
        mutate_operations.extend(
            _build_keyword_mutate_operations(client, ad_group_resource, keywords, "PAUSED")
        )

    # 7. Campaign-level negative keywords
    if campaign_negative_keywords:
        mutate_operations.extend(
            _build_campaign_negative_mutate_operations(
                client, campaign_resource, campaign_negative_keywords
            )
        )

    # 8. Ad-group-level negative keywords
    if ad_group_negative_keywords:
        # Force negative=True regardless of input flags
        ag_negs = [{**kw, "negative": True} for kw in ad_group_negative_keywords]
        mutate_operations.extend(
            _build_keyword_mutate_operations(client, ad_group_resource, ag_negs, "PAUSED")
        )

    # 9. Extensions (sitelinks + callouts + structured snippets)
    mutate_operations.extend(
        _build_extension_mutate_operations(
            client, customer_id, campaign_resource,
            sitelinks, callouts, structured_snippets, final_url, next_temp_id,
        )
    )

    response = _execute_mutate_google_ads(client, customer_id, mutate_operations)

    for resp in response.mutate_operation_responses:
        if resp.campaign_result.resource_name:
            return resp.campaign_result.resource_name
    return "Unknown Campaign Resource"


def add_ad_group(
    client: GoogleAdsClient, customer_id: str, campaign_id: str,
    ad_group_name: str,
    cpc_bid_micros: int = 1_000_000,
    status: str = "PAUSED",
    headlines: Optional[List[str]] = None,
    descriptions: Optional[List[str]] = None,
    final_url: Optional[str] = None,
    keywords: Optional[List[Dict[str, Any]]] = None,
    negative_keywords: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, Optional[str]]:
    """Creates a new ad group inside an existing campaign with optional RSA + keywords.

    Atomic: builds one MutateGoogleAdsRequest. If RSA fields are present
    (headlines + descriptions + final_url) an RSA is created; otherwise just the
    ad group + keywords. Returns {"ad_group_id": "...", "ad_id": "..."|None}.
    """
    keywords = keywords or []
    negative_keywords = negative_keywords or []
    campaign_resource = f"customers/{customer_id}/campaigns/{campaign_id}"

    next_temp_id = _make_temp_id_factory()
    mutate_operations = []

    ad_group_resource = f"customers/{customer_id}/adGroups/{next_temp_id()}"
    ag_op = client.get_type("MutateOperation")
    ad_group = ag_op.ad_group_operation.create
    ad_group.resource_name = ad_group_resource
    ad_group.name = ad_group_name
    ad_group.campaign = campaign_resource
    ad_group.status = _ad_group_status_enum(client, status)
    ad_group.type = client.enums.AdGroupTypeEnum.SEARCH_STANDARD
    ad_group.cpc_bid_micros = cpc_bid_micros
    mutate_operations.append(ag_op)

    create_rsa = bool(headlines and descriptions and final_url)
    if create_rsa:
        mutate_operations.append(
            _build_rsa_mutate_operation(
                client, ad_group_resource, headlines, descriptions, final_url, status
            )
        )

    if keywords:
        mutate_operations.extend(
            _build_keyword_mutate_operations(client, ad_group_resource, keywords, status)
        )
    if negative_keywords:
        ag_negs = [{**kw, "negative": True} for kw in negative_keywords]
        mutate_operations.extend(
            _build_keyword_mutate_operations(client, ad_group_resource, ag_negs, status)
        )

    response = _execute_mutate_google_ads(client, customer_id, mutate_operations)

    new_ad_group_id = None
    new_ad_id = None
    for resp in response.mutate_operation_responses:
        if resp.ad_group_result.resource_name and new_ad_group_id is None:
            new_ad_group_id = resp.ad_group_result.resource_name.split("/")[-1]
        if resp.ad_group_ad_result.resource_name and new_ad_id is None:
            new_ad_id = resp.ad_group_ad_result.resource_name.split("~")[-1]
    return {"ad_group_id": new_ad_group_id, "ad_id": new_ad_id}


def create_rsa_ad(
    client: GoogleAdsClient, customer_id: str, ad_group_id: str,
    headlines: List[str], descriptions: List[str], final_url: str,
    status: str = "PAUSED",
) -> Dict[str, str]:
    """Adds a new RSA to an existing ad group (alongside any existing ads).

    Distinct from update_rsa_ad: this just creates a new ad without removing
    the old one — used for A/B ad testing where you want 2+ RSAs in the same
    ad group.
    """
    ad_group_resource = f"customers/{customer_id}/adGroups/{ad_group_id}"
    op = _build_rsa_mutate_operation(
        client, ad_group_resource, headlines, descriptions, final_url, status
    )

    response = _execute_mutate_google_ads(client, customer_id, [op])
    for resp in response.mutate_operation_responses:
        if resp.ad_group_ad_result.resource_name:
            new_ad_id = resp.ad_group_ad_result.resource_name.split("~")[-1]
            return {"ad_id": new_ad_id, "ad_group_id": ad_group_id}
    raise RuntimeError("RSA creation succeeded but resource_name was missing from response")


def clone_campaign(
    client: GoogleAdsClient, customer_id: str, source_campaign_id: str,
    new_campaign_name: str,
    new_budget_micros: Optional[int] = None,
    new_geo_target_ids: Optional[List[int]] = None,
    new_language_ids: Optional[List[int]] = None,
    new_bidding_strategy: Optional[str] = None,
    new_target_cpa: Optional[float] = None,
    new_target_roas: Optional[float] = None,
    new_status: str = "PAUSED",
) -> Dict[str, Any]:
    """Atomically clone an entire SEARCH campaign with optional overrides.

    Reads source structure (campaign + ad groups + ads + keywords + negatives at
    both levels + extensions + targeting + budget) then builds one batched
    MutateGoogleAdsRequest so a partial clone can't leave orphaned resources.
    Returns {"new_campaign_id": "...", "cloned": {...counts}}.
    """
    from .readers import load_campaign_full_config  # local import to avoid cycle

    cfg = load_campaign_full_config(client, customer_id, source_campaign_id)
    if not cfg:
        raise ValueError(
            f"Source campaign {source_campaign_id} not found in account {customer_id}"
        )

    bidding_strategy = (new_bidding_strategy or cfg["bidding_strategy"]).upper()
    geo_ids = new_geo_target_ids if new_geo_target_ids is not None else cfg["geo_target_ids"]
    lang_ids = new_language_ids if new_language_ids is not None else cfg["language_ids"]
    budget_micros = new_budget_micros if new_budget_micros is not None else cfg["budget_micros"]

    next_temp_id = _make_temp_id_factory()
    mutate_operations = []

    # 1. Budget — explicitly_shared=False so Smart Bidding strategies that require
    # a dedicated budget don't get rejected with "incompatible with shared budget".
    budget_resource = f"customers/{customer_id}/campaignBudgets/{next_temp_id()}"
    budget_op = client.get_type("MutateOperation")
    budget = budget_op.campaign_budget_operation.create
    budget.resource_name = budget_resource
    budget.name = f"Budget for {new_campaign_name} ({int(time.time())})"
    budget.amount_micros = budget_micros
    budget.delivery_method = client.enums.BudgetDeliveryMethodEnum.STANDARD
    budget.explicitly_shared = False
    mutate_operations.append(budget_op)

    # 2. Campaign
    campaign_resource = f"customers/{customer_id}/campaigns/{next_temp_id()}"
    campaign_op = client.get_type("MutateOperation")
    campaign = campaign_op.campaign_operation.create
    campaign.resource_name = campaign_resource
    campaign.name = new_campaign_name
    campaign.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.SEARCH
    campaign.status = _campaign_status_enum(client, new_status)
    campaign.campaign_budget = budget_resource
    _apply_bidding_strategy(client, campaign, bidding_strategy, new_target_cpa, new_target_roas)
    campaign.contains_eu_political_advertising = (
        client.enums.EuPoliticalAdvertisingStatusEnum.DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING
    )
    campaign.network_settings.target_google_search = True
    campaign.network_settings.target_search_network = True
    campaign.network_settings.target_content_network = False
    mutate_operations.append(campaign_op)

    # 3. Targeting (positive geos + langs)
    for lang_id in lang_ids:
        op = client.get_type("MutateOperation")
        op.campaign_criterion_operation.create.campaign = campaign_resource
        op.campaign_criterion_operation.create.language.language_constant = (
            f"languageConstants/{lang_id}"
        )
        mutate_operations.append(op)

    for geo_id in geo_ids:
        op = client.get_type("MutateOperation")
        op.campaign_criterion_operation.create.campaign = campaign_resource
        op.campaign_criterion_operation.create.location.geo_target_constant = (
            f"geoTargetConstants/{geo_id}"
        )
        mutate_operations.append(op)

    # Negative geos (excluded locations) — copy from source unless geo override
    # also wants to drop them. Caller's override semantics only touch positive
    # geos; negative-location exclusions are always preserved.
    for excl_geo in cfg.get("excluded_geo_target_ids", []):
        op = client.get_type("MutateOperation")
        c = op.campaign_criterion_operation.create
        c.campaign = campaign_resource
        c.negative = True
        c.location.geo_target_constant = f"geoTargetConstants/{excl_geo}"
        mutate_operations.append(op)

    # IP exclusions
    for ip in cfg.get("ip_blocks", []):
        op = client.get_type("MutateOperation")
        c = op.campaign_criterion_operation.create
        c.campaign = campaign_resource
        c.negative = True
        c.ip_block.ip_address = ip
        mutate_operations.append(op)

    # 4. Campaign-level negative keywords
    if cfg.get("campaign_negatives"):
        mutate_operations.extend(
            _build_campaign_negative_mutate_operations(
                client, campaign_resource, cfg["campaign_negatives"]
            )
        )

    # 5. Ad groups + ads + keywords + ad-group-level negatives
    counts = {"ad_groups": 0, "ads": 0, "keywords": 0, "negatives": 0, "extensions": 0}
    counts["negatives"] += len(cfg.get("campaign_negatives", []))

    for ag in cfg["ad_groups"]:
        ag_resource = f"customers/{customer_id}/adGroups/{next_temp_id()}"
        ag_op = client.get_type("MutateOperation")
        ad_group = ag_op.ad_group_operation.create
        ad_group.resource_name = ag_resource
        ad_group.name = ag["name"]
        ad_group.campaign = campaign_resource
        ad_group.status = _ad_group_status_enum(client, ag.get("status", "PAUSED"))
        ad_group.type = client.enums.AdGroupTypeEnum.SEARCH_STANDARD
        ad_group.cpc_bid_micros = ag.get("cpc_bid_micros") or 1_000_000
        mutate_operations.append(ag_op)
        counts["ad_groups"] += 1

        for ad in ag.get("ads", []):
            mutate_operations.append(
                _build_rsa_mutate_operation(
                    client, ag_resource,
                    ad["headlines"], ad["descriptions"],
                    ad["final_urls"][0] if ad["final_urls"] else "",
                    ad.get("status", "PAUSED"),
                )
            )
            counts["ads"] += 1

        if ag.get("keywords"):
            mutate_operations.extend(
                _build_keyword_mutate_operations(
                    client, ag_resource, ag["keywords"], "PAUSED"
                )
            )
            counts["keywords"] += len(ag["keywords"])

        if ag.get("negative_keywords"):
            negs = [{**kw, "negative": True} for kw in ag["negative_keywords"]]
            mutate_operations.extend(
                _build_keyword_mutate_operations(client, ag_resource, negs, "PAUSED")
            )
            counts["negatives"] += len(negs)

    # 6. Extensions (campaign-level)
    extensions = cfg.get("extensions", {})
    sl = extensions.get("sitelinks", [])
    co = extensions.get("callouts", [])
    sn = extensions.get("structured_snippets", [])
    counts["extensions"] = len(sl) + len(co) + len(sn)
    fallback_url = ""
    for ag in cfg["ad_groups"]:
        for ad in ag.get("ads", []):
            if ad["final_urls"]:
                fallback_url = ad["final_urls"][0]
                break
        if fallback_url:
            break
    mutate_operations.extend(
        _build_extension_mutate_operations(
            client, customer_id, campaign_resource,
            sl, co, sn, fallback_url, next_temp_id,
        )
    )

    response = _execute_mutate_google_ads(client, customer_id, mutate_operations)

    new_campaign_id = None
    for resp in response.mutate_operation_responses:
        if resp.campaign_result.resource_name:
            new_campaign_id = resp.campaign_result.resource_name.split("/")[-1]
            break

    return {"new_campaign_id": new_campaign_id, "cloned": counts}
