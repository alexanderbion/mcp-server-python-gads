from typing import List, Dict, Any, Optional
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from google.protobuf.field_mask_pb2 import FieldMask
from google.api_core import protobuf_helpers
from .client import execute_with_retry
from .readers import (
    get_campaign_budget_resource,
    list_campaign_targeting_criteria,
)


def _mutate(client: GoogleAdsClient, service_method, request_type_name: str,
            customer_id: str, operations: list):
    """Validate then execute a mutate call using a typed request object.

    v23 SDK no longer accepts validate_only as a kwarg on mutate_* methods —
    it must be set on the typed request.
    """
    request = client.get_type(request_type_name)
    request.customer_id = customer_id
    request.operations.extend(operations)
    request.validate_only = True
    execute_with_retry(service_method, request=request)
    request.validate_only = False
    return execute_with_retry(service_method, request=request)


def add_negative_keywords(client: GoogleAdsClient, customer_id: str, campaign_id: str,
                         keywords: List[str], match_type: str = "PHRASE") -> None:
    """Applies negative keywords to an existing campaign with retry logic."""
    service = client.get_service("CampaignCriterionService")
    campaign_path = client.get_service("CampaignService").campaign_path(customer_id, campaign_id)

    ops = []
    for kw in keywords:
        op = client.get_type("CampaignCriterionOperation")
        criterion = op.create
        criterion.campaign = campaign_path
        criterion.negative = True
        criterion.keyword.text = kw
        criterion.keyword.match_type = getattr(client.enums.KeywordMatchTypeEnum, match_type.upper())
        ops.append(op)

    _mutate(client, service.mutate_campaign_criteria, "MutateCampaignCriteriaRequest", customer_id, ops)


def add_sitelinks_to_campaign(
    client: GoogleAdsClient, customer_id: str, campaign_resource: str,
    sitelinks: List[Dict[str, Any]], final_url: str = ""
) -> None:
    """Adds sitelink assets and links them to a campaign."""
    asset_service = client.get_service("AssetService")
    campaign_asset_service = client.get_service("CampaignAssetService")

    sitelink_ops = []
    for sl in sitelinks:
        asset_op = client.get_type("AssetOperation")
        asset = asset_op.create
        asset.sitelink_asset.link_text = sl["link_text"][:25]
        asset.sitelink_asset.description1 = sl.get("description1", "")[:35]
        asset.sitelink_asset.description2 = sl.get("description2", "")[:35]
        sl_url = sl.get("final_url", final_url)
        if sl_url and not sl_url.startswith(("http://", "https://")):
            sl_url = "https://" + sl_url
        asset.final_urls.append(sl_url)
        sitelink_ops.append(asset_op)

    response = _mutate(client, asset_service.mutate_assets, "MutateAssetsRequest", customer_id, sitelink_ops)

    link_ops = []
    for result in response.results:
        link_op = client.get_type("CampaignAssetOperation")
        link_op.create.campaign = campaign_resource
        link_op.create.asset = result.resource_name
        link_op.create.field_type = client.enums.AssetFieldTypeEnum.SITELINK
        link_ops.append(link_op)

    _mutate(client, campaign_asset_service.mutate_campaign_assets, "MutateCampaignAssetsRequest", customer_id, link_ops)


def add_callouts_to_campaign(
    client: GoogleAdsClient, customer_id: str, campaign_resource: str, callouts: List[Dict[str, str]]
) -> None:
    """Adds callout assets and links them to a campaign."""
    asset_service = client.get_service("AssetService")
    campaign_asset_service = client.get_service("CampaignAssetService")

    callout_ops = []
    for co in callouts:
        asset_op = client.get_type("AssetOperation")
        asset_op.create.callout_asset.callout_text = co["text"][:25]
        callout_ops.append(asset_op)

    response = _mutate(client, asset_service.mutate_assets, "MutateAssetsRequest", customer_id, callout_ops)

    link_ops = []
    for result in response.results:
        link_op = client.get_type("CampaignAssetOperation")
        link_op.create.campaign = campaign_resource
        link_op.create.asset = result.resource_name
        link_op.create.field_type = client.enums.AssetFieldTypeEnum.CALLOUT
        link_ops.append(link_op)

    _mutate(client, campaign_asset_service.mutate_campaign_assets, "MutateCampaignAssetsRequest", customer_id, link_ops)


def add_snippets_to_campaign(
    client: GoogleAdsClient, customer_id: str, campaign_resource: str, snippets: List[Dict[str, Any]]
) -> None:
    """Adds structured snippet assets and links them to a campaign."""
    asset_service = client.get_service("AssetService")
    campaign_asset_service = client.get_service("CampaignAssetService")

    snippet_ops = []
    for sn in snippets:
        asset_op = client.get_type("AssetOperation")
        asset = asset_op.create
        asset.structured_snippet_asset.header = sn["header"]
        for val in sn.get("values", []):
            asset.structured_snippet_asset.values.append(val[:25])
        snippet_ops.append(asset_op)

    response = _mutate(client, asset_service.mutate_assets, "MutateAssetsRequest", customer_id, snippet_ops)

    link_ops = []
    for result in response.results:
        link_op = client.get_type("CampaignAssetOperation")
        link_op.create.campaign = campaign_resource
        link_op.create.asset = result.resource_name
        link_op.create.field_type = client.enums.AssetFieldTypeEnum.STRUCTURED_SNIPPET
        link_ops.append(link_op)

    _mutate(client, campaign_asset_service.mutate_campaign_assets, "MutateCampaignAssetsRequest", customer_id, link_ops)


def add_keywords_to_ad_group(
    client: GoogleAdsClient, customer_id: str, ad_group_resource: str, keywords: List[Dict[str, Any]]
) -> None:
    """Adds keywords to an existing ad group."""
    service = client.get_service("AdGroupCriterionService")

    ops = []
    for kw in keywords:
        op = client.get_type("AdGroupCriterionOperation")
        criterion = op.create
        criterion.ad_group = ad_group_resource
        criterion.status = client.enums.AdGroupCriterionStatusEnum.ENABLED
        criterion.keyword.text = kw["text"]
        criterion.keyword.match_type = getattr(
            client.enums.KeywordMatchTypeEnum, kw["match_type"].upper()
        )
        if kw.get("negative"):
            criterion.negative = True
        ops.append(op)

    _mutate(client, service.mutate_ad_group_criteria, "MutateAdGroupCriteriaRequest", customer_id, ops)


def update_rsa_ad(
    client: GoogleAdsClient, customer_id: str,
    ad_group_id: str, ad_id: str,
    new_headlines: List[str], new_descriptions: List[str]
) -> Dict[str, str]:
    """Replace an RSA ad's headlines/descriptions.

    RSA ad content is immutable in Google Ads — you cannot mutate
    responsive_search_ad.headlines/descriptions on an existing ad. Instead, this
    creates a new RSA with the updated content (preserving final_urls and status)
    and removes the old ad atomically. Returns the new ad_id.
    """
    existing = _get_ad_metadata(client, customer_id, ad_group_id, ad_id)
    if not existing:
        raise ValueError(f"Ad {ad_id} not found in ad group {ad_group_id}")

    ad_group_resource = f"customers/{customer_id}/adGroups/{ad_group_id}"

    create_op = client.get_type("AdGroupAdOperation")
    new_ad_group_ad = create_op.create
    new_ad_group_ad.ad_group = ad_group_resource
    new_ad_group_ad.status = getattr(
        client.enums.AdGroupAdStatusEnum, existing["status_name"]
    )
    for url in existing["final_urls"]:
        new_ad_group_ad.ad.final_urls.append(url)

    rsa = new_ad_group_ad.ad.responsive_search_ad
    for h in new_headlines:
        asset = client.get_type("AdTextAsset")
        asset.text = h[:30]
        rsa.headlines.append(asset)
    for d in new_descriptions:
        asset = client.get_type("AdTextAsset")
        asset.text = d[:90]
        rsa.descriptions.append(asset)

    remove_op = client.get_type("AdGroupAdOperation")
    remove_op.remove = f"customers/{customer_id}/adGroupAds/{ad_group_id}~{ad_id}"

    service = client.get_service("AdGroupAdService")
    response = _mutate(
        client, service.mutate_ad_group_ads, "MutateAdGroupAdsRequest",
        customer_id, [create_op, remove_op],
    )

    new_resource = response.results[0].resource_name
    new_ad_id = new_resource.split("~")[-1]
    return {"new_ad_id": new_ad_id, "removed_ad_id": ad_id}


def _get_ad_metadata(client: GoogleAdsClient, customer_id: str,
                    ad_group_id: str, ad_id: str) -> Dict[str, Any] | None:
    """Fetch final_urls and status of an existing RSA ad."""
    query = f"""
        SELECT ad_group_ad.ad.final_urls, ad_group_ad.status
        FROM ad_group_ad
        WHERE ad_group_ad.ad_group = 'customers/{customer_id}/adGroups/{ad_group_id}'
          AND ad_group_ad.ad.id = {ad_id}
          AND ad_group_ad.status != 'REMOVED'
        LIMIT 1
    """
    service = client.get_service("GoogleAdsService")
    request = client.get_type("SearchGoogleAdsRequest")
    request.customer_id = customer_id
    request.query = query
    results = execute_with_retry(service.search, request=request)
    for row in results:
        return {
            "final_urls": list(row.ad_group_ad.ad.final_urls),
            "status_name": row.ad_group_ad.status.name,
        }
    return None


def add_ad_group_negative_keywords(client: GoogleAdsClient, customer_id: str, ad_group_id: str,
                                   keywords: List[str], match_type: str = "PHRASE") -> None:
    """Applies negative keywords to an existing ad group with retry logic."""
    service = client.get_service("AdGroupCriterionService")
    ad_group_path = client.get_service("AdGroupService").ad_group_path(customer_id, ad_group_id)

    ops = []
    for kw in keywords:
        op = client.get_type("AdGroupCriterionOperation")
        criterion = op.create
        criterion.ad_group = ad_group_path
        criterion.negative = True
        criterion.keyword.text = kw
        criterion.keyword.match_type = getattr(client.enums.KeywordMatchTypeEnum, match_type.upper())
        ops.append(op)

    _mutate(client, service.mutate_ad_group_criteria, "MutateAdGroupCriteriaRequest", customer_id, ops)


def pause_keywords(client: GoogleAdsClient, customer_id: str, operations_list: List[Dict[str, str]]) -> None:
    """Pauses or removes ad group keywords based on the provided operations list."""
    service = client.get_service("AdGroupCriterionService")

    ops = []
    for item in operations_list:
        op = client.get_type("AdGroupCriterionOperation")
        criterion = op.update
        criterion.resource_name = f"customers/{customer_id}/adGroupCriteria/{item['ad_group_id']}~{item['criterion_id']}"
        if item["action"] == "REMOVED":
            criterion.status = client.enums.AdGroupCriterionStatusEnum.REMOVED
        else:
            criterion.status = client.enums.AdGroupCriterionStatusEnum.PAUSED
        op.update_mask.CopyFrom(FieldMask(paths=["status"]))
        ops.append(op)

    _mutate(client, service.mutate_ad_group_criteria, "MutateAdGroupCriteriaRequest", customer_id, ops)


# ─── New consolidated updaters ──────────────────────────────────────────


SMART_BIDDING_DEDICATED_BUDGET = {
    "MAXIMIZE_CONVERSIONS",
    "MAXIMIZE_CONVERSION_VALUE",
    "TARGET_CPA",
    "TARGET_ROAS",
}


def _apply_bidding_strategy_for_update(
    client: GoogleAdsClient, campaign, strategy: str,
    target_cpa: Optional[float], target_roas: Optional[float],
) -> List[str]:
    """Set the right oneof submessage for a bidding-strategy switch and return mask paths.

    Two valid mask shapes:
      - Empty submessage (no concrete subfield set) → mask path is the parent
        ('target_spend'). Tells the API "switch strategy, use defaults". Setting
        zero-valued subfields like cpc_bid_ceiling_micros=0 is rejected as
        "Too low" — Google Ads reads 0 as a literal $0 cap.
      - Submessage with a populated subfield → mask path includes the subfield
        ('target_cpa.target_cpa_micros'). Required for TARGET_CPA/TARGET_ROAS
        which can't run without a concrete target value.
    """
    s = strategy.upper()
    # SetInParent must be called on the raw proto (._pb), not the proto-plus
    # wrapper — the wrapper interprets attribute access as a field lookup and
    # raises "Unknown field for <Strategy>: SetInParent" otherwise.
    if s == "MAXIMIZE_CLICKS":
        campaign.target_spend._pb.SetInParent()
        return ["target_spend"]
    if s == "MAXIMIZE_CONVERSIONS":
        campaign.maximize_conversions._pb.SetInParent()
        return ["maximize_conversions"]
    if s == "MAXIMIZE_CONVERSION_VALUE":
        campaign.maximize_conversion_value._pb.SetInParent()
        return ["maximize_conversion_value"]
    if s == "TARGET_CPA":
        if target_cpa is None:
            raise ValueError("target_cpa is required when bidding_strategy=TARGET_CPA")
        campaign.target_cpa.target_cpa_micros = int(round(target_cpa * 1_000_000))
        return ["target_cpa.target_cpa_micros"]
    if s == "TARGET_ROAS":
        if target_roas is None:
            raise ValueError("target_roas is required when bidding_strategy=TARGET_ROAS")
        campaign.target_roas.target_roas = target_roas
        return ["target_roas.target_roas"]
    if s == "MANUAL_CPC":
        campaign.manual_cpc._pb.SetInParent()
        return ["manual_cpc"]
    raise ValueError(
        f"Unsupported bidding_strategy: '{strategy}'. "
        "Use MANUAL_CPC, MAXIMIZE_CLICKS, MAXIMIZE_CONVERSIONS, "
        "MAXIMIZE_CONVERSION_VALUE, TARGET_CPA, or TARGET_ROAS."
    )


def update_campaign(
    client: GoogleAdsClient, customer_id: str, campaign_id: str,
    name: Optional[str] = None,
    status: Optional[str] = None,
    daily_budget: Optional[float] = None,
    bidding_strategy: Optional[str] = None,
    target_cpa: Optional[float] = None,
    target_roas: Optional[float] = None,
) -> Dict[str, Any]:
    """Update one or more fields on an existing campaign.

    Builds a dynamic field mask from non-None params. If daily_budget is given,
    the linked CampaignBudget is mutated separately. When switching to a Smart
    Bidding strategy that requires a dedicated budget, the budget's
    explicitly_shared flag is also flipped to False (legacy budgets created
    with the API default may be shared). Returns
    {"campaign_id": ..., "updated_fields": [...]} on success, or
    {"campaign_id": ..., "updated_fields": [...], "error": "..."} for the
    sandbox-only TARGET_CPA/TARGET_ROAS conversion-history rejection.
    """
    updated: List[str] = []

    # 1. Budget mutation (combine amount + explicitly_shared flip when needed)
    bidding_strategy_upper = bidding_strategy.upper() if bidding_strategy else None
    needs_budget_flip = bidding_strategy_upper in SMART_BIDDING_DEDICATED_BUDGET
    if daily_budget is not None or needs_budget_flip:
        budget_resource = get_campaign_budget_resource(client, customer_id, campaign_id)
        if not budget_resource:
            raise ValueError(
                f"Could not find campaign_budget for campaign {campaign_id} in account {customer_id}"
            )
        budget_service = client.get_service("CampaignBudgetService")
        budget_op = client.get_type("CampaignBudgetOperation")
        budget = budget_op.update
        budget.resource_name = budget_resource
        budget_mask: List[str] = []
        if daily_budget is not None:
            budget.amount_micros = int(round(daily_budget * 1_000_000))
            budget_mask.append("amount_micros")
            updated.append("daily_budget")
        if needs_budget_flip:
            budget.explicitly_shared = False
            budget_mask.append("explicitly_shared")
        budget_op.update_mask.CopyFrom(FieldMask(paths=budget_mask))
        _mutate(
            client, budget_service.mutate_campaign_budgets,
            "MutateCampaignBudgetsRequest", customer_id, [budget_op],
        )

    # 2. Campaign-level mutation (name/status/bidding_strategy)
    needs_campaign_mutation = any(v is not None for v in (name, status, bidding_strategy))
    if needs_campaign_mutation:
        campaign_service = client.get_service("CampaignService")
        op = client.get_type("CampaignOperation")
        campaign = op.update
        campaign.resource_name = (
            f"customers/{customer_id}/campaigns/{campaign_id}"
        )
        mask_paths: List[str] = []

        if name is not None:
            campaign.name = name
            mask_paths.append("name")
            updated.append("name")
        if status is not None:
            campaign.status = getattr(client.enums.CampaignStatusEnum, status.upper())
            mask_paths.append("status")
            updated.append("status")
        if bidding_strategy is not None:
            mask_paths.extend(
                _apply_bidding_strategy_for_update(
                    client, campaign, bidding_strategy, target_cpa, target_roas
                )
            )
            updated.append("bidding_strategy")

        op.update_mask.CopyFrom(FieldMask(paths=mask_paths))
        try:
            _mutate(
                client, campaign_service.mutate_campaigns,
                "MutateCampaignsRequest", customer_id, [op],
            )
        except GoogleAdsException as e:
            # TARGET_CPA / TARGET_ROAS need real conversion history. The sandbox
            # (and any new account) returns "operation is not allowed for the
            # given context" — a Google Ads constraint, not a fixable bug. Raise
            # a ValueError so the server's error handler returns an error-only
            # response (no contradictory status: success envelope).
            if bidding_strategy_upper in ("TARGET_CPA", "TARGET_ROAS"):
                for err in e.failure.errors:
                    if "operation is not allowed" in err.message.lower():
                        raise ValueError(
                            f"{bidding_strategy_upper} requires conversion history on the account. "
                            "If this is a new account or sandbox with no conversion data, "
                            "Google Ads will reject the strategy switch. Use MAXIMIZE_CONVERSIONS "
                            "or MAXIMIZE_CONVERSION_VALUE to let Smart Bidding gather data first."
                        )
            raise

    return {"campaign_id": campaign_id, "updated_fields": updated}


def update_targeting(
    client: GoogleAdsClient, customer_id: str, campaign_id: str,
    add_geo_target_ids: Optional[List[int]] = None,
    remove_geo_target_ids: Optional[List[int]] = None,
    exclude_geo_target_ids: Optional[List[int]] = None,
    add_language_ids: Optional[List[int]] = None,
    remove_language_ids: Optional[List[int]] = None,
) -> Dict[str, Any]:
    """Add/remove/exclude geo and language targeting criteria on a campaign.

    Returns counts and the IDs actioned. Excludes are added as negative
    LocationInfo CampaignCriteria — equivalent to the UI's "Location → Exclude".
    """
    add_geo_target_ids = add_geo_target_ids or []
    remove_geo_target_ids = remove_geo_target_ids or []
    exclude_geo_target_ids = exclude_geo_target_ids or []
    add_language_ids = add_language_ids or []
    remove_language_ids = remove_language_ids or []

    service = client.get_service("CampaignCriterionService")
    campaign_resource = f"customers/{customer_id}/campaigns/{campaign_id}"

    ops = []
    added_geos: List[int] = []
    added_langs: List[int] = []
    excluded_geos: List[int] = []
    removed_geos: List[int] = []
    removed_langs: List[int] = []

    # Adds — positive geos
    for gid in add_geo_target_ids:
        op = client.get_type("CampaignCriterionOperation")
        c = op.create
        c.campaign = campaign_resource
        c.location.geo_target_constant = f"geoTargetConstants/{gid}"
        ops.append(op)
        added_geos.append(gid)

    # Adds — languages
    for lid in add_language_ids:
        op = client.get_type("CampaignCriterionOperation")
        c = op.create
        c.campaign = campaign_resource
        c.language.language_constant = f"languageConstants/{lid}"
        ops.append(op)
        added_langs.append(lid)

    # Excludes — negative location criteria
    for gid in exclude_geo_target_ids:
        op = client.get_type("CampaignCriterionOperation")
        c = op.create
        c.campaign = campaign_resource
        c.negative = True
        c.location.geo_target_constant = f"geoTargetConstants/{gid}"
        ops.append(op)
        excluded_geos.append(gid)

    # Removes — need resource_names from existing criteria
    if remove_geo_target_ids or remove_language_ids:
        existing = list_campaign_targeting_criteria(client, customer_id, campaign_id)
        # Map (type, id, negative) → resource_name; only remove non-negative geos/langs
        # exclude removes are handled by add-as-negative + future remove call (not in this func)
        for gid in remove_geo_target_ids:
            for crit in existing:
                if (crit["type"] == "LOCATION"
                        and not crit["negative"]
                        and crit.get("geo_target_id") == gid):
                    op = client.get_type("CampaignCriterionOperation")
                    op.remove = crit["resource_name"]
                    ops.append(op)
                    removed_geos.append(gid)
                    break
        for lid in remove_language_ids:
            for crit in existing:
                if (crit["type"] == "LANGUAGE"
                        and not crit["negative"]
                        and crit.get("language_id") == lid):
                    op = client.get_type("CampaignCriterionOperation")
                    op.remove = crit["resource_name"]
                    ops.append(op)
                    removed_langs.append(lid)
                    break

    if not ops:
        raise ValueError("No targeting changes specified — provide at least one ID")

    _mutate(
        client, service.mutate_campaign_criteria,
        "MutateCampaignCriteriaRequest", customer_id, ops,
    )

    return {
        "added": {"geo_target_ids": added_geos, "language_ids": added_langs},
        "removed": {"geo_target_ids": removed_geos, "language_ids": removed_langs},
        "excluded": {"geo_target_ids": excluded_geos},
    }


def add_extensions_to_campaign(
    client: GoogleAdsClient, customer_id: str, campaign_id: str,
    sitelinks: Optional[List[Dict[str, Any]]] = None,
    callouts: Optional[List[Dict[str, str]]] = None,
    structured_snippets: Optional[List[Dict[str, Any]]] = None,
) -> Dict[str, int]:
    """Add any combination of sitelink/callout/structured-snippet assets and link them to a campaign.

    Returns counts per type added.
    """
    sitelinks = sitelinks or []
    callouts = callouts or []
    structured_snippets = structured_snippets or []
    campaign_resource = f"customers/{customer_id}/campaigns/{campaign_id}"

    asset_service = client.get_service("AssetService")
    campaign_asset_service = client.get_service("CampaignAssetService")

    counts = {"sitelinks": 0, "callouts": 0, "structured_snippets": 0}
    asset_ops = []
    # Track ordering so we can map response.results back to extension type
    types_in_order: List[str] = []

    for sl in sitelinks:
        asset_op = client.get_type("AssetOperation")
        a = asset_op.create
        a.sitelink_asset.link_text = sl["link_text"][:25]
        a.sitelink_asset.description1 = sl.get("description1", "")[:35]
        a.sitelink_asset.description2 = sl.get("description2", "")[:35]
        sl_url = sl.get("final_url", "")
        if sl_url and not sl_url.startswith(("http://", "https://")):
            sl_url = "https://" + sl_url
        if sl_url:
            a.final_urls.append(sl_url)
        asset_ops.append(asset_op)
        types_in_order.append("SITELINK")

    for co in callouts:
        asset_op = client.get_type("AssetOperation")
        asset_op.create.callout_asset.callout_text = co["text"][:25]
        asset_ops.append(asset_op)
        types_in_order.append("CALLOUT")

    for sn in structured_snippets:
        asset_op = client.get_type("AssetOperation")
        a = asset_op.create
        a.structured_snippet_asset.header = sn["header"]
        for val in sn.get("values", []):
            a.structured_snippet_asset.values.append(val[:25])
        asset_ops.append(asset_op)
        types_in_order.append("STRUCTURED_SNIPPET")

    if not asset_ops:
        raise ValueError("Provide at least one of sitelinks/callouts/structured_snippets")

    asset_response = _mutate(
        client, asset_service.mutate_assets, "MutateAssetsRequest", customer_id, asset_ops,
    )

    link_ops = []
    for result, kind in zip(asset_response.results, types_in_order):
        link_op = client.get_type("CampaignAssetOperation")
        link_op.create.campaign = campaign_resource
        link_op.create.asset = result.resource_name
        if kind == "SITELINK":
            link_op.create.field_type = client.enums.AssetFieldTypeEnum.SITELINK
            counts["sitelinks"] += 1
        elif kind == "CALLOUT":
            link_op.create.field_type = client.enums.AssetFieldTypeEnum.CALLOUT
            counts["callouts"] += 1
        else:
            link_op.create.field_type = client.enums.AssetFieldTypeEnum.STRUCTURED_SNIPPET
            counts["structured_snippets"] += 1
        link_ops.append(link_op)

    _mutate(
        client, campaign_asset_service.mutate_campaign_assets,
        "MutateCampaignAssetsRequest", customer_id, link_ops,
    )

    return counts


def add_negatives(
    client: GoogleAdsClient, customer_id: str, scope: str, parent_id: str,
    keyword_texts: List[str], match_type: str = "PHRASE",
) -> Dict[str, Any]:
    """Add negative keywords at either CAMPAIGN or AD_GROUP scope.

    Routes to the same underlying CampaignCriterionService /
    AdGroupCriterionService calls used by the prior split tools.
    """
    scope_u = scope.upper()
    if scope_u == "CAMPAIGN":
        add_negative_keywords(client, customer_id, parent_id, keyword_texts, match_type)
    elif scope_u == "AD_GROUP":
        add_ad_group_negative_keywords(client, customer_id, parent_id, keyword_texts, match_type)
    else:
        raise ValueError(f"scope must be 'CAMPAIGN' or 'AD_GROUP', got '{scope}'")
    return {"scope": scope_u, "parent_id": parent_id, "added": len(keyword_texts)}
