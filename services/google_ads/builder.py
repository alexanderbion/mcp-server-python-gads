import time
from typing import Optional, List, Dict, Any
from google.ads.googleads.client import GoogleAdsClient
from .client import execute_with_retry

def create_paused_campaign(
    client: GoogleAdsClient, customer_id: str, campaign_name: str,
    budget_micros: int, headlines: List[str], descriptions: List[str],
    keywords: List[Dict[str, str]],
    negative_keywords: List[Dict[str, str]],
    language_id: int = 1000,
    geo_target_id: int = 2840,
    final_url: str = "",
    bidding_strategy: str = "MANUAL_CPC",
    sitelinks: Optional[List[Dict[str, Any]]] = None,
    callouts: Optional[List[Dict[str, str]]] = None,
    structured_snippets: Optional[List[Dict[str, Any]]] = None,
) -> str:
    """Creates a fully formed SEARCH campaign atomically using GoogleAdsService.mutate."""
    sitelinks = sitelinks or []
    callouts = callouts or []
    structured_snippets = structured_snippets or []

    mutate_operations = []
    temp_id_counter = -1

    def next_temp_id() -> str:
        nonlocal temp_id_counter
        val = temp_id_counter
        temp_id_counter -= 1
        return str(val)

    # 1. Budget
    budget_temp_id = next_temp_id()
    budget_resource = f"customers/{customer_id}/campaignBudgets/{budget_temp_id}"

    budget_op = client.get_type("MutateOperation")
    budget = budget_op.campaign_budget_operation.create
    budget.resource_name = budget_resource
    budget.name = f"Budget for {campaign_name} ({int(time.time())})"
    budget.amount_micros = budget_micros
    budget.delivery_method = client.enums.BudgetDeliveryMethodEnum.STANDARD
    mutate_operations.append(budget_op)

    # 2. Campaign
    campaign_temp_id = next_temp_id()
    campaign_resource = f"customers/{customer_id}/campaigns/{campaign_temp_id}"

    campaign_op = client.get_type("MutateOperation")
    campaign = campaign_op.campaign_operation.create
    campaign.resource_name = campaign_resource
    campaign.name = campaign_name
    campaign.advertising_channel_type = client.enums.AdvertisingChannelTypeEnum.SEARCH
    campaign.status = client.enums.CampaignStatusEnum.PAUSED
    campaign.campaign_budget = budget_resource

    if bidding_strategy == "MAXIMIZE_CLICKS":
        client.copy_from(campaign.target_spend, client.get_type("TargetSpend"))
    elif bidding_strategy == "MAXIMIZE_CONVERSIONS":
        client.copy_from(campaign.maximize_conversions, client.get_type("MaximizeConversions"))
    elif bidding_strategy == "ENHANCED_CPC":
        client.copy_from(campaign.manual_cpc, client.get_type("ManualCpc"))
        campaign.manual_cpc.enhanced_cpc_enabled = True
    else:
        client.copy_from(campaign.manual_cpc, client.get_type("ManualCpc"))

    campaign.contains_eu_political_advertising = (
        client.enums.EuPoliticalAdvertisingStatusEnum.DOES_NOT_CONTAIN_EU_POLITICAL_ADVERTISING
    )
    campaign.network_settings.target_google_search = True
    campaign.network_settings.target_search_network = True
    campaign.network_settings.target_content_network = False
    mutate_operations.append(campaign_op)

    # 3. Campaign Criteria (Language, Geo)
    lang_op = client.get_type("MutateOperation")
    lang_op.campaign_criterion_operation.create.campaign = campaign_resource
    lang_op.campaign_criterion_operation.create.language.language_constant = f"languageConstants/{language_id}"
    mutate_operations.append(lang_op)

    geo_op = client.get_type("MutateOperation")
    geo_op.campaign_criterion_operation.create.campaign = campaign_resource
    geo_op.campaign_criterion_operation.create.location.geo_target_constant = f"geoTargetConstants/{geo_target_id}"
    mutate_operations.append(geo_op)

    # 4. Ad Group
    ag_temp_id = next_temp_id()
    ad_group_resource = f"customers/{customer_id}/adGroups/{ag_temp_id}"

    ag_op = client.get_type("MutateOperation")
    ad_group = ag_op.ad_group_operation.create
    ad_group.resource_name = ad_group_resource
    ad_group.name = f"Main Ad Group - {campaign_name}"
    ad_group.campaign = campaign_resource
    ad_group.status = client.enums.AdGroupStatusEnum.PAUSED
    ad_group.type = client.enums.AdGroupTypeEnum.SEARCH_STANDARD
    ad_group.cpc_bid_micros = 1_000_000
    mutate_operations.append(ag_op)

    # 5. AdGroupAd (RSA)
    ad_op = client.get_type("MutateOperation")
    ad_group_ad = ad_op.ad_group_ad_operation.create
    ad_group_ad.ad_group = ad_group_resource
    ad_group_ad.status = client.enums.AdGroupAdStatusEnum.PAUSED

    if final_url and not final_url.startswith(("http://", "https://")):
        final_url = "https://" + final_url

    ad_group_ad.ad.final_urls.append(final_url)
    rsa = ad_group_ad.ad.responsive_search_ad

    for h in headlines[:15]:
        asset = client.get_type("AdTextAsset")
        asset.text = h[:30]
        rsa.headlines.append(asset)

    for d in descriptions[:4]:
        asset = client.get_type("AdTextAsset")
        asset.text = d[:90]
        rsa.descriptions.append(asset)

    mutate_operations.append(ad_op)

    # 6. Keywords
    if keywords:
        for kw in keywords:
            kw_op = client.get_type("MutateOperation")
            criterion = kw_op.ad_group_criterion_operation.create
            criterion.ad_group = ad_group_resource
            criterion.status = client.enums.AdGroupCriterionStatusEnum.PAUSED
            criterion.keyword.text = kw["text"]
            criterion.keyword.match_type = getattr(
                client.enums.KeywordMatchTypeEnum, kw["match_type"].upper()
            )
            mutate_operations.append(kw_op)

    # 7. Negative Keywords
    if negative_keywords:
        for kw in negative_keywords:
            neg_op = client.get_type("MutateOperation")
            criterion = neg_op.ad_group_criterion_operation.create
            criterion.ad_group = ad_group_resource
            criterion.negative = True
            criterion.keyword.text = kw["text"]
            criterion.keyword.match_type = getattr(
                client.enums.KeywordMatchTypeEnum, kw["match_type"].upper()
            )
            mutate_operations.append(neg_op)

    # 8. Sitelinks
    if sitelinks:
        for sl in sitelinks:
            asset_temp_id = next_temp_id()
            asset_resource = f"customers/{customer_id}/assets/{asset_temp_id}"

            asset_op = client.get_type("MutateOperation")
            asset = asset_op.asset_operation.create
            asset.resource_name = asset_resource
            asset.sitelink_asset.link_text = sl["link_text"][:25]
            asset.sitelink_asset.description1 = sl.get("description1", "")[:35]
            asset.sitelink_asset.description2 = sl.get("description2", "")[:35]

            sl_url = sl.get("final_url", final_url)
            if sl_url and not sl_url.startswith(("http://", "https://")):
                sl_url = "https://" + sl_url
            asset.final_urls.append(sl_url)
            mutate_operations.append(asset_op)

            link_op = client.get_type("MutateOperation")
            link_op.campaign_asset_operation.create.campaign = campaign_resource
            link_op.campaign_asset_operation.create.asset = asset_resource
            link_op.campaign_asset_operation.create.field_type = client.enums.AssetFieldTypeEnum.SITELINK
            mutate_operations.append(link_op)

    # 9. Callouts
    if callouts:
        for co in callouts:
            asset_temp_id = next_temp_id()
            asset_resource = f"customers/{customer_id}/assets/{asset_temp_id}"

            asset_op = client.get_type("MutateOperation")
            asset = asset_op.asset_operation.create
            asset.resource_name = asset_resource
            asset.callout_asset.callout_text = co["text"][:25]
            mutate_operations.append(asset_op)

            link_op = client.get_type("MutateOperation")
            link_op.campaign_asset_operation.create.campaign = campaign_resource
            link_op.campaign_asset_operation.create.asset = asset_resource
            link_op.campaign_asset_operation.create.field_type = client.enums.AssetFieldTypeEnum.CALLOUT
            mutate_operations.append(link_op)

    # 10. Structured Snippets
    if structured_snippets:
        for sn in structured_snippets:
            asset_temp_id = next_temp_id()
            asset_resource = f"customers/{customer_id}/assets/{asset_temp_id}"

            asset_op = client.get_type("MutateOperation")
            asset = asset_op.asset_operation.create
            asset.resource_name = asset_resource
            asset.structured_snippet_asset.header = sn["header"]
            for val in sn.get("values", []):
                asset.structured_snippet_asset.values.append(val[:25])
            mutate_operations.append(asset_op)

            link_op = client.get_type("MutateOperation")
            link_op.campaign_asset_operation.create.campaign = campaign_resource
            link_op.campaign_asset_operation.create.asset = asset_resource
            link_op.campaign_asset_operation.create.field_type = client.enums.AssetFieldTypeEnum.STRUCTURED_SNIPPET
            mutate_operations.append(link_op)

    googleads_service = client.get_service("GoogleAdsService")

    # PASS 1: VALIDATE ONLY
    request = client.get_type("MutateGoogleAdsRequest")
    request.customer_id = customer_id
    request.mutate_operations.extend(mutate_operations)
    request.partial_failure = False
    request.validate_only = True

    # execute_with_retry will throw if validation fails, catching bad ads/keywords immediately.
    execute_with_retry(googleads_service.mutate, request=request)

    # PASS 2: EXECUTE
    request.validate_only = False
    response = execute_with_retry(googleads_service.mutate, request=request)

    # Extract the actual campaign_resource from the response
    for resp in response.mutate_operation_responses:
        if resp.campaign_result.resource_name:
            return resp.campaign_result.resource_name

    return "Unknown Campaign Resource"
