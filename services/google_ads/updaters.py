from typing import List, Dict, Any
from google.ads.googleads.client import GoogleAdsClient
from .client import execute_with_retry


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

    execute_with_retry(service.mutate_campaign_criteria, customer_id=customer_id, operations=ops, validate_only=True)
    execute_with_retry(service.mutate_campaign_criteria, customer_id=customer_id, operations=ops)


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

    # Validate asset creation first
    execute_with_retry(asset_service.mutate_assets, customer_id=customer_id, operations=sitelink_ops, validate_only=True)
    response = execute_with_retry(asset_service.mutate_assets, customer_id=customer_id, operations=sitelink_ops)

    link_ops = []
    for result in response.results:
        link_op = client.get_type("CampaignAssetOperation")
        link_op.create.campaign = campaign_resource
        link_op.create.asset = result.resource_name
        link_op.create.field_type = client.enums.AssetFieldTypeEnum.SITELINK
        link_ops.append(link_op)

    execute_with_retry(campaign_asset_service.mutate_campaign_assets, customer_id=customer_id, operations=link_ops)


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

    execute_with_retry(asset_service.mutate_assets, customer_id=customer_id, operations=callout_ops, validate_only=True)
    response = execute_with_retry(asset_service.mutate_assets, customer_id=customer_id, operations=callout_ops)

    link_ops = []
    for result in response.results:
        link_op = client.get_type("CampaignAssetOperation")
        link_op.create.campaign = campaign_resource
        link_op.create.asset = result.resource_name
        link_op.create.field_type = client.enums.AssetFieldTypeEnum.CALLOUT
        link_ops.append(link_op)

    execute_with_retry(campaign_asset_service.mutate_campaign_assets, customer_id=customer_id, operations=link_ops)


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

    execute_with_retry(asset_service.mutate_assets, customer_id=customer_id, operations=snippet_ops, validate_only=True)
    response = execute_with_retry(asset_service.mutate_assets, customer_id=customer_id, operations=snippet_ops)

    link_ops = []
    for result in response.results:
        link_op = client.get_type("CampaignAssetOperation")
        link_op.create.campaign = campaign_resource
        link_op.create.asset = result.resource_name
        link_op.create.field_type = client.enums.AssetFieldTypeEnum.STRUCTURED_SNIPPET
        link_ops.append(link_op)

    execute_with_retry(campaign_asset_service.mutate_campaign_assets, customer_id=customer_id, operations=link_ops)


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

    execute_with_retry(service.mutate_ad_group_criteria, customer_id=customer_id, operations=ops, validate_only=True)
    execute_with_retry(service.mutate_ad_group_criteria, customer_id=customer_id, operations=ops)


def update_rsa_ad(
    client: GoogleAdsClient, customer_id: str,
    ad_group_id: str, ad_id: str,
    new_headlines: List[str], new_descriptions: List[str]
) -> None:
    """Updates an RSA ad with new headlines and descriptions (replaces all)."""
    service = client.get_service("AdService")
    ad_resource = f"customers/{customer_id}/ads/{ad_id}"

    op = client.get_type("AdOperation")
    ad = op.update
    ad.resource_name = ad_resource

    rsa = ad.responsive_search_ad
    for h in new_headlines:
        asset = client.get_type("AdTextAsset")
        asset.text = h[:30]
        rsa.headlines.append(asset)
    for d in new_descriptions:
        asset = client.get_type("AdTextAsset")
        asset.text = d[:90]
        rsa.descriptions.append(asset)

    field_mask = client.get_type("FieldMask")
    field_mask.paths.append("responsive_search_ad.headlines")
    field_mask.paths.append("responsive_search_ad.descriptions")
    op.update_mask.CopyFrom(field_mask)

    execute_with_retry(service.mutate_ads, customer_id=customer_id, operations=[op], validate_only=True)
    execute_with_retry(service.mutate_ads, customer_id=customer_id, operations=[op])


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

    execute_with_retry(service.mutate_ad_group_criteria, customer_id=customer_id, operations=ops, validate_only=True)
    execute_with_retry(service.mutate_ad_group_criteria, customer_id=customer_id, operations=ops)


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
        field_mask = client.get_type("FieldMask")
        field_mask.paths.append("status")
        op.update_mask.CopyFrom(field_mask)
        ops.append(op)

    execute_with_retry(service.mutate_ad_group_criteria, customer_id=customer_id, operations=ops, validate_only=True)
    execute_with_retry(service.mutate_ad_group_criteria, customer_id=customer_id, operations=ops)
