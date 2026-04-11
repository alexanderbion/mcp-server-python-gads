from typing import List, Dict, Any
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.field_mask_pb2 import FieldMask
from .client import execute_with_retry


def add_negative_keywords(
    client: GoogleAdsClient, customer_id: str, campaign_id: str,
    keywords: List[Dict[str, str]],
) -> None:
    """Applies campaign-level negative keywords.

    Each keyword dict has {"text": str, "match_type": "BROAD|PHRASE|EXACT"}.
    """
    service = client.get_service("CampaignCriterionService")
    campaign_path = client.get_service("CampaignService").campaign_path(customer_id, campaign_id)

    ops = []
    for kw in keywords:
        op = client.get_type("CampaignCriterionOperation")
        criterion = op.create
        criterion.campaign = campaign_path
        criterion.negative = True
        criterion.keyword.text = kw["text"]
        criterion.keyword.match_type = getattr(
            client.enums.KeywordMatchTypeEnum, kw["match_type"].upper()
        )
        ops.append(op)

    request = client.get_type("MutateCampaignCriteriaRequest")
    request.customer_id = customer_id
    request.operations.extend(ops)
    request.validate_only = True
    execute_with_retry(service.mutate_campaign_criteria, request=request)
    request.validate_only = False
    execute_with_retry(service.mutate_campaign_criteria, request=request)


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
    asset_request = client.get_type("MutateAssetsRequest")
    asset_request.customer_id = customer_id
    asset_request.operations.extend(sitelink_ops)
    asset_request.validate_only = True
    execute_with_retry(asset_service.mutate_assets, request=asset_request)
    asset_request.validate_only = False
    response = execute_with_retry(asset_service.mutate_assets, request=asset_request)

    link_ops = []
    for result in response.results:
        link_op = client.get_type("CampaignAssetOperation")
        link_op.create.campaign = campaign_resource
        link_op.create.asset = result.resource_name
        link_op.create.field_type = client.enums.AssetFieldTypeEnum.SITELINK
        link_ops.append(link_op)

    link_request = client.get_type("MutateCampaignAssetsRequest")
    link_request.customer_id = customer_id
    link_request.operations.extend(link_ops)
    link_request.validate_only = True
    execute_with_retry(campaign_asset_service.mutate_campaign_assets, request=link_request)
    link_request.validate_only = False
    execute_with_retry(campaign_asset_service.mutate_campaign_assets, request=link_request)


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

    asset_request = client.get_type("MutateAssetsRequest")
    asset_request.customer_id = customer_id
    asset_request.operations.extend(callout_ops)
    asset_request.validate_only = True
    execute_with_retry(asset_service.mutate_assets, request=asset_request)
    asset_request.validate_only = False
    response = execute_with_retry(asset_service.mutate_assets, request=asset_request)

    link_ops = []
    for result in response.results:
        link_op = client.get_type("CampaignAssetOperation")
        link_op.create.campaign = campaign_resource
        link_op.create.asset = result.resource_name
        link_op.create.field_type = client.enums.AssetFieldTypeEnum.CALLOUT
        link_ops.append(link_op)

    link_request = client.get_type("MutateCampaignAssetsRequest")
    link_request.customer_id = customer_id
    link_request.operations.extend(link_ops)
    link_request.validate_only = True
    execute_with_retry(campaign_asset_service.mutate_campaign_assets, request=link_request)
    link_request.validate_only = False
    execute_with_retry(campaign_asset_service.mutate_campaign_assets, request=link_request)


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

    asset_request = client.get_type("MutateAssetsRequest")
    asset_request.customer_id = customer_id
    asset_request.operations.extend(snippet_ops)
    asset_request.validate_only = True
    execute_with_retry(asset_service.mutate_assets, request=asset_request)
    asset_request.validate_only = False
    response = execute_with_retry(asset_service.mutate_assets, request=asset_request)

    link_ops = []
    for result in response.results:
        link_op = client.get_type("CampaignAssetOperation")
        link_op.create.campaign = campaign_resource
        link_op.create.asset = result.resource_name
        link_op.create.field_type = client.enums.AssetFieldTypeEnum.STRUCTURED_SNIPPET
        link_ops.append(link_op)

    link_request = client.get_type("MutateCampaignAssetsRequest")
    link_request.customer_id = customer_id
    link_request.operations.extend(link_ops)
    link_request.validate_only = True
    execute_with_retry(campaign_asset_service.mutate_campaign_assets, request=link_request)
    link_request.validate_only = False
    execute_with_retry(campaign_asset_service.mutate_campaign_assets, request=link_request)


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

    request = client.get_type("MutateAdGroupCriteriaRequest")
    request.customer_id = customer_id
    request.operations.extend(ops)
    request.validate_only = True
    execute_with_retry(service.mutate_ad_group_criteria, request=request)
    request.validate_only = False
    execute_with_retry(service.mutate_ad_group_criteria, request=request)


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

    field_mask = FieldMask()
    field_mask.paths.append("responsive_search_ad.headlines")
    field_mask.paths.append("responsive_search_ad.descriptions")
    op.update_mask.CopyFrom(field_mask)

    request = client.get_type("MutateAdsRequest")
    request.customer_id = customer_id
    request.operations.append(op)
    request.validate_only = True
    execute_with_retry(service.mutate_ads, request=request)
    request.validate_only = False
    execute_with_retry(service.mutate_ads, request=request)


def create_rsa_ad(
    client: GoogleAdsClient, customer_id: str, ad_group_id: str,
    headlines: List[str], descriptions: List[str], final_url: str,
) -> str:
    """Creates a new paused RSA ad in an existing ad group. Returns the resource name."""
    service = client.get_service("AdGroupAdService")

    op = client.get_type("AdGroupAdOperation")
    ad_group_ad = op.create
    ad_group_ad.ad_group = f"customers/{customer_id}/adGroups/{ad_group_id}"
    ad_group_ad.status = client.enums.AdGroupAdStatusEnum.PAUSED

    url = final_url
    if url and not url.startswith(("http://", "https://")):
        url = "https://" + url
    ad_group_ad.ad.final_urls.append(url)

    rsa = ad_group_ad.ad.responsive_search_ad
    for h in headlines[:15]:
        asset = client.get_type("AdTextAsset")
        asset.text = h[:30]
        rsa.headlines.append(asset)
    for d in descriptions[:4]:
        asset = client.get_type("AdTextAsset")
        asset.text = d[:90]
        rsa.descriptions.append(asset)

    request = client.get_type("MutateAdGroupAdsRequest")
    request.customer_id = customer_id
    request.operations.append(op)
    request.validate_only = True
    execute_with_retry(service.mutate_ad_group_ads, request=request)
    request.validate_only = False
    response = execute_with_retry(service.mutate_ad_group_ads, request=request)

    return response.results[0].resource_name


def remove_campaign_by_id(
    client: GoogleAdsClient, customer_id: str, campaign_id: str,
) -> None:
    """Removes (deletes) a campaign. Irreversible."""
    service = client.get_service("CampaignService")
    op = client.get_type("CampaignOperation")
    op.remove = f"customers/{customer_id}/campaigns/{campaign_id}"

    request = client.get_type("MutateCampaignsRequest")
    request.customer_id = customer_id
    request.operations.append(op)
    request.validate_only = True
    execute_with_retry(service.mutate_campaigns, request=request)
    request.validate_only = False
    execute_with_retry(service.mutate_campaigns, request=request)


def remove_ad_by_id(
    client: GoogleAdsClient, customer_id: str, ad_group_id: str, ad_id: str,
) -> None:
    """Removes an ad from an ad group."""
    service = client.get_service("AdGroupAdService")
    op = client.get_type("AdGroupAdOperation")
    op.remove = f"customers/{customer_id}/adGroupAds/{ad_group_id}~{ad_id}"

    request = client.get_type("MutateAdGroupAdsRequest")
    request.customer_id = customer_id
    request.operations.append(op)
    request.validate_only = True
    execute_with_retry(service.mutate_ad_group_ads, request=request)
    request.validate_only = False
    execute_with_retry(service.mutate_ad_group_ads, request=request)


FIELD_TYPE_BY_EXT: Dict[str, str] = {
    "sitelinks": "SITELINK",
    "callouts": "CALLOUT",
    "snippets": "STRUCTURED_SNIPPET",
}


def remove_campaign_extensions(
    client: GoogleAdsClient, customer_id: str, campaign_id: str,
    ext_type: str, asset_ids: List[str],
) -> int:
    """Remove sitelink / callout / structured-snippet links from a campaign.

    Returns the number of CampaignAsset links actually removed.
    """
    field_type = FIELD_TYPE_BY_EXT.get(ext_type)
    if field_type is None:
        raise ValueError(
            f"ext_type '{ext_type}' invalid — must be one of: {', '.join(FIELD_TYPE_BY_EXT)}"
        )

    asset_resources = [f"customers/{customer_id}/assets/{aid}" for aid in asset_ids]
    asset_in = ", ".join(f"'{r}'" for r in asset_resources)
    query = f"""
        SELECT campaign_asset.resource_name
        FROM campaign_asset
        WHERE campaign_asset.campaign = 'customers/{customer_id}/campaigns/{campaign_id}'
          AND campaign_asset.asset IN ({asset_in})
          AND campaign_asset.field_type = '{field_type}'
          AND campaign_asset.status != 'REMOVED'
    """
    ga_service = client.get_service("GoogleAdsService")
    search_request = client.get_type("SearchGoogleAdsRequest")
    search_request.customer_id = customer_id
    search_request.query = query
    results = execute_with_retry(ga_service.search, request=search_request)

    ca_resources = [row.campaign_asset.resource_name for row in results]
    if not ca_resources:
        return 0

    ca_service = client.get_service("CampaignAssetService")
    ops = []
    for rn in ca_resources:
        op = client.get_type("CampaignAssetOperation")
        op.remove = rn
        ops.append(op)

    request = client.get_type("MutateCampaignAssetsRequest")
    request.customer_id = customer_id
    request.operations.extend(ops)
    request.validate_only = True
    execute_with_retry(ca_service.mutate_campaign_assets, request=request)
    request.validate_only = False
    execute_with_retry(ca_service.mutate_campaign_assets, request=request)

    return len(ca_resources)


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
        field_mask = FieldMask()
        field_mask.paths.append("status")
        op.update_mask.CopyFrom(field_mask)
        ops.append(op)

    request = client.get_type("MutateAdGroupCriteriaRequest")
    request.customer_id = customer_id
    request.operations.extend(ops)
    request.validate_only = True
    execute_with_retry(service.mutate_ad_group_criteria, request=request)
    request.validate_only = False
    execute_with_retry(service.mutate_ad_group_criteria, request=request)
