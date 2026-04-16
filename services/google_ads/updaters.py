from typing import List, Dict, Any
from google.ads.googleads.client import GoogleAdsClient
from google.protobuf.field_mask_pb2 import FieldMask
from .client import execute_with_retry


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
