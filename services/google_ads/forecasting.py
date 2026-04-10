from typing import List, Dict, Any
from google.ads.googleads.client import GoogleAdsClient
from .client import execute_with_retry

def get_keyword_forecast_metrics(
    client: GoogleAdsClient, customer_id: str, keywords: List[str],
    geo_target_id: int = 2840, language_id: int = 1000
) -> List[Dict[str, Any]]:
    """
    Calls KeywordPlanIdeaService to fetch keyword historical metrics.
    Retrieves avg_monthly_searches, and top of page bid ranges.
    Returns calculated values as a dictionary.
    """
    if not keywords:
        return []

    service = client.get_service("KeywordPlanIdeaService")
    request = client.get_type("GenerateKeywordHistoricalMetricsRequest")
    request.customer_id = customer_id
    request.keywords.extend(keywords)
    request.language = f"languageConstants/{language_id}"
    request.geo_target_constants.append(f"geoTargetConstants/{geo_target_id}")

    response = execute_with_retry(service.generate_keyword_historical_metrics, request=request)

    results = []
    for result in response.results:
        metrics = result.keyword_metrics
        text = result.text

        avg_monthly_searches = getattr(metrics, "avg_monthly_searches", 0) or 0
        low_bid_micros = getattr(metrics, "low_top_of_page_bid_micros", 0) or 0
        high_bid_micros = getattr(metrics, "high_top_of_page_bid_micros", 0) or 0

        results.append({
            "keyword": text,
            "avg_monthly_searches": avg_monthly_searches,
            "low_top_of_page_bid_micros": low_bid_micros,
            "high_top_of_page_bid_micros": high_bid_micros
        })

    return results
