import os
import time
import logging
from typing import Callable, Any
from google.ads.googleads.client import GoogleAdsClient
from google.ads.googleads.errors import GoogleAdsException
from dotenv import load_dotenv

# Load .env from the mcp-prime-ads directory
load_dotenv(os.path.join(os.path.dirname(__file__), "../../.env"))

# Configure logging
logger = logging.getLogger(__name__)
if not logger.handlers:
    # Set up basic configuration if not already set globally
    logging.basicConfig(level=logging.INFO)

def get_google_ads_client() -> GoogleAdsClient:
    """Initializes and returns the Google Ads Client from environment variables."""
    credentials = {
        "developer_token": os.getenv("GOOGLE_ADS_DEVELOPER_TOKEN"),
        "client_id": os.getenv("GOOGLE_ADS_CLIENT_ID"),
        "client_secret": os.getenv("GOOGLE_ADS_CLIENT_SECRET"),
        "refresh_token": os.getenv("GOOGLE_ADS_REFRESH_TOKEN"),
        "login_customer_id": os.getenv("GOOGLE_ADS_LOGIN_CUSTOMER_ID", "").replace("-", ""),
        "use_proto_plus": True
    }
    return GoogleAdsClient.load_from_dict(credentials)

NON_RETRYABLE_KEYWORDS = {"policy", "prohibited", "disapproved", "violation"}

def _is_retryable(ex: GoogleAdsException) -> bool:
    """Returns False for policy violations and other non-transient errors."""
    for error in ex.failure.errors:
        error_code_str = str(error.error_code).lower()
        error_msg = error.message.lower()
        combined = error_code_str + " " + error_msg
        if any(kw in combined for kw in NON_RETRYABLE_KEYWORDS):
            return False
    return True

def _log_policy_details(ex: GoogleAdsException) -> None:
    """Logs detailed policy violation info from a GoogleAdsException."""
    logger.error("Request ID: %s", ex.request_id)
    for error in ex.failure.errors:
        logger.error("  Error Code: %s", error.error_code)
        logger.error("  Error Message: %s", error.message)

        if hasattr(error, 'details') and error.details and hasattr(error.details, 'policy_violation_details') and error.details.policy_violation_details:
            violation = error.details.policy_violation_details
            logger.error("  Policy Name: %s", violation.external_policy_name)
            logger.error("  Policy Description: %s", violation.external_policy_description)
            if hasattr(violation, 'key') and violation.key:
                logger.error("  Policy Topic: %s", violation.key.policy_name)
                logger.error("  Violating Text: %s", violation.key.violating_text)

        if hasattr(error, 'details') and error.details and hasattr(error.details, 'policy_finding_details') and error.details.policy_finding_details:
            finding = error.details.policy_finding_details
            if hasattr(finding, 'policy_topic_entries'):
                for entry in finding.policy_topic_entries:
                    logger.error("  Policy Topic: %s (type: %s)", entry.topic, entry.type_.name)
                    if hasattr(entry, 'evidences'):
                        for ev in entry.evidences:
                            logger.error("    Evidence: %s", ev)

def execute_with_retry(func: Callable[..., Any], *args, max_retries: int = 3, delay: float = 1.0, **kwargs) -> Any:
    """Executes a Google Ads API call with exponential backoff retry logic."""
    for attempt in range(max_retries):
        try:
            return func(*args, **kwargs)
        except GoogleAdsException as ex:
            if attempt == max_retries - 1 or not _is_retryable(ex):
                logger.error("=== Non-retryable or final failure ===")
                _log_policy_details(ex)
                raise

            logger.warning("Attempt %d failed, retrying in %.1fs... (ID: %s)", attempt + 1, delay, ex.request_id)
            time.sleep(delay)
            delay *= 2  # Exponential backoff
