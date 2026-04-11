# Google Ads MCP v2 — Full Refactor Specification

## Overview

Refactor the current 21-tool Google Ads MCP into a consolidated 15-tool architecture with a clear permission boundary between read/add tools and destructive tools.

**Current state:** 21 tools, all functional (v1 bugs fixed), no destructive safeguards.

**Target state:** 15 tools (10 core + 5 restricted), validate-then-push pattern on all write operations.

---

## Architecture: Tool Inventory

### Core Tools (always allowed) — 10 tools

These tools can read data and add new entities. They **cannot** delete, remove, pause, or modify existing entities.

| # | Tool | Purpose |
|---|------|---------|
| 1 | `list_accounts` | List all Google Ads accounts under the MCC |
| 2 | `list_campaigns` | List all non-removed Search campaigns for an account |
| 3 | `list_ad_groups` | List all ad groups in a campaign |
| 4 | `load_campaign` | Load all campaign data in one call (replaces 6 tools) |
| 5 | `get_performance` | Get keyword or search term performance (replaces 2 tools) |
| 6 | `create_campaign` | Create a complete paused Search campaign |
| 7 | `manage_keywords` | Add positive or negative keywords at campaign or ad group level (replaces 3 tools) |
| 8 | `manage_extensions` | Add sitelinks, callouts, or structured snippets (replaces 3 tools) |
| 9 | `create_ad` | Create a new RSA ad in an existing ad group (new) |
| 10 | `forecast_budget` | Forecast monthly budget for a keyword set |

### Restricted Tools (require explicit allow) — 5 tools

These tools can modify or delete existing entities. They should be gated behind user confirmation or an allow-list.

| # | Tool | Purpose |
|---|------|---------|
| 11 | `update_ad` | Replace headlines/descriptions on an existing RSA |
| 12 | `remove_keywords` | Pause or remove keywords |
| 13 | `remove_extensions` | Remove sitelinks, callouts, or structured snippets |
| 14 | `remove_campaign` | Remove (delete) a campaign |
| 15 | `remove_ad` | Remove an ad from an ad group |

---

## Tool Specifications

### 1. `list_accounts`

**No changes from v1.**

- **Params:** none
- **Returns:** Array of `{ id, name, resource_name }`
- **Notes:** Call this first to get `customer_id` for all other tools.

---

### 2. `list_campaigns`

**No changes from v1.**

- **Params:**
  - `customer_id` (string, required)
- **Returns:** Array of `{ id, name, status, budget_micros, bidding_strategy }`
- **Notes:** Returns only non-removed SEARCH campaigns.

---

### 3. `list_ad_groups`

**No changes from v1.**

- **Params:**
  - `customer_id` (string, required)
  - `campaign_id` (string, required)
- **Returns:** Array of `{ id, name, status, cpc_bid_micros }`

---

### 4. `load_campaign` ← CONSOLIDATED

**Replaces:** `load_full_campaign`, `get_ads`, `get_keywords`, `get_negative_keywords`, `get_extensions`, `get_targeting`

Returns all data for a campaign in a single call. The `ad_group_id` param is optional — if omitted, only campaign-level data is returned (targeting, extensions, campaign-level negatives). If provided, also includes ads and ad-group-level keywords.

- **Params:**
  - `customer_id` (string, required)
  - `campaign_id` (string, required)
  - `ad_group_id` (string, optional)
- **Returns:**
```json
{
  "targeting": {
    "geo_targets": ["geoTargetConstants/2705"],
    "languages": ["languageConstants/1034"]
  },
  "extensions": {
    "sitelinks": [...],
    "callouts": [...],
    "structured_snippets": [...]
  },
  "negative_keywords": {
    "campaign_level": [...],
    "ad_group_level": [...]
  },
  // Only if ad_group_id is provided:
  "ads": [...],
  "keywords": {
    "positive": [...],
    "negative": [...]
  }
}
```
- **Implementation notes:**
  - When `ad_group_id` is omitted, skip the AdGroupCriterion and AdGroupAd queries.
  - Always return `negative_keywords` split by level (campaign vs ad group).

---

### 5. `get_performance` ← CONSOLIDATED

**Replaces:** `get_keyword_performance`, `get_search_term_performance`

- **Params:**
  - `customer_id` (string, required)
  - `campaign_id` (string, required)
  - `report_type` (string, required) — one of: `"keywords"`, `"search_terms"`, `"campaign"`
  - `days` (integer, optional, default: 30) — lookback period in days
- **Returns:** Array of performance rows with impressions, clicks, CTR, conversions, all_conversions, cost. Fields vary by report_type:
  - `"keywords"` — includes keyword_text, match_type, ad_group_id, ad_group_name, status
  - `"search_terms"` — includes search_term, status, ad_group_id, ad_group_name
  - `"campaign"` — includes campaign-level aggregate metrics (impressions, clicks, cost, conversions, ROAS)
- **Implementation notes:**
  - The `days` param allows custom date ranges instead of hardcoded 30 days.
  - `"campaign"` report type is new — implement with a simple campaign-level GAQL query.
  - All report types return top 50 by cost.

---

### 6. `create_campaign`

**Mostly unchanged from v1.** Add validate-then-push pattern.

- **Params:** Same as v1 (customer_id, campaign_name, daily_budget_usd, url, headlines, descriptions, keywords_json, negative_keywords_json, bidding_strategy, language_id, geo_target_id, sitelinks_json, callouts_json, snippets_json)
- **Behavior:**
  1. Run client-side validation (char limits, required fields).
  2. If client validation passes, send mutate request with `validate_only=True`.
  3. If server validation passes, send mutate request with `validate_only=False`.
  4. Return the created campaign resource name and summary.
- **Always creates in PAUSED status.**

---

### 7. `manage_keywords` ← CONSOLIDATED (add only)

**Replaces:** `add_keywords`, `add_campaign_negatives`, `add_ad_group_negatives`

Single tool for adding positive and negative keywords at both levels.

- **Params:**
  - `customer_id` (string, required)
  - `campaign_id` (string, required)
  - `action` (string, required) — one of: `"add"`, `"add_negative"`
  - `level` (string, required) — one of: `"campaign"`, `"ad_group"`
  - `ad_group_id` (string, required if level is `"ad_group"`)
  - `keywords_json` (string, required) — JSON array of `{ "text": "...", "match_type": "BROAD|PHRASE|EXACT" }`
- **Behavior:**
  - `action: "add"` + `level: "ad_group"` → adds positive keywords to the ad group
  - `action: "add_negative"` + `level: "campaign"` → adds campaign-level negatives
  - `action: "add_negative"` + `level: "ad_group"` → adds ad-group-level negatives
  - `action: "add"` + `level: "campaign"` → error (positive keywords only exist at ad group level)
- **Validate-then-push:** Use `validate_only=True` before committing.

---

### 8. `manage_extensions` ← CONSOLIDATED (add only)

**Replaces:** `add_sitelinks`, `add_callouts`, `add_structured_snippets`

- **Params:**
  - `customer_id` (string, required)
  - `campaign_id` (string, required)
  - `type` (string, required) — one of: `"sitelinks"`, `"callouts"`, `"snippets"`
  - `data_json` (string, required) — JSON payload, format depends on type:
    - `"sitelinks"`: `[{ "link_text": "...", "description1": "...", "description2": "...", "final_url": "..." }]`
    - `"callouts"`: `[{ "text": "..." }]`  (each max 25 chars)
    - `"snippets"`: `[{ "header": "Types|Brands|Services|...", "values": ["a","b","c"] }]`
- **Validate-then-push:** Use `validate_only=True` before committing.

---

### 9. `create_ad` (NEW)

Creates a new RSA ad in an existing ad group.

- **Params:**
  - `customer_id` (string, required)
  - `ad_group_id` (string, required)
  - `headlines` (array of strings, required) — 3-15 headlines, each max 30 chars
  - `descriptions` (array of strings, required) — 2-4 descriptions, each max 90 chars
  - `final_url` (string, required) — landing page URL
- **Behavior:**
  - Creates the ad in PAUSED status.
  - Uses validate-then-push pattern.
- **Returns:** `{ id, status, resource_name }`

---

### 10. `forecast_budget`

**No changes from v1.**

- **Params:**
  - `customer_id` (string, required)
  - `keywords` (array of strings, required)
  - `target_ctr` (number, optional, default: 3.0)
  - `geo_target_id` (integer, optional, default: 2840 = US)
  - `language_id` (integer, optional, default: 1000 = English)
- **Notes:** Requires basic or standard API access (will not work with explorer/test tokens).

---

### 11. `update_ad` ⚠️ RESTRICTED

**No changes from v1.** Moved to restricted tier.

- **Params:**
  - `customer_id` (string, required)
  - `ad_group_id` (string, required)
  - `ad_id` (string, required)
  - `headlines` (array of strings, required) — full replacement set
  - `descriptions` (array of strings, required) — full replacement set
- **IMPORTANT:** This REPLACES all headlines and descriptions. Always fetch the current ad first with `load_campaign`, merge changes, then call this.
- **Validate-then-push:** Use `validate_only=True` before committing.

---

### 12. `remove_keywords` ⚠️ RESTRICTED

**Replaces:** `modify_keyword_status`

- **Params:**
  - `customer_id` (string, required)
  - `actions_json` (string, required) — JSON array of `{ "ad_group_id": "...", "criterion_id": "...", "action": "PAUSED|REMOVED" }`
- **Notes:** Use `load_campaign` first to get criterion IDs. PAUSED keywords can be re-enabled; REMOVED keywords are permanent.

---

### 13. `remove_extensions` ⚠️ RESTRICTED (NEW)

Removes sitelinks, callouts, or structured snippets from a campaign.

- **Params:**
  - `customer_id` (string, required)
  - `campaign_id` (string, required)
  - `type` (string, required) — one of: `"sitelinks"`, `"callouts"`, `"snippets"`
  - `asset_ids` (array of strings, required) — IDs of assets to remove (get these from `load_campaign`)
- **Notes:** Removes the link between the asset and the campaign. Does not delete the asset from the account.

---

### 14. `remove_campaign` ⚠️ RESTRICTED (NEW)

Removes (deletes) a campaign.

- **Params:**
  - `customer_id` (string, required)
  - `campaign_id` (string, required)
- **Behavior:** Sets campaign status to REMOVED. This is irreversible.

---

### 15. `remove_ad` ⚠️ RESTRICTED (NEW)

Removes an ad from an ad group.

- **Params:**
  - `customer_id` (string, required)
  - `ad_group_id` (string, required)
  - `ad_id` (string, required)
- **Behavior:** Sets ad status to REMOVED.

---

## Validate-Then-Push Pattern

All write operations should use this two-step pattern:

```python
def mutate_with_validation(client, service, request_type, mutate_method, customer_id, operations):
    """Generic validate-then-push for any mutate operation."""

    # Step 1: Build request
    request = client.get_type(request_type)
    request.customer_id = customer_id
    request.operations = operations

    # Step 2: Dry run
    request.validate_only = True
    try:
        mutate_method(request=request)
    except GoogleAdsException as ex:
        # Return validation errors
        errors = []
        for error in ex.failure.errors:
            errors.append({
                "field": error.location.field_path_elements[-1].field_name if error.location.field_path_elements else "",
                "message": error.message
            })
        return {"status": "validation_failed", "errors": errors}

    # Step 3: Real push (validation passed)
    request.validate_only = False
    response = mutate_method(request=request)
    return {"status": "success", "resource_name": response.results[0].resource_name}
```

---

## Common Reference Data

### Geo Target IDs

**How to find:** When the user specifies a location (country, region, or city), use web search to look up the Google Ads geo target ID. Search for: `Google Ads geo target ID [location name]` or consult the official CSV: `https://developers.google.com/google-ads/api/data/geotargets`. The CSV contains all locations with their Criteria IDs, canonical names, and types (Country, Region, City, etc.).

**Key behavior:**
- If the user says "target Slovenia" → search for the country-level ID (2705)
- If the user says "target Maribor" → search for the city-level ID (Maribor has its own geo target ID, separate from Slovenia)
- If the user says "target Ljubljana" → search for the city-level ID
- Cities, regions, and countries all have separate IDs. Always match the specificity the user requested.
- When unsure, search the web. Do NOT guess geo target IDs.

**Common references (for quick use):**

| Location | ID | Type |
|----------|-----|------|
| United States | 2840 | Country |
| United Kingdom | 2826 | Country |
| Germany | 2276 | Country |
| Austria | 2040 | Country |
| Slovenia | 2705 | Country |
| Croatia | 2191 | Country |

For cities (e.g. Ljubljana, Maribor, Zagreb, Wien, München) and regions — always look up the ID via web search first.

### Language IDs

**How to find:** Use web search to look up the Google Ads language constant ID. Search for: `Google Ads language ID [language name]` or consult the official reference: `https://developers.google.com/google-ads/api/data/codes-formats#languages`. 

**Key behavior:**
- Match the language to the campaign's target audience, not the country. A country like Switzerland might need German (1014), French (1001), or Italian (1004).
- When creating campaigns in less common languages, always search for the correct ID first.
- Do NOT guess language IDs.

**Common references (for quick use):**

| Language | ID |
|----------|-----|
| English | 1000 |
| German | 1014 |
| Slovenian | 1034 |
| Croatian | 1039 |
| Spanish | 1003 |
| French | 1001 |
| Italian | 1004 |
| Russian | 1031 |

For other languages — always look up the ID via web search first.

### Bidding Strategies

| Strategy | When to use |
|----------|-------------|
| `MANUAL_CPC` | Full control over bids, low budgets |
| `ENHANCED_CPC` | Manual with Google's bid adjustments |
| `MAXIMIZE_CLICKS` | Traffic-focused, new campaigns |
| `MAXIMIZE_CONVERSIONS` | Conversion-focused, needs conversion tracking |

### Structured Snippet Headers

Valid values: `Amenities`, `Brands`, `Courses`, `Degree programs`, `Destinations`, `Featured hotels`, `Insurance coverage`, `Models`, `Neighborhoods`, `Service catalog`, `Shows`, `Styles`, `Types`

### Character Limits

| Element | Max chars |
|---------|-----------|
| Headline | 30 |
| Description | 90 |
| Sitelink link_text | 25 |
| Sitelink description1 | 35 |
| Sitelink description2 | 35 |
| Callout text | 25 |
| Campaign name | 128 |

### RSA Limits

| Element | Min | Max |
|---------|-----|-----|
| Headlines | 3 | 15 |
| Descriptions | 2 | 4 |

---

## V2 Backlog (Future Enhancements)

These features are NOT in scope for the v2 refactor but should be tracked for future versions:

### Campaign Management
- `update_campaign` — change status (pause/enable), budget, bidding strategy, name
- `update_targeting` — add/remove geo targets and languages on existing campaigns
- `update_ad_group` — change status, name, CPC bid

### Reporting
- `get_ad_performance` — ad-level metrics, headline/description combination performance
- `get_change_history` — audit log of recent account changes
- `get_conversion_actions` — list conversion tracking setup

### Advanced
- Cross-campaign search term reports
- Shared negative keyword lists
- Ad scheduling / dayparting
- Device bid adjustments
- PMax / Shopping / Display campaign support

---

## Migration Checklist

- [ ] Implement validate-then-push pattern as shared utility
- [ ] Consolidate `load_full_campaign` + 5 get tools → `load_campaign` (ad_group_id optional)
- [ ] Consolidate `get_keyword_performance` + `get_search_term_performance` → `get_performance` (add `days` param)
- [ ] Consolidate `add_keywords` + `add_campaign_negatives` + `add_ad_group_negatives` → `manage_keywords`
- [ ] Consolidate `add_sitelinks` + `add_callouts` + `add_structured_snippets` → `manage_extensions`
- [ ] Build `create_ad` (new tool)
- [ ] Build `remove_extensions` (new tool)
- [ ] Build `remove_campaign` (new tool)
- [ ] Build `remove_ad` (new tool)
- [ ] Rename `modify_keyword_status` → `remove_keywords`
- [ ] Move `update_ad` to restricted tier
- [ ] Add `report_type: "campaign"` to `get_performance`
- [ ] Add custom `days` param to performance reports
- [ ] Update MCP tool descriptions for all consolidated tools
- [ ] Test all 15 tools against a sandbox account
- [ ] Verify restricted tools are gated correctly