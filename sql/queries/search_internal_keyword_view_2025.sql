CREATE TEMP FUNCTION get_business_channel(account_id STRING, device_type STRING) AS (
  CASE
    WHEN account_id = 'BLIBLI-SEOUL' AND device_type = 'MOBILE'  THEN 'MOBILE-WEB'
    WHEN account_id = 'BLIBLI-SEOUL' AND device_type = 'TABLET'  THEN 'TABLET-WEB'
    WHEN account_id = 'BLIBLI-SEOUL' AND device_type = 'DESKTOP' THEN 'DESKTOP-WEB'
    WHEN account_id = 'BLIBLI-SEOUL' AND device_type = 'N/A'     THEN 'BLIBLI-SEOUL(N/A)'
    ELSE account_id
  END
);

CREATE OR REPLACE TABLE `rome-prod.temp.search_internal_keyword_view_2025` AS

WITH filtered_base AS (
  SELECT
    account_id,
    device_type,
    device,
    session_id,
    search_id,
    catalog_page_type,
    client_member_id,
    actual_search_internal_keyword,
    page_number,
    page_url,
    applied_filter,
    all_category_click,
    sort_by,
    search_internal_keyword,
    browser,
    sequence,
    extension_data,
    product_list,
    search_result_count,
    search_internal_category_id,
    search_internal_category_name,
    DATETIME(datetime, "Asia/Jakarta") AS event_datetime,
    DATE(datetime, "Asia/Jakarta") AS event_date,
    IF(account_id = 'BLIBLI-SEOUL', 'WEB', app_version) AS app_version
  FROM `geneva-prod.bwa.bwa_data_search_view`
  WHERE 1=1
    AND DATE(datetime, "Asia/Jakarta") >= '2025-01-01'
    AND upper(account_id) IN ('BLIBLI-SEOUL', 'BLIBLI-ANDROID', 'BLIBLI-IOS')
    AND catalog_page_type IN ('search', 'searchpage', 'n-1 page')
    AND search_id IS NOT NULL
    AND search_id NOT IN ('', 'undefined')
    AND search_result_count > 0
),

valid_sessions AS (
  SELECT DISTINCT session_id
  FROM `geneva-prod.bwa.bwa_data_search`
),

excluded_sessions AS (
  SELECT DISTINCT session_id
  FROM `rome-prod.datamart.search_members`
  WHERE client_status IN ('INTERNAL', 'GOOGLE_BOT')
),

stopwords AS (
  SELECT DISTINCT LOWER(stopword_keyword) AS keyword
  FROM `geneva-prod.staging.exclude_stopword_keyword`
),

base AS (
  SELECT *
  FROM filtered_base fb
  WHERE
    fb.session_id IN (SELECT session_id FROM valid_sessions)
    AND fb.session_id NOT IN (SELECT session_id FROM excluded_sessions)
    AND LOWER(fb.search_internal_keyword) NOT IN (SELECT keyword FROM stopwords)
),

sku_agg AS (
  SELECT
    search_id,
    session_id,
    search_internal_keyword,
    STRING_AGG(DISTINCT pl.sku, ',') AS skus
  FROM base
  CROSS JOIN UNNEST(product_list) pl
  WHERE pl.sku IS NOT NULL AND pl.sku <> ''
  GROUP BY search_id, session_id,search_internal_keyword
),

extension_kv AS (
  SELECT
    b.search_id,
    b.session_id,
    b.search_internal_keyword,

    MAX(IF(ed.key = 'is_intent_minded', ed.val, NULL)) AS is_intent_minded,
    MAX(IF(ed.key = 'is_seeded_query', ed.val, NULL)) AS is_seeded_query,
    MAX(IF(ed.key = 'is_spellcheck_result', ed.val, NULL)) AS is_spellcheck_result,
    MAX(IF(ed.key = 'is_ner_applied', ed.val, NULL)) AS is_ner_applied,
    MAX(IF(ed.key = 'is_ner_eligible', ed.val, NULL)) AS is_ner_eligible,
    MAX(IF(ed.key = 'all_category_clicked', ed.val, NULL)) AS all_category_clicked,

    MAX(IF(ed.key = 'search_keyword_type', ed.val, NULL)) AS search_keyword_type,
    MAX(IF(ed.key = 'refined_original_search_keyword', ed.val, NULL)) AS refined_original_search_keyword,

    MAX(IF(ed.key = 'appliedQuickFilters', ed.val, NULL)) AS appliedQuickFilters,
    MAX(IF(ed.key = 'quick_filters', ed.val, NULL)) AS quick_filters,
    MAX(IF(ed.key = 'recently_used_filters', ed.val, NULL)) AS recently_used_filters,
    MAX(IF(ed.key = 'applied_displayed_filters', ed.val, NULL)) AS applied_displayed_filters,

    MAX(IF(ed.key = 'algo_id', ed.val, NULL)) AS algo_id,
    MAX(IF(ed.key IN ('algo_name', 'algo'), ed.val, NULL)) AS algo_name,

    MAX(IF(ed.key = 'result_type', ed.val, NULL)) AS result_type,
    MAX(IF(ed.key = 'source', ed.val, NULL)) AS source,
    MAX(IF(ed.key = 'error_status_code', ed.val, NULL)) AS error_status_code,

    MAX(IF(ed.key = 'ds_category_ids', ed.val, NULL)) AS ds_category_ids

  FROM base b
  LEFT JOIN UNNEST(b.extension_data) ed
    ON ed.val IS NOT NULL AND ed.val <> ''
  GROUP BY b.search_id, b.session_id, b.search_internal_keyword
)

SELECT
  get_business_channel(b.account_id, b.device_type) AS business_channel,
  b.event_date AS date,
  b.search_id,
  b.session_id,
  b.catalog_page_type,
  b.client_member_id,
  b.actual_search_internal_keyword,
  b.account_id,
  b.device,
  b.device_type,
  b.page_number,
  b.page_url,
  b.event_datetime AS datetime,
  b.applied_filter,
  b.all_category_click,
  b.sort_by,
  b.search_internal_keyword,
  b.browser,
  b.sequence,
  b.app_version,
  b.search_result_count,
  b.search_internal_category_id,
  b.search_internal_category_name,
  s.skus,
  e.* except(search_id,search_internal_keyword,session_id)
FROM base b
LEFT JOIN sku_agg s
  USING (search_id,search_internal_keyword, session_id)
LEFT JOIN extension_kv e
  USING (search_id,search_internal_keyword, session_id)