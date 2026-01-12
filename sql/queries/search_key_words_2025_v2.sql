-- Query to get the clicks and loads data for the search internal keyword
-- CREATE OR REPLACE TABLE `rome-prod.temp.search_key_words_2025_v2` AS

WITH loads_base AS (
  SELECT
    DATE(datetime) AS date,
    session_id,
    search_id,
    LOWER(search_internal_keyword) AS search_internal_keyword,
    catalog_page_type,
    account_id,
    device_type,
    device,
    app_version,
    search_result_count,
    business_channel,
    result_type,
    is_spellcheck_result,
    search_keyword_type,
    search_internal_category_id,
    ds_category_ids,
    SPLIT(ds_category_ids, '/')[SAFE_OFFSET(0)] AS c1_code,
    SPLIT(ds_category_ids, '/')[SAFE_OFFSET(1)] AS c2_code,
    SPLIT(ds_category_ids, '/')[SAFE_OFFSET(2)] AS c3_code
  FROM `rome-prod.temp.search_internal_keyword_view_2025`
  WHERE DATE(datetime) BETWEEN '2025-01-01' AND '2026-01-05'
),

keyword_category AS (
  SELECT
    LOWER(search_internal_keyword) AS search_internal_keyword,
    ANY_VALUE(c1CategoryCode) AS c1_code,
    ANY_VALUE(c2CategoryCode) AS c2_code,
    ANY_VALUE(c3CategoryCode) AS c3_code,
    ANY_VALUE(c3_category) AS ds_category_ids
  FROM `rome-prod.temp.search_internal_keyword_category_mapping_20260109`
  WHERE c1CategoryCode IS NOT NULL
  GROUP BY 1
),

loads AS (
  SELECT DISTINCT
    l.* EXCEPT (c1_code, c2_code, c3_code,ds_category_ids),
    COALESCE(l.c1_code, k.c1_code) AS c1_code,
    COALESCE(l.c2_code, k.c2_code) AS c2_code,
    COALESCE(l.c3_code, k.c3_code) AS c3_code,
    COALESCE(l.ds_category_ids,k.ds_category_ids) as ds_category_ids
  FROM loads_base l
  LEFT JOIN keyword_category k
    USING (search_internal_keyword)
),

loads_agg AS (
  SELECT
    search_internal_keyword,
    COUNT(DISTINCT session_id) AS search_loads
  FROM loads
  GROUP BY 1
),

clicks_base AS (
  SELECT DISTINCT
    session_id,
    search_id,
    LOWER(search_internal_keyword) AS search_internal_keyword
  FROM `rome-prod.temp.search_internal_keyword_clicks_2025`
  WHERE DATE(event_datetime) BETWEEN '2025-01-01' AND '2026-01-05'
),

clicks_agg AS (
  SELECT
    search_internal_keyword,
    COUNT(DISTINCT session_id) AS search_clicks
  FROM clicks_base
  GROUP BY 1
),

load_totals AS (
  SELECT
    COUNT(DISTINCT CONCAT(session_id, search_internal_keyword)) AS total_loads,
    COUNT(DISTINCT search_internal_keyword) AS total_keywords
  FROM loads
),

click_totals AS (
  SELECT
    COUNT(DISTINCT CONCAT(session_id, search_internal_keyword)) AS total_clicks
  FROM clicks_base
),

sales_category AS (
  SELECT DISTINCT
    c1_code,
    c1_name,
    c1_name_english,
    c2_code,
    c2_name,
    c2_name_english,
    c3_code,
    c3_name,
    c3_name_english
  FROM `rome-prod.temp.vw_catalog_all_sales_category_data`
)

SELECT DISTINCT
  l.*,

  l.session_id AS loads_session_id,
  cb.session_id AS clicks_session_id,

  la.search_loads,
  ca.search_clicks,

  sc.c1_name,
  sc.c2_name,
  sc.c3_name,
  sc.c1_name_english,
  sc.c2_name_english,
  sc.c3_name_english,

  lt.total_loads,
  ct.total_clicks,
  lt.total_keywords

FROM loads l
LEFT JOIN  clicks_base cb
  USING (session_id, search_id, search_internal_keyword)
LEFT JOIN loads_agg la
  USING (search_internal_keyword)
LEFT JOIN clicks_agg ca
  USING (search_internal_keyword)
LEFT JOIN sales_category sc
  USING (c3_code)
CROSS JOIN load_totals lt
CROSS JOIN click_totals ct