-- =========================================================
-- NORMALIZATION FUNCTION (USED EVERYWHERE)
-- =========================================================
CREATE TEMP FUNCTION norm_str(s STRING) AS (
  LOWER(
    TRIM(
      REGEXP_REPLACE(
        REGEXP_REPLACE(
          NORMALIZE(LOWER(s), NFKC),
          r'[^a-z0-9 ]',
          ' '
        ),
        r'\s+',
        ' '
      )
    )
  )
);

-- =========================================================
-- FINAL TABLE
-- =========================================================
CREATE OR REPLACE TABLE `rome-prod.temp.search_keyword_intent_exact` AS
WITH
-- =========================================================
-- BASE KEYWORDS
-- =========================================================
base_keywords AS (
  SELECT DISTINCT
    search_internal_keyword,
    norm_str(search_internal_keyword) AS normalized_keyword
  FROM `rome-prod.temp.search_key_words_2025_v2`
),

-- =========================================================
-- NER FLATTENED + NORMALIZED
-- =========================================================
ner_flat AS (
  SELECT
    r.search_internal_keyword,
    attr.attribute_name,
    val AS attribute_value_raw,
    norm_str(val) AS attribute_value_norm
  FROM `rome-prod.temp.ner_results` r
  CROSS JOIN UNNEST(r.attributes) attr
  CROSS JOIN UNNEST(attr.attribute_values) val
  WHERE attr.attribute_name IN ('brand', 'product_type')
),

-- =========================================================
-- NER EXACT MATCH
-- =========================================================
ner_exact AS (
  SELECT
    b.search_internal_keyword,

    LOGICAL_OR(
      attribute_name = 'brand'
      AND b.normalized_keyword = n.attribute_value_norm
    ) AS is_brand_exact_ner,

    LOGICAL_OR(
      attribute_name = 'product_type'
      AND b.normalized_keyword = n.attribute_value_norm
    ) AS is_product_type_exact_ner,

    MAX(
      IF(
        attribute_name = 'brand'
        AND b.normalized_keyword = n.attribute_value_norm,
        attribute_value_raw,
        NULL
      )
    ) AS matched_brand_raw_ner,

    MAX(
      IF(
        attribute_name = 'product_type'
        AND b.normalized_keyword = n.attribute_value_norm,
        attribute_value_raw,
        NULL
      )
    ) AS matched_product_type_raw_ner

  FROM base_keywords b
  LEFT JOIN ner_flat n
    USING (search_internal_keyword)
  GROUP BY b.search_internal_keyword
),

-- =========================================================
-- MASTER BRAND DICTIONARY
-- =========================================================
brand_dict AS (
  SELECT DISTINCT
    brand_name AS brand_raw,
    norm_str(brand_name) AS brand_norm
  FROM `geneva-prod.staging.master_brand`
  WHERE mark_for_delete = FALSE
    AND valid_brand = TRUE
),

brand_exact AS (
  SELECT
    b.search_internal_keyword,
    TRUE AS is_brand_exact_master,
    d.brand_raw AS matched_brand_raw_master
  FROM base_keywords b
  JOIN brand_dict d
    ON b.normalized_keyword = d.brand_norm
),

-- =========================================================
-- PRODUCT TYPE DICTIONARY
-- =========================================================
product_type_dict AS (
  SELECT DISTINCT
    std.attribute_value AS product_type_raw,
    norm_str(std.attribute_value) AS product_type_norm
  FROM `data-science-prod-218306.kg.attex`,
       UNNEST(std_prediction) std
  WHERE item_sku IS NOT NULL
    AND std.attribute_name = 'product_type'
),

product_type_exact AS (
  SELECT
    b.search_internal_keyword,
    TRUE AS is_product_type_exact_dict,
    d.product_type_raw AS matched_product_type_raw_master
  FROM base_keywords b
  JOIN product_type_dict d
    ON b.normalized_keyword = d.product_type_norm
),

-- =========================================================
-- SELLER DICTIONARY
-- =========================================================
seller_dict AS (
  SELECT DISTINCT
    business_partner_name AS seller_raw,
    norm_str(business_partner_name) AS seller_norm
  FROM `rome-prod.datamart.bpt_profile_details`
  WHERE merchant_status = 'ACTIVE'
    AND company_merchant_flag = TRUE
    AND mark_for_delete = FALSE
    AND business_partner_code IS NOT NULL
),

seller_exact AS (
  SELECT
    b.search_internal_keyword,
    TRUE AS is_seller_exact,
    d.seller_raw AS matched_seller_raw
  FROM base_keywords b
  JOIN seller_dict d
    ON b.normalized_keyword = d.seller_norm
)

-- =========================================================
-- FINAL SELECT
-- =========================================================
SELECT
  b.search_internal_keyword,
  b.normalized_keyword,

  ne.is_brand_exact_ner,
  be.is_brand_exact_master,
  COALESCE(be.is_brand_exact_master, FALSE)
    OR COALESCE(ne.is_brand_exact_ner, FALSE) AS is_brand_exact,

  pe.is_product_type_exact_dict,
  ne.is_product_type_exact_ner,
  COALESCE(pe.is_product_type_exact_dict, FALSE)
    OR COALESCE(ne.is_product_type_exact_ner, FALSE) AS is_product_type_exact,

  COALESCE(se.is_seller_exact, FALSE) AS is_seller_exact,

  COALESCE(
    be.matched_brand_raw_master,
    ne.matched_brand_raw_ner
  ) AS matched_brand_raw,

  COALESCE(
    pe.matched_product_type_raw_master,
    ne.matched_product_type_raw_ner
  ) AS matched_product_type_raw,

  se.matched_seller_raw,

  CASE
    WHEN (
      (COALESCE(be.is_brand_exact_master, ne.is_brand_exact_ner, FALSE)
        AND COALESCE(pe.is_product_type_exact_dict, ne.is_product_type_exact_ner, FALSE))
      OR
      (COALESCE(be.is_brand_exact_master, ne.is_brand_exact_ner, FALSE)
        AND COALESCE(se.is_seller_exact, FALSE))
      OR
      (COALESCE(pe.is_product_type_exact_dict, ne.is_product_type_exact_ner, FALSE)
        AND COALESCE(se.is_seller_exact, FALSE))
    ) THEN 'mixed'
    WHEN COALESCE(be.is_brand_exact_master, ne.is_brand_exact_ner, FALSE)
      THEN 'brand'
    WHEN COALESCE(se.is_seller_exact, FALSE)
      THEN 'seller'
    WHEN COALESCE(pe.is_product_type_exact_dict, ne.is_product_type_exact_ner, FALSE)
      THEN 'product_type'
    ELSE 'other'
  END AS intent_type_exact

FROM base_keywords b
LEFT JOIN brand_exact be USING (search_internal_keyword)
LEFT JOIN product_type_exact pe USING (search_internal_keyword)
LEFT JOIN seller_exact se USING (search_internal_keyword)
LEFT JOIN ner_exact ne USING (search_internal_keyword);