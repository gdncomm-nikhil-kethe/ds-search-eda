CREATE OR REPLACE TABLE `rome-prod.temp.search_keyword_intent_fuzzy` AS
WITH
base AS (
  SELECT
    search_internal_keyword,
    normalized_keyword,
    is_brand_exact,
    is_product_type_exact,
    is_seller_exact,
    intent_type_exact,
    LENGTH(normalized_keyword) AS keyword_len,
    SUBSTR(normalized_keyword, 1, 1) AS first_char
  FROM `rome-prod.temp.search_keyword_intent_exact`
),

ner_norm AS (
  SELECT
    r.search_internal_keyword,
    attr.attribute_name,
    LOWER(REGEXP_REPLACE(NORMALIZE(val, NFKC), r'[^a-z0-9 ]', ' ')) AS ner_value
  FROM `rome-prod.temp.ner_results` r
  CROSS JOIN UNNEST(r.attributes) attr
  CROSS JOIN UNNEST(attr.attribute_values) val
  WHERE attr.attribute_name IN ('brand', 'product_type')
),

brand_dict AS (
  SELECT
    LOWER(REGEXP_REPLACE(NORMALIZE(brand_name, NFKC), r'[^a-z0-9 ]', ' ')) AS value,
    LENGTH(LOWER(REGEXP_REPLACE(NORMALIZE(brand_name, NFKC), r'[^a-z0-9 ]', ' '))) AS len,
    SUBSTR(LOWER(REGEXP_REPLACE(NORMALIZE(brand_name, NFKC), r'[^a-z0-9 ]', ' ')), 1, 1) AS fc
  FROM `geneva-prod.staging.master_brand`
  WHERE mark_for_delete = FALSE
    AND valid_brand = TRUE
),

product_type_dict AS (
  SELECT DISTINCT
    LOWER(REGEXP_REPLACE(NORMALIZE(std.attribute_value, NFKC), r'[^a-z0-9 ]', ' ')) AS value,
    LENGTH(LOWER(REGEXP_REPLACE(NORMALIZE(std.attribute_value, NFKC), r'[^a-z0-9 ]', ' '))) AS len,
    SUBSTR(LOWER(REGEXP_REPLACE(NORMALIZE(std.attribute_value, NFKC), r'[^a-z0-9 ]', ' ')), 1, 1) AS fc
  FROM `data-science-prod-218306.kg.attex`,
       UNNEST(std_prediction) std
  WHERE item_sku IS NOT NULL
    AND std.attribute_name = 'product_type'
),

seller_dict AS (
  SELECT DISTINCT
    LOWER(REGEXP_REPLACE(NORMALIZE(business_partner_name, NFKC), r'[^a-z0-9 ]', ' ')) AS value,
    LENGTH(LOWER(REGEXP_REPLACE(NORMALIZE(business_partner_name, NFKC), r'[^a-z0-9 ]', ' '))) AS len,
    SUBSTR(LOWER(REGEXP_REPLACE(NORMALIZE(business_partner_name, NFKC), r'[^a-z0-9 ]', ' ')), 1, 1) AS fc
  FROM `rome-prod.datamart.bpt_profile_details`
  WHERE  business_partner_code IS NOT NULL
),

brand_fuzzy_master AS (
  SELECT
    b.search_internal_keyword,
    d.value AS matched_brand_master
  FROM base b
  JOIN brand_dict d
    ON b.first_char = d.fc
   AND ABS(b.keyword_len - d.len) <= 2
  WHERE NOT b.is_brand_exact
    AND (
      (b.keyword_len <= 7 AND EDIT_DISTANCE(b.normalized_keyword, d.value) <= 1)
      OR
      (b.keyword_len > 7 AND EDIT_DISTANCE(b.normalized_keyword, d.value) <= 2)
    )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY b.search_internal_keyword
    ORDER BY EDIT_DISTANCE(b.normalized_keyword, d.value)
  ) = 1
),

brand_fuzzy_ner AS (
  SELECT
    b.search_internal_keyword,
    n.ner_value AS matched_brand_ner
  FROM base b
  JOIN ner_norm n
    ON b.search_internal_keyword = n.search_internal_keyword
   AND n.attribute_name = 'brand'
  WHERE NOT b.is_brand_exact
    AND (
      (b.keyword_len <= 5 AND EDIT_DISTANCE(b.normalized_keyword, n.ner_value) <= 1)
      OR
      (b.keyword_len > 5 AND EDIT_DISTANCE(b.normalized_keyword, n.ner_value) <= 2)
    )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY b.search_internal_keyword
    ORDER BY EDIT_DISTANCE(b.normalized_keyword, n.ner_value)
  ) = 1
),

product_type_fuzzy_master AS (
  SELECT
    b.search_internal_keyword,
    d.value AS matched_product_type_master
  FROM base b
  JOIN product_type_dict d
    ON b.first_char = d.fc
   AND ABS(b.keyword_len - d.len) <= 2
  WHERE NOT b.is_product_type_exact
    AND (
      (b.keyword_len <= 7 AND EDIT_DISTANCE(b.normalized_keyword, d.value) <= 1)
      OR
      (b.keyword_len > 7 AND EDIT_DISTANCE(b.normalized_keyword, d.value) <= 2)
    )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY b.search_internal_keyword
    ORDER BY EDIT_DISTANCE(b.normalized_keyword, d.value)
  ) = 1
),

product_type_fuzzy_ner AS (
  SELECT
    b.search_internal_keyword,
    n.ner_value AS matched_product_type_ner
  FROM base b
  JOIN ner_norm n
    ON b.search_internal_keyword = n.search_internal_keyword
   AND n.attribute_name = 'product_type'
  WHERE NOT b.is_product_type_exact
    AND (
      (b.keyword_len <= 5 AND EDIT_DISTANCE(b.normalized_keyword, n.ner_value) <= 1)
      OR
      (b.keyword_len > 5 AND EDIT_DISTANCE(b.normalized_keyword, n.ner_value) <= 2)
    )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY b.search_internal_keyword
    ORDER BY EDIT_DISTANCE(b.normalized_keyword, n.ner_value)
  ) = 1
),

seller_fuzzy AS (
  SELECT
    b.search_internal_keyword,
    d.value AS matched_seller
  FROM base b
  JOIN seller_dict d
    ON b.first_char = d.fc
   AND ABS(b.keyword_len - d.len) <= 2
  WHERE NOT b.is_seller_exact
    AND (
      (b.keyword_len <= 3 AND b.normalized_keyword = d.value)
      OR
      (b.keyword_len <= 7 AND EDIT_DISTANCE(b.normalized_keyword, d.value) <= 1)
      OR
      (b.keyword_len > 7 AND EDIT_DISTANCE(b.normalized_keyword, d.value) <= 2)
    )
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY b.search_internal_keyword
    ORDER BY EDIT_DISTANCE(b.normalized_keyword, d.value)
  ) = 1
)

SELECT
  b.search_internal_keyword,
  b.normalized_keyword,

  (b.is_brand_exact OR bm.search_internal_keyword IS NOT NULL) AS is_brand_fuzzy_master,
  (b.is_brand_exact OR bn.search_internal_keyword IS NOT NULL) AS is_brand_fuzzy_ner,

  (b.is_product_type_exact OR ptm.search_internal_keyword IS NOT NULL) AS is_product_type_fuzzy_master,
  (b.is_product_type_exact OR ptn.search_internal_keyword IS NOT NULL) AS is_product_type_fuzzy_ner,

  (b.is_seller_exact OR s.search_internal_keyword IS NOT NULL) AS is_seller_fuzzy,

  bm.matched_brand_master,
  bn.matched_brand_ner,
  ptm.matched_product_type_master,
  ptn.matched_product_type_ner,
  s.matched_seller,

  CASE
    WHEN (
      (
        (b.is_brand_exact OR bm.search_internal_keyword IS NOT NULL OR bn.search_internal_keyword IS NOT NULL)
        AND
        (b.is_product_type_exact OR ptm.search_internal_keyword IS NOT NULL OR ptn.search_internal_keyword IS NOT NULL)
      )
      OR
      (
        (b.is_brand_exact OR bm.search_internal_keyword IS NOT NULL OR bn.search_internal_keyword IS NOT NULL)
        AND
        (b.is_seller_exact OR s.search_internal_keyword IS NOT NULL)
      )
      OR
      (
        (b.is_product_type_exact OR ptm.search_internal_keyword IS NOT NULL OR ptn.search_internal_keyword IS NOT NULL)
        AND
        (b.is_seller_exact OR s.search_internal_keyword IS NOT NULL)
      )
    ) THEN 'mixed'
    WHEN (b.is_brand_exact OR bm.search_internal_keyword IS NOT NULL OR bn.search_internal_keyword IS NOT NULL)
      THEN 'brand'
    WHEN (b.is_seller_exact OR s.search_internal_keyword IS NOT NULL)
      THEN 'seller'
    WHEN (b.is_product_type_exact OR ptm.search_internal_keyword IS NOT NULL OR ptn.search_internal_keyword IS NOT NULL)
      THEN 'product_type'
    ELSE 'other'
  END AS intent_type_fuzzy

FROM base b
LEFT JOIN brand_fuzzy_master bm USING (search_internal_keyword)
LEFT JOIN brand_fuzzy_ner bn USING (search_internal_keyword)
LEFT JOIN product_type_fuzzy_master ptm USING (search_internal_keyword)
LEFT JOIN product_type_fuzzy_ner ptn USING (search_internal_keyword)
LEFT JOIN seller_fuzzy s USING (search_internal_keyword);