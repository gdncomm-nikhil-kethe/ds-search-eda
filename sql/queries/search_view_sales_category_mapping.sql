-- create or replace table `rome-prod.temp.search_view_sales_category_mapping` as 
WITH exploded AS (
  SELECT DISTINCT
    search_internal_category_id,
    code
  FROM `rome-prod.temp.search_internal_keyword_view_2025`,
   UNNEST(SPLIT(search_internal_category_id, '/')) AS code
  WHERE 1=1
    AND DATE(datetime)
        BETWEEN '2025-01-01' AND '2026-01-05'
),

mapped AS (
  SELECT
    e.search_internal_category_id,
    pc.category_code,
    pc.category_level
  FROM exploded e
  LEFT JOIN `geneva-prod.master.pcb_category` pc
    ON e.code = pc.category_code
   AND pc.catalog_code = '12051'
),

ranked AS (
  SELECT
    *,
    ROW_NUMBER() OVER (
      PARTITION BY search_internal_category_id
      ORDER BY category_level DESC
    ) AS rn
  FROM mapped
  WHERE category_level IS NOT NULL
),

chosen_level AS (
  SELECT
    search_internal_category_id,
    category_level,
    category_code
  FROM ranked
  WHERE rn = 1
),

resolved_hierarchy AS (
  SELECT
    c.search_internal_category_id,

    h.c1_name AS c1_name,
    h.c1_name_english,

    CASE
      WHEN c.category_level >= 2 THEN h.c2_name
      ELSE NULL
    END AS c2_name,

     CASE
      WHEN c.category_level >= 2 THEN h.c2_name_english
      ELSE NULL
    END AS c2_name_english,

    CASE
      WHEN c.category_level >= 3 THEN h.c3_name
      ELSE NULL
    END AS c3_name,

        CASE
      WHEN c.category_level >= 3 THEN h.c3_name_english
      ELSE NULL
    END AS c3_name_english,

    c.category_level AS matched_level,
    c.category_code  AS matched_category_code

  FROM chosen_level c
  LEFT JOIN `rome-prod.gds_datasource.vw_catalog_active_sales_category_data` h
    ON (
      (c.category_level = 1 AND h.c1_code = c.category_code) OR
      (c.category_level = 2 AND h.c2_code = c.category_code) OR
      (c.category_level = 3 AND h.c3_code = c.category_code) OR
      (c.category_level = 4 AND h.c4_code = c.category_code) OR
      (c.category_level = 5 AND h.c5_code = c.category_code) OR
      (c.category_level = 6 AND h.c6_code = c.category_code) OR
      (c.category_level = 7 AND h.c7_code = c.category_code) OR
      (c.category_level = 8 AND h.c8_code = c.category_code)
    )
)

SELECT distinct
  search_internal_category_id,
  c1_name,
  c2_name,
  c3_name,
  c1_name_english,
  c2_name_english,
  c3_name_english,
  matched_level,
  matched_category_code
FROM resolved_hierarchy
ORDER BY search_internal_category_id;