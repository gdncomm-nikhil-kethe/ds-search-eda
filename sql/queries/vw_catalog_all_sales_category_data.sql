SELECT DISTINCT
  c1.id as c1_id,
  c1.category_code AS c1_code,
  c1.name AS c1_name,
  c1.name_english AS c1_name_english,

  c2.id as c2_id,
  c2.category_code AS c2_code,
  c2.name AS c2_name,
  c2.name_english AS c2_name_english,

  c3.id as c3_id,
  c3.category_code AS c3_code,
  c3.name AS c3_name,
  c3.name_english AS c3_name_english,

  c4.id as c4_id,
  c4.category_code AS c4_code,
  c4.name AS c4_name,
  c4.name_english AS c4_name_english,

  c5.id as c5_id,
  c5.category_code AS c5_code,
  c5.name AS c5_name,
  c5.name_english AS c5_name_english,

  c6.id as c6_id,
  c6.category_code AS c6_code,
  c6.name AS c6_name,
  c6.name_english AS c6_name_english,

  c7.id as c7_id,
  c7.category_code AS c7_code,
  c7.name AS c7_name,
  c7.name_english AS c7_name_english,

  c8.id as c8_id,
  c8.category_code AS c8_code,
  c8.name AS c8_name,
  c8.name_english AS c8_name_english,

  case when c8.id is not null then c8.id
  when c7.id is not null then c7.id
  when c6.id is not null then c6.id
  when c5.id is not null then c5.id
  when c4.id is not null then c4.id
  when c3.id is not null then c3.id
  when c2.id is not null then c2.id
  else "404 error" end cn_id,

  case when c8.category_code is not null then c8.category_code
  when c7.category_code is not null then c7.category_code
  when c6.category_code is not null then c6.category_code
  when c5.category_code is not null then c5.category_code
  when c4.category_code is not null then c4.category_code
  when c3.category_code is not null then c3.category_code
  when c2.category_code is not null then c2.category_code
  else "404 error" end cn_code,

  case when c8.name is not null then c8.name
  when c7.name is not null then c7.name
  when c6.name is not null then c6.name
  when c5.name is not null then c5.name
  when c4.name is not null then c4.name
  when c3.name is not null then c3.name
  when c2.name is not null then c2.name
  else "404 error" end cn_name,

  case when c8.name_english is not null then c8.name_english
  when c7.name_english is not null then c7.name_english
  when c6.name_english is not null then c6.name_english
  when c5.name_english is not null then c5.name_english
  when c4.name_english is not null then c4.name_english
  when c3.name_english is not null then c3.name_english
  when c2.name_english is not null then c2.name_english
  else "404 error" end cn_name_english

FROM `geneva-prod.master.pcb_category` c1
JOIN `geneva-prod.master.pcb_category` c2
  ON c2.parent_category_id = c1.id AND c2.category_level = 2
LEFT JOIN `geneva-prod.master.pcb_category` c3
  ON c3.parent_category_id = c2.id AND c3.category_level = 3 
LEFT JOIN `geneva-prod.master.pcb_category` c4
  ON c4.parent_category_id = c3.id AND c4.category_level = 4 
LEFT JOIN `geneva-prod.master.pcb_category` c5
  ON c5.parent_category_id = c4.id AND c5.category_level = 5
LEFT JOIN `geneva-prod.master.pcb_category` c6
  ON c6.parent_category_id = c5.id AND c6.category_level = 6
LEFT JOIN `geneva-prod.master.pcb_category` c7
  ON c7.parent_category_id = c6.id AND c7.category_level = 7 
LEFT JOIN `geneva-prod.master.pcb_category` c8
  ON c8.parent_category_id = c7.id AND c8.category_level = 8 
WHERE
  c1.catalog_code = '12051'
  AND c1.category_level = 1