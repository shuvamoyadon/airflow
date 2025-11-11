-- ============================================================
-- Template: final_merge_template.sql
-- Purpose: SCD Type 1 MERGE into target table
-- ============================================================

MERGE `{{ target_table }}` AS T
USING (
  SELECT
    {{ select_columns }}
  FROM `{{ raw_table }}`
) AS S
ON T.sgk_cfm_customer_id = S.sgk_cfm_customer_id

-- 🔁 Update existing records
WHEN MATCHED THEN
  UPDATE SET
    {% for col in source_columns if col not in ['sgk_cfm_customer_id', 'created_date', 'last_modified_date'] -%}
    T.{{ col }} = S.{{ col }}{% if not loop.last %},{% endif %}
    {% endfor %},
    T.last_modified_date = CURRENT_TIMESTAMP()

-- 🆕 Insert new records
WHEN NOT MATCHED THEN
  INSERT (
    {% for col in source_columns if col not in ['created_date', 'last_modified_date'] -%}
    {{ col }},{% endfor %} created_date, last_modified_date
  )
  VALUES (
    {% for col in source_columns if col not in ['created_date', 'last_modified_date'] -%}
    S.{{ col }},{% endfor %} CURRENT_TIMESTAMP(), CURRENT_TIMESTAMP()
  );
