-- ============================================================
-- Template: final_merge_template.sql
-- Purpose: SCD Type 1 MERGE into target table using timestamps from JSON
-- ============================================================

MERGE `{{ target_table }}` AS T
USING (
  SELECT
    {{ select_columns }}
  FROM `{{ raw_table }}`
) AS S
ON T.Sf_account_id = S.Sf_account_id

-- 🔁 Update existing records only if Sf_account_id exists
WHEN MATCHED AND S.Sf_account_id IS NOT NULL THEN
  UPDATE SET
    {% for col in source_columns if col not in ['Sf_account_id', 'last_process_dts', 'source_last_process_dts'] -%}
    T.{{ col }} = S.{{ col }}{% if not loop.last %},{% endif %}
    {% endfor %},
    T.last_process_dts = S.last_process_dts,
    T.source_last_process_dts = S.source_last_process_dts

-- 🆕 Insert new records when Sf_account_id is not found
WHEN NOT MATCHED THEN
  INSERT (
    {% for col in source_columns if col not in ['last_process_dts', 'source_last_process_dts'] -%}
    {{ col }},{% endfor %} last_process_dts, source_last_process_dts
  )
  VALUES (
    {% for col in source_columns if col not in ['last_process_dts', 'source_last_process_dts'] -%}
    S.{{ col }},{% endfor %} S.last_process_dts, S.source_last_process_dts
  );
