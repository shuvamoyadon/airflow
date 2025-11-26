-- sql/src_customer.sql

CREATE OR REPLACE TABLE `{{ project_name }}.{{ dataset }}.src_customer`
(
  customer_id         STRING      NOT NULL,
  sources             STRING,
  first_name          STRING,
  last_name           STRING,
  email               STRING,
  phone_number        STRING,
  country_code        STRING,
  created_at          TIMESTAMP   NOT NULL,
  updated_at          TIMESTAMP,
  is_active           BOOL        DEFAULT TRUE,
  batch_id            STRING,
  ingestion_timestamp TIMESTAMP   DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
CLUSTER BY customer_id;
