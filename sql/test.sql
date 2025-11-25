
CREATE OR REPLACE TABLE `edp-dev-carema.edp_ent_cma_plss_onboarding_src.src_customer`
(
  customer_id        STRING      NOT NULL,
  sources     STRING,
  first_name         STRING,
  last_name          STRING,
  email              STRING,
  phone_number       STRING,
  country_code       STRING,
  created_at         TIMESTAMP   NOT NULL,
  updated_at         TIMESTAMP,
  is_active          BOOL        DEFAULT TRUE,

  -- optional metadata columns
  batch_id           STRING,
  ingestion_timestamp TIMESTAMP  DEFAULT CURRENT_TIMESTAMP()
)
PARTITION BY DATE(created_at)
CLUSTER BY customer_id;
