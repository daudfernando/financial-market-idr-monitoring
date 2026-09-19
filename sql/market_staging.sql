-- Run in BigQuery location asia-southeast2 before Dataflow deployment.
CREATE TABLE IF NOT EXISTS `jcdeah-009.daud_finalproject.stg_market_events` (
  schema_version INT64, event_id STRING, symbol STRING, currency STRING,
  price_usd FLOAT64, event_timestamp TIMESTAMP, event_date DATE,
  ingested_at TIMESTAMP, replayed_at TIMESTAMP, source STRING,
  record_type STRING, source_interval STRING, ingestion_mode STRING,
  raw_payload STRING, kafka_topic STRING, kafka_partition INT64, kafka_offset INT64
)
PARTITION BY event_date
CLUSTER BY symbol, ingestion_mode;

-- Transport/staging can contain duplicate observations; analytical readers use this view.
-- event_id is a price observation identity, not an exchange trade identifier.
CREATE OR REPLACE VIEW `jcdeah-009.daud_finalproject.market_events_deduplicated` AS
SELECT * FROM `jcdeah-009.daud_finalproject.stg_market_events`
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY event_id
  ORDER BY ingested_at DESC, kafka_topic, kafka_partition, kafka_offset DESC
) = 1;
