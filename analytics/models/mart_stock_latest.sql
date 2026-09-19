select *, max(minute_utc) over (partition by ingestion_mode, record_type) as universe_latest_minute,
  minute_utc = max(minute_utc) over (partition by ingestion_mode, record_type) as aligned_with_latest
from {{ ref('mart_stock_monitoring') }}
qualify row_number() over (
  partition by symbol, ingestion_mode, record_type order by minute_utc desc, ingested_at desc, event_id desc
) = 1
