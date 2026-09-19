-- Historical replay prices are minute-bar closes, timestamped at bar START.
-- Choose last observation per minute and keep modes/types isolated.
select *,
  timestamp_trunc(event_timestamp, minute) as minute_utc,
  date(event_timestamp, 'Asia/Jakarta') as event_date_wib,
  date(event_timestamp, 'America/New_York') as session_date
from {{ ref('stg_prices') }}
qualify row_number() over (
  partition by symbol, ingestion_mode, record_type, timestamp_trunc(event_timestamp, minute)
  order by event_timestamp desc, ingested_at desc, event_id desc
) = 1
