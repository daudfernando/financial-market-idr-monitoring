select latest.* from {{ ref('mart_stock_latest') }} latest
join (select symbol, ingestion_mode, record_type, max(minute_utc) last_minute
      from {{ ref('mart_stock_monitoring') }} group by 1,2,3) expected
using (symbol, ingestion_mode, record_type)
where latest.minute_utc != expected.last_minute
