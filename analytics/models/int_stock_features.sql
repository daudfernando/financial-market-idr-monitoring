with averages as (
  select *,
    avg(price_usd) over short_window as moving_average_short,
    avg(price_usd) over long_window as moving_average_long,
    count(*) over long_window as ma_observation_count,
    first_value(price_usd) over session_window as session_first_price,
    first_value(minute_utc) over session_window as session_first_minute
  from {{ ref('mart_market_risk') }}
  window
    short_window as (partition by symbol, ingestion_mode, record_type, session_date
      order by unix_seconds(minute_utc) range between 120 preceding and current row),
    long_window as (partition by symbol, ingestion_mode, record_type, session_date
      order by unix_seconds(minute_utc) range between 420 preceding and current row),
    session_window as (partition by symbol, ingestion_mode, record_type, session_date
      order by minute_utc rows between unbounded preceding and current row)
)
select *,
  lag(moving_average_short) over w as previous_short_ma,
  lag(moving_average_long) over w as previous_long_ma,
  lag(ma_observation_count) over w as previous_ma_count,
  safe_divide(price_usd, session_first_price) - 1 as session_return
from averages
window w as (partition by symbol, ingestion_mode, record_type, session_date order by minute_utc)
