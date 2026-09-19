with previous as (
  select *,
    lag(minute_utc) over w as previous_minute,
    lag(price_usd) over w as previous_price,
    lag(usd_idr) over w as previous_fx,
    lag(exposure_idr) over w as previous_exposure
  from {{ ref('fact_market_exposure') }}
  window w as (partition by symbol, ingestion_mode, record_type, session_date order by minute_utc)
), changes as (
  select *, timestamp_diff(minute_utc, previous_minute, second) = 60 as consecutive_minute,
    if(timestamp_diff(minute_utc, previous_minute, second) = 60,
       safe_divide(price_usd, previous_price) - 1, null) as asset_return,
    if(timestamp_diff(minute_utc, previous_minute, second) = 60,
       (price_usd - previous_price) * previous_fx, null) as asset_effect_idr,
    if(timestamp_diff(minute_utc, previous_minute, second) = 60,
       previous_price * (usd_idr - previous_fx), null) as fx_effect_idr,
    if(timestamp_diff(minute_utc, previous_minute, second) = 60,
       (price_usd - previous_price) * (usd_idr - previous_fx), null) as interaction_effect_idr,
    if(timestamp_diff(minute_utc, previous_minute, second) = 60,
       exposure_idr - previous_exposure, null) as total_change_idr
  from previous
), baseline as (
  select *, count(asset_return) over history as baseline_count,
    avg(asset_return) over history as baseline_mean,
    stddev_samp(asset_return) over history as baseline_stddev
  from changes
  window history as (partition by symbol, ingestion_mode, record_type, session_date
    order by unix_seconds(minute_utc) range between 1800 preceding and 60 preceding)
)
select *,
  if(baseline_count >= 20, safe_divide(asset_return - baseline_mean, baseline_stddev), null) as return_zscore,
  case when asset_return is null or baseline_count < 20 or baseline_stddev is null or baseline_stddev = 0
       then 'insufficient_data'
       when abs(safe_divide(asset_return - baseline_mean, baseline_stddev)) >= 3 then 'review_movement'
       else 'within_demo_threshold' end as risk_indicator
from baseline
