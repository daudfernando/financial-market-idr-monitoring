with peers as (
  select *,
    count(*) over peer_window as comparison_symbol_count,
    sum(session_return) over peer_window as comparison_return_sum,
    min(session_first_minute) over peer_window as earliest_peer_start,
    max(session_first_minute) over peer_window as latest_peer_start,
    dense_rank() over (partition by minute_utc, ingestion_mode, record_type order by session_return desc) as raw_peer_rank
  from {{ ref('int_stock_features') }}
  window peer_window as (partition by minute_utc, ingestion_mode, record_type)
), classified as (
  select *,
    {{ strategy_signal('moving_average_short', 'moving_average_long', 'previous_short_ma', 'previous_long_ma',
       'ma_observation_count = 8 and previous_ma_count = 8 and consecutive_minute and usd_idr is not null') }} as strategy_signal
  from peers
)
select * except (comparison_return_sum, raw_peer_rank),
  case symbol when 'JPM' then 'JPMorgan Chase' when 'BAC' then 'Bank of America'
              when 'GS' then 'Goldman Sachs' when 'MS' then 'Morgan Stanley' end as institution_name,
  'sma_3_8_crossover_v1' as signal_rule_version,
  'SIMULATION_RULE_NOT_INVESTMENT_ADVICE' as signal_type,
  case strategy_signal
    when 'BUY' then 'SMA3 crosses above SMA8; simulated entry signal'
    when 'SELL' then 'SMA3 crosses below SMA8; simulated exit signal, not a short order'
    when 'HOLD' then 'No new crossover; no new action, not proof of an existing position'
    else 'Insufficient consecutive history or unavailable FX' end as signal_reason,
  if(record_type = 'historical_bar', timestamp_add(minute_utc, interval 1 minute), event_timestamp) as signal_available_at,
  if(usd_idr is null, 'missing_fx', if(ma_observation_count < 8 or previous_ma_count < 8
      or not coalesce(consecutive_minute, false), 'insufficient_history', 'ready')) as data_status,
  comparison_symbol_count = 4 and earliest_peer_start = latest_peer_start as comparison_complete,
  if(comparison_symbol_count = 4 and earliest_peer_start = latest_peer_start,
     (comparison_return_sum - session_return) / 3, null) as peer_average_return,
  if(comparison_symbol_count = 4 and earliest_peer_start = latest_peer_start,
     session_return - (comparison_return_sum - session_return) / 3, null) as excess_return_vs_peers,
  if(comparison_symbol_count = 4 and earliest_peer_start = latest_peer_start, raw_peer_rank, null) as peer_rank,
  if(ingestion_mode = 'replay', 'real_historical_replay', 'live_price_update') as data_kind,
  ingested_at as last_updated_at
from classified
