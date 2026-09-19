select * from {{ ref('mart_stock_monitoring') }}
where strategy_signal not in ('BUY', 'SELL', 'HOLD', 'NO_SIGNAL')
  or strategy_signal is null
  or (data_status != 'ready' and strategy_signal != 'NO_SIGNAL')
  or (not comparison_complete and (peer_average_return is not null or peer_rank is not null))
  or signal_available_at < event_timestamp
