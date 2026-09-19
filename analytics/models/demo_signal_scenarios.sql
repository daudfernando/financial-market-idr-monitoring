-- Explicit synthetic fixtures. Never union this relation into market data.
with scenarios as (
  select 'cross_up' scenario_id, 101.0 short_ma, 100.0 long_ma, 99.0 prev_short, 100.0 prev_long, true ready, 'BUY' expected_signal
  union all select 'cross_down', 99.0, 100.0, 101.0, 100.0, true, 'SELL'
  union all select 'trend_continues', 102.0, 100.0, 101.0, 100.0, true, 'HOLD'
  union all select 'equal_averages', 100.0, 100.0, 100.0, 100.0, true, 'HOLD'
  union all select 'missing_history', 101.0, 100.0, 99.0, 100.0, false, 'NO_SIGNAL'
)
select *, {{ strategy_signal('short_ma', 'long_ma', 'prev_short', 'prev_long', 'ready') }} as strategy_signal,
  'synthetic_formula_fixture' as data_kind, 'sma_3_8_crossover_v1' as signal_rule_version
from scenarios
