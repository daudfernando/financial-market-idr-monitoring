select * from {{ ref('demo_signal_scenarios') }} where strategy_signal != expected_signal
