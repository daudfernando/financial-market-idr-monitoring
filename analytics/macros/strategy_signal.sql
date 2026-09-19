{% macro strategy_signal(short_ma, long_ma, previous_short, previous_long, ready) %}
case when not coalesce({{ ready }}, false) then 'NO_SIGNAL'
     when {{ previous_short }} <= {{ previous_long }} and {{ short_ma }} > {{ long_ma }} then 'BUY'
     when {{ previous_short }} >= {{ previous_long }} and {{ short_ma }} < {{ long_ma }} then 'SELL'
     else 'HOLD' end
{% endmacro %}
