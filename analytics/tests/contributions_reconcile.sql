select * from {{ ref('mart_market_risk') }}
where total_change_idr is not null
  and (asset_effect_idr is null or fx_effect_idr is null or interaction_effect_idr is null
    or abs(total_change_idr - asset_effect_idr - fx_effect_idr - interaction_effect_idr) > 0.01)
