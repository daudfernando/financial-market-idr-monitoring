select * from {{ ref('fact_market_exposure') }}
where jisdor_date > event_date_wib
   or fx_available_at > event_timestamp
   or (fx_available_at is null and jisdor_date >= event_date_wib)
