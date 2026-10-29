with matched as (
  select p.*, f.jisdor_date, f.usd_idr, f.fx_available_at,
    f.source_run_id as fx_source_run_id,
    case when f.jisdor_date is null then 'missing_fx'
         when f.fx_available_at is null then 'prior_day_assumption'
         else 'verified_availability' end as fx_join_policy
  from {{ ref('int_market_minutes') }} p
  left join {{ source('raw', 'jisdor') }} f
    on f.currency = p.currency
    and f.jisdor_date <= p.event_date_wib
    and ((f.fx_available_at is not null and f.fx_available_at <= p.event_timestamp)
      or (f.fx_available_at is null and f.jisdor_date < p.event_date_wib))
  qualify row_number() over (partition by p.event_id order by f.jisdor_date desc) = 1
)
select *, 1 as quantity,
  price_usd * cast(usd_idr as float64) as exposure_idr,
  date_diff(event_date_wib, jisdor_date, day) as fx_age_calendar_days,
  case when usd_idr is null then 'unavailable' else 'indicative_per_share' end as valuation_status
from matched
