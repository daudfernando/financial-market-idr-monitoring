select * from {{ ref('stg_prices') }}
where price_usd <= 0 or is_nan(price_usd) or is_inf(price_usd)
  or currency != 'USD' or symbol not in ('JPM', 'BAC', 'GS', 'MS')
