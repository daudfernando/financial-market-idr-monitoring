-- Empty input must not be reported as a successful analytics demonstration.
select count(*) as row_count from {{ ref('stg_prices') }}
having count(*) = 0
