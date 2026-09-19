select count(distinct symbol) symbol_count from {{ ref('mart_stock_monitoring') }}
having symbol_count != 4
