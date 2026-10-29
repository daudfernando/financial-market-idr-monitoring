select * from unnest([
  struct('JPM' as symbol, 'JPMorgan Chase & Co.' as company_name, 'USD' as currency),
  struct('BAC' as symbol, 'Bank of America Corporation' as company_name, 'USD' as currency),
  struct('GS' as symbol, 'The Goldman Sachs Group, Inc.' as company_name, 'USD' as currency),
  struct('MS' as symbol, 'Morgan Stanley' as company_name, 'USD' as currency)
])
