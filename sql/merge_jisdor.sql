MERGE `{target}` T
USING `{staging}` S
ON T.jisdor_date = S.jisdor_date AND T.currency = S.currency
WHEN MATCHED AND S.ingested_at > T.ingested_at THEN
  UPDATE SET usd_idr = S.usd_idr, source = S.source,
    ingested_at = S.ingested_at, fx_available_at = S.fx_available_at,
    source_run_id = S.source_run_id
WHEN NOT MATCHED THEN
  INSERT (jisdor_date, currency, usd_idr, source, ingested_at, fx_available_at, source_run_id)
  VALUES (S.jisdor_date, S.currency, S.usd_idr, S.source, S.ingested_at,
          S.fx_available_at, S.source_run_id)
