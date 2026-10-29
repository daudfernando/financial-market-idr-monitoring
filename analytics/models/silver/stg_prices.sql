select *
from {{ source('raw', 'market') }}
qualify row_number() over (
  partition by event_id order by ingested_at desc, kafka_partition, kafka_offset desc
) = 1
