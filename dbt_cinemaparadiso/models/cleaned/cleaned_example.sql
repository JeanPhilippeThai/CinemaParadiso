select
    *
from
    {{ ref('cleaning_example') }}
limit 10
-- from 
--     {{ source('raw_bigquery_dataset', 'raw_bfi_movies') }}