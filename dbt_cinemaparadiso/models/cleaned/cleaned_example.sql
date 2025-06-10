select
    *
from
    {{ ref('cleaning_example') }}
limit 10