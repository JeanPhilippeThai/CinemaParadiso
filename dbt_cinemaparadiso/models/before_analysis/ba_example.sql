select
    *
from
    {{ ref('cleaned_example') }}
limit 10