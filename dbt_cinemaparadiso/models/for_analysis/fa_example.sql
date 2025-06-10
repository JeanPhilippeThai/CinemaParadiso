select
    *
from
    {{ ref('ref_language_mapping') }}
limit 10