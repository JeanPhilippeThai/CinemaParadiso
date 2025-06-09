select
    title as super_title,
    vote_average + 100 as new_vote
from {{ source('raw_bigquery_dataset', 'bfi_movies') }}