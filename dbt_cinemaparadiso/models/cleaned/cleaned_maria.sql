select
    title as maria_super_title,
    vote_average + 100 as maria_new_vote
from {{ source('raw_bigquery_dataset', 'raw_bfi_movies') }}