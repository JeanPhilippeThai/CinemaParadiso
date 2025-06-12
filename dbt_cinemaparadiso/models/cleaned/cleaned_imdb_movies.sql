--- create the final table
SELECT
    *
FROM  {{ ref('cleaning_raw_imdb_movies') }} 