WITH renamed_and_coalesced AS (
  SELECT
    Movie AS title,
    Year AS movie_year,
    Score AS score,
    `Adjusted Score` AS adjusted_score,
    COALESCE(Director, 'unknown') AS director,
    COALESCE(Cast, 'unknown') AS movie_cast, -- Fixed: removed quotes around 'Cast'
    `Box Office Collection` AS box_office_collection,
    time_minute,
    Votes AS imdb_votes
  FROM {{ source('raw_bigquery_dataset', 'raw_cinema_box_office') }}
),

final_cleaning AS (
  SELECT
    *,
    LOWER(
      REGEXP_REPLACE(
        REGEXP_REPLACE(TRIM(title), r'[^a-zA-Z0-9 ]', ''),
        r'\s+', ' '
      )
    ) AS cleaned_movie_title
  FROM renamed_and_coalesced
)

SELECT *
FROM final_cleaning