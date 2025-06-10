with unested_genre as(
  SELECT
    LOWER(
      TRIM(
        REGEXP_REPLACE(
          REGEXP_REPLACE(title, r'[^\p{L}\p{N}\s]', ' '),
          r'\s+', ' '
        )
      )
    ) as title,
    id,
    vote_average,
    vote_count,
    COALESCE(lm.name, bm.original_language) AS language_name,
    popularity,
    release_date,
    IF('28' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS action,
    IF('12' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS adventure,
    IF('16' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS animation,
    IF('35' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS comedy,
    IF('80' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS crime,
    IF('99' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS documentary,
    IF('18' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS drama,
    IF('10751' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS family,
    IF('14' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS fantasy,
    IF('36' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS history,
    IF('27' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS horror,
    IF('10402' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS music,
    IF('9648' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS mystery,
    IF('10749' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS romance,
    IF('878' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS science_fiction,
    IF('10770' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS tv_movie,
    IF('53' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS thriller,
    IF('10752' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS war,
    IF('37' IN UNNEST(JSON_EXTRACT_ARRAY(genre_ids)), 1, 0) AS western
  FROM {{ source('raw_bigquery_dataset', 'raw_bfi_movies') }} bm
  LEFT JOIN {{ ref('ref_language_mapping') }} lm
    ON bm.original_language = lm.code
)
select *
from unested_genre