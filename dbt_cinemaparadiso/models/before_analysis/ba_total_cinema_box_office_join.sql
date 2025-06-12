WITH
--1. Normalize enhanced titles
  normalized_enhanced AS (
    SELECT
    *
    , LOWER( ---lower all letters
        REGEXP_REPLACE(
          TRIM(
            TRANSLATE(cleaned_title,
              'áàäâãåéèëêíìïîóòöôõúùüûñçÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÑÇ#&@*!?',
              'aaaaaaeeeeiiiiooooouuuuncAAAAAAEEEEIIIIOOOOOUUUUNC      ' ---remove all the accents from all letters
            )
          ),
          r'[^a-zA-Z0-9\s]', '' ---remove all special characters
        )
      ) AS norm_title
    From {{ ref('cleaned_enhanced_box_office') }}
  ),
-- 2. Normalize cinema titles
  normalized_cinema AS (
    SELECT *,
      LOWER(
        REGEXP_REPLACE(
          TRIM(
            TRANSLATE(cleaned_movie_title,
              'áàäâãåéèëêíìïîóòöôõúùüûñçÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÑÇ#&@*!?',
              'aaaaaaeeeeiiiiooooouuuuncAAAAAAEEEEIIIIOOOOOUUUUNC      '
            )
          ),
          r'[^a-zA-Z0-9\s]', ''
        )
      ) AS norm_title
    From {{ ref('cleaned_cinema_box_office') }}
  ),
  -- 4. Select if there is a match, from which table I want to et the metrics from.
  serialized_data as (
SELECT
  COALESCE(enhanced.norm_title, cinema.norm_title) AS title, -- final normalized title
  CAST(
    COALESCE(
      CAST(enhanced.movie_year AS STRING),
      CAST(cinema.movie_year AS STRING)
    ) AS INT64
  ) AS year,
  COALESCE(
    CAST(enhanced.imdb_rating AS FLOAT64),
    CAST(cinema.imdb_rating AS FLOAT64)
  ) AS rating,
  COALESCE(
    CAST(enhanced.vote_count AS INT64),
    CAST(cinema.imdb_votes AS INT64)
  ) AS votes,
  COALESCE(
    CAST(enhanced.worldwide_gross AS INT64),
    CAST(cinema.box_office_collection AS INT64)
  ) AS worldwide_gross,
    CAST(cinema.time_minute AS INT64) AS duration
    ,COALESCE(enhanced.genres, cinema.genres) AS genres
-- 5. Full outer join
FROM normalized_enhanced AS enhanced
FULL OUTER JOIN normalized_cinema AS cinema
  ON enhanced.norm_title = cinema.norm_title
  AND CAST(enhanced.movie_year AS STRING) = CAST(cinema.movie_year AS STRING))
  select title,year,rating,votes,duration,worldwide_gross,
   CASE WHEN CONTAINS_SUBSTR(genres, 'Action') THEN 1 ELSE 0 END AS genre_action,
        CASE WHEN CONTAINS_SUBSTR(genres, 'Adventure') THEN 1 ELSE 0 END AS genre_adventure,
        CASE WHEN CONTAINS_SUBSTR(genres, 'Animation') THEN 1 ELSE 0 END AS genre_animation,
        CASE WHEN CONTAINS_SUBSTR(genres, 'Comedy') THEN 1 ELSE 0 END AS genre_comedy,
        CASE WHEN CONTAINS_SUBSTR(genres, 'Crime') THEN 1 ELSE 0 END AS genre_crime,
        CASE WHEN CONTAINS_SUBSTR(genres, 'Documentary') THEN 1 ELSE 0 END AS genre_documentary,
        CASE WHEN CONTAINS_SUBSTR(genres, 'Drama') THEN 1 ELSE 0 END AS genre_drama,
        CASE WHEN CONTAINS_SUBSTR(genres, 'Family') THEN 1 ELSE 0 END AS genre_family,
        CASE WHEN CONTAINS_SUBSTR(genres, 'Fantasy') THEN 1 ELSE 0 END AS genre_fantasy,
        CASE WHEN CONTAINS_SUBSTR(genres, 'History') THEN 1 ELSE 0 END AS genre_history,
        CASE WHEN CONTAINS_SUBSTR(genres, 'Horror') THEN 1 ELSE 0 END AS genre_horror,
        CASE WHEN CONTAINS_SUBSTR(genres, 'Music') THEN 1 ELSE 0 END AS genre_music,
        CASE WHEN CONTAINS_SUBSTR(genres, 'Mystery') THEN 1 ELSE 0 END AS genre_mystery,
        CASE WHEN CONTAINS_SUBSTR(genres, 'Romance') THEN 1 ELSE 0 END AS genre_romance,
        -- Combine 'Sci-Fi' and 'Science Fiction' as you suggested
        CASE WHEN CONTAINS_SUBSTR(genres, 'Sci-Fi') OR CONTAINS_SUBSTR(genres, 'Science Fiction') THEN 1 ELSE 0 END AS genre_scifi,
        CASE WHEN CONTAINS_SUBSTR(genres, 'TV Movie') THEN 1 ELSE 0 END AS genre_tv_movie,
        CASE WHEN CONTAINS_SUBSTR(genres, 'Thriller') THEN 1 ELSE 0 END AS genre_thriller,
        CASE WHEN CONTAINS_SUBSTR(genres, 'War') THEN 1 ELSE 0 END AS genre_war,
        CASE WHEN CONTAINS_SUBSTR(genres, 'Western') THEN 1 ELSE 0 END AS genre_western
        from serialized_data