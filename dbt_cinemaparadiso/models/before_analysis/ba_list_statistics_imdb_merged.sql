-- 1. Normalize statistics titles: 
WITH
  normalized_statistics AS (
    SELECT *,
      LOWER( ---lower all letters
        REGEXP_REPLACE( 
          TRIM(
            TRANSLATE(title,
              'áàäâãåéèëêíìïîóòöôõúùüûñçÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÑÇ#&@*!?',
              'aaaaaaeeeeiiiiooooouuuuncAAAAAAEEEEIIIIOOOOOUUUUNC      ' ---remove all the accents from all letters
            )
          ),
          r'[^a-zA-Z0-9\s]', '' ---remove all special characters
        )
      ) AS norm_title
    FROM {{ ref('cleaned_movie_statistics') }}
  ),
  

-- 2. Normalize list titles
  normalized_list AS (
    SELECT *,
      LOWER(
        REGEXP_REPLACE(
          TRIM(
            TRANSLATE(title,
              'áàäâãåéèëêíìïîóòöôõúùüûñçÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÑÇ#&@*!?',
              'aaaaaaeeeeiiiiooooouuuuncAAAAAAEEEEIIIIOOOOOUUUUNC      '
            )
          ),
          r'[^a-zA-Z0-9\s]', ''
        )
      ) AS norm_title
    FROM {{ ref('cleaned_list_movies') }}
  ),

-- 3. Normalize IMDb titles
  normalized_imdb AS (
    SELECT *,
      LOWER(
        REGEXP_REPLACE(
          TRIM(
            TRANSLATE(title,
              'áàäâãåéèëêíìïîóòöôõúùüûñçÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÑÇ#&@*!?',
              'aaaaaaeeeeiiiiooooouuuuncAAAAAAEEEEIIIIOOOOOUUUUNC      '
            )
          ),
          r'[^a-zA-Z0-9\s]', ''
        )
      ) AS norm_title
    FROM {{ ref('cleaned_imdb_movies') }}
  )

-- 4. Select if there is a match, from which table I want to et the metrics from.
SELECT 
  COALESCE(statistics.norm_title, list.norm_title, imdb.norm_title) AS title, -- final normalized title

  CAST(
    COALESCE(
      CAST(statistics.year AS STRING),
      CAST(list.year AS STRING),
      CAST(imdb.year AS STRING)
    ) AS INT64
  ) AS year, 

  COALESCE(
    CAST(statistics.rating AS FLOAT64), 
    CAST(list.rating AS FLOAT64), 
    CAST(imdb.rating AS FLOAT64)
  ) AS rating,

  COALESCE(
    CAST(statistics.votes AS INT64), 
    CAST(list.votes AS INT64), 
    CAST(imdb.votes AS INT64)
  ) AS votes,

  COALESCE(
    CAST(statistics.duration AS INT64), 
    CAST(list.duration AS INT64), 
    CAST(imdb.duration AS INT64)
  ) AS duration,

  COALESCE(statistics.genre, list.genre, imdb.genre) AS genre

-- 5. Full outer join 
FROM normalized_statistics AS statistics
FULL OUTER JOIN normalized_list AS list
  ON statistics.norm_title = list.norm_title
  AND CAST(statistics.year AS STRING) = CAST(list.year AS STRING)
FULL OUTER JOIN normalized_imdb AS imdb
  ON COALESCE(statistics.norm_title, list.norm_title) = imdb.norm_title
  AND CAST(COALESCE(statistics.year, list.year) AS STRING) = CAST(imdb.year AS STRING);