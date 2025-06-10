WITH clean_columns_movie_stats AS (
  SELECT 
    movie_title,
    -- Extract year from production_date
    EXTRACT(YEAR FROM CAST(production_date AS DATE)) AS production_year,
    runtime_minutes,
    movie_averageRating,
    movie_numerOfVotes,
    approval_Index,
    -- Clean column names (remove spaces and underscores)
    `Production budget _` AS production_budget,
    `Domestic gross _` AS domestic_gross,
    `Worldwide gross _` AS worldwide_gross,
    
    -- Create binary genre columns
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
    CASE WHEN CONTAINS_SUBSTR(genres, 'Sci-Fi') OR CONTAINS_SUBSTR(genres, 'Science Fiction') THEN 1 ELSE 0 END AS genre_scifi,
    CASE WHEN CONTAINS_SUBSTR(genres, 'TV Movie') THEN 1 ELSE 0 END AS genre_tv_movie,
    CASE WHEN CONTAINS_SUBSTR(genres, 'Thriller') THEN 1 ELSE 0 END AS genre_thriller,
    CASE WHEN CONTAINS_SUBSTR(genres, 'War') THEN 1 ELSE 0 END AS genre_war,
    CASE WHEN CONTAINS_SUBSTR(genres, 'Western') THEN 1 ELSE 0 END AS genre_western
    
  FROM {{ source('raw_bigquery_dataset', 'raw_movie_statistics') }}
)
SELECT * FROM clean_columns_movie_stats;

