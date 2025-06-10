
#craete enhanced_box_office_cleaning_view
CREATE OR REPLACE VIEW cinemaparadiso-462409.cinema_paradiso.cleaned_enhanced_box_office AS
WITH enhanced_rename AS(
  SELECT
    Rank AS movie_rank,
    `Release Group` AS title,
    _Worldwide AS worldwide_gross,
    _Domestic AS domestic_gross,
    `Domestic %` AS domestic_percentage,
    _Foreign AS foreign_gross,
    `Foreign %` AS foreign_percentage,
    Year AS movie_year,
    COALESCE(Genres, 'unknown') AS genres, -- Added COALESCE for genres
    COALESCE(Rating, 'unknown') AS rating,   -- Added COALESCE for rating
    Vote_Count AS vote_count,
    COALESCE(Original_Language, 'unknown') AS original_language, -- Added COALESCE for original_language
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
        CASE WHEN CONTAINS_SUBSTR(genres, 'Western') THEN 1 ELSE 0 END AS genre_western,
    
    COALESCE(Production_Countries, 'unknown') AS production_countries -- Added COALESCE for production_countries
  FROM {{ source('raw_bigquery_dataset', 'raw_enhanced_box_office') }}
),
title_lower_case AS(
  SELECT
    *,
    LOWER(TRIM(title)) AS cleaned_titled,
    -- Handle cases where rating might still be 'unknown' or invalid after COALESCE
    CASE
        WHEN rating = 'unknown' THEN NULL
        ELSE ROUND(CAST(TRIM(SPLIT(rating, '/')[OFFSET(0)]) AS FLOAT64), 2)
    END AS imdb_rating
  FROM enhanced_rename
)
SELECT *
FROM title_lower_case;



