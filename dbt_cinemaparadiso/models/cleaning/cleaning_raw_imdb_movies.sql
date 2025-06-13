-- 1. Remove description and ceryificate column. Removed min element from duration and cast it to integer.
WITH imdb_movies_cleaned1 AS(
  SELECT
      title, 
      year, 
      CAST(RTRIM(duration, " min") AS INT64) AS duration,
      genre, 
      rating, 
      stars, 
      votes
  FROM {{ source('raw_bigquery_dataset', 'raw_imdb_movies') }} 
  ),


-- 2. Filter TV shows out removing dates that have 2 years
imdb_movies_cleaned2 AS(
  SELECT 
      *
  FROM imdb_movies_cleaned1
  WHERE year NOT LIKE "%–%"
),


-- 3. Clean the year column to remove non numerical values 
imdb_movies_cleaned3 AS(
  SELECT 
      title,
      REGEXP_REPLACE(year, '[^0-9]', '') AS year,
      duration, 
      genre, 
      rating, 
      stars, 
      votes
  FROM imdb_movies_cleaned2
),


-- 4. Look for duplicated values - using a window function create a column with the number of rows per grouped by title and year. 
imdb_movies_cleaned4 AS(
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY title, year) AS row_num
  FROM imdb_movies_cleaned3
),


-- 5. Delete the rows where number_of_rows > 1, I checked them mannualy and they are series
imdb_movies_cleaned5 AS(
  SELECT
    * 
  FROM imdb_movies_cleaned4
  WHERE row_num = 1
),


-- 6. Add a columns with the genre present/ not present: 
imdb_movies_cleaned6 AS(
  SELECT
      *,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Action') THEN 1 ELSE 0 END AS genre_action,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Adventure') THEN 1 ELSE 0 END AS genre_adventure,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Animation') THEN 1 ELSE 0 END AS genre_animation,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Comedy') THEN 1 ELSE 0 END AS genre_comedy,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Crime') THEN 1 ELSE 0 END AS genre_crime,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Documentary') THEN 1 ELSE 0 END AS genre_documentary,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Drama') THEN 1 ELSE 0 END AS genre_drama,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Family') THEN 1 ELSE 0 END AS genre_family,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Fantasy') THEN 1 ELSE 0 END AS genre_fantasy,
      CASE WHEN CONTAINS_SUBSTR(genre, 'History') THEN 1 ELSE 0 END AS genre_history,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Horror') THEN 1 ELSE 0 END AS genre_horror,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Music') THEN 1 ELSE 0 END AS genre_music,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Musical') THEN 1 ELSE 0 END AS genre_musical,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Mystery') THEN 1 ELSE 0 END AS genre_mystery,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Romance') THEN 1 ELSE 0 END AS genre_romance,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Sci-Fi') OR CONTAINS_SUBSTR(genre, 'Science Fiction') THEN 1 ELSE 0 END AS genre_scifi,
      CASE WHEN CONTAINS_SUBSTR(genre, 'TV Movie') THEN 1 ELSE 0 END AS genre_tv_movie,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Thriller') THEN 1 ELSE 0 END AS genre_thriller,
      CASE WHEN CONTAINS_SUBSTR(genre, 'War') THEN 1 ELSE 0 END AS genre_war,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Western') THEN 1 ELSE 0 END AS genre_western,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Biography') THEN 1 ELSE 0 END AS genre_biography,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Short') THEN 1 ELSE 0 END AS genre_short,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Film-Noir') THEN 1 ELSE 0 END AS genre_film_noir,
  FROM imdb_movies_cleaned5
)


--- 7. Look for null values
-- titles: no nulls
-- year: no nulls
-- stars: no nulls
-- genre: 33 nulls
-- duration: 473 nulls
-- rating: 402 nulls (same rows as votes)
-- votes: 402 nulls (same rows as rating)
---- Remove the votes nulls:
SELECT 
    *
FROM imdb_movies_cleaned6
WHERE votes IS NOT NULL AND genre IS NOT NULL AND duration IS NOT NULL

