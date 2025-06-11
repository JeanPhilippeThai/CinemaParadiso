--- 1. Select columns and add standarised names with imdb and mivies_list
WITH movie_statistics_clean1 AS(
SELECT 
    movie_title AS title,
    -- extract the year and cast to date -- 
    EXTRACT(YEAR FROM CAST(production_date AS DATE)) AS year,
    runtime_minutes AS duration,
    genres AS genre,
    director_name AS director,
    movie_averageRating AS rating, 
    movie_numerOfVotes AS votes, 
    approval_Index, 
    `Production budget _` AS production_budget, 
    `Domestic gross _` AS domestic_gross, 
    `Worldwide gross _` AS worldwide_gross
FROM `cinemaparadiso-462409.cinema_paradiso.raw_movie_statistics` 
),


--- 2. Remove the empty values: director, not empty but "-" (326 rows, many are duplicates)
movie_statistics_clean2 AS(
    SELECT 
        *
    FROM movie_statistics_clean1
    WHERE director != "-"
),

--- 3. Look for duplicated values (title, year and director): no duplicates
movie_statistics_clean3 AS(
SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY title, year, director) AS row_num
FROM movie_statistics_clean2
),

--- 4. Add a columns with the genre present/ not present: 
movie_statistics_clean4 AS(  
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
      CASE WHEN CONTAINS_SUBSTR(genre, 'Music') THEN 1 ELSE 0 END AS genre_musical,
      CASE WHEN CONTAINS_SUBSTR(genre, 'Musical') THEN 1 ELSE 0 END AS genre_music,
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
  FROM movie_statistics_clean3
)


--- 5. Clean the title column: Remove leading and trailing whitespace,commas, dashes, apostrophes, parenthesis!
SELECT
    *,
    LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(title), r'[^a-zA-Z0-9 ]', ''), r'\s+',' ')) AS title_title
FROM movie_statistics_clean4
