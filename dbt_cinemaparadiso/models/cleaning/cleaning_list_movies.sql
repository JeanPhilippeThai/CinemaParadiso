--- 1. Selection of useful columns and first cleanings
WITH cleaning_greatest_film1 AS(
  SELECT 
      title,
      -- change type to integer --
      CAST (year AS int64) AS year,
      -- keep numbers only, remove min and change to integer type --
      CAST(RTRIM(runtime, " min") AS INT64) AS duration, 
      genre,
      imdb_rating, 
      imdb_votes,
      director,
      actors,
      awards,
      country,
      language,
      runtime,
      -- if null 0, if true 1. 1 present, 0 absent --
      IF (included_in_guardian_list= true, 1, 0) AS included_in_guardian_list,
      IF (included_in_1001_book = true, 1, 0) AS included_in_1001_book,
      rank_afc_2007, 
      rank_afc_1998, 
      rank_wga_2005, 
      rank_ss_2012
  FROM cinemaparadiso-462409.cinema_paradiso.cleaning_greatest_film
),

--- 2. Look for dupuplicates (row_num> 1)
cleaning_greatest_film2 AS(
  SELECT
    *,
    ROW_NUMBER() OVER(PARTITION BY title, year) AS row_num
  FROM cleaning_greatest_film1
),

---  3. Remove duplicates (row_num> 1)
cleaning_greatest_film3 AS(
  SELECT
    *
  FROM cleaning_greatest_film2
  WHERE row_num = 1
),

--- 4. Clean null values for: 
cleaning_greatest_film4 AS(
  SELECT
    title,
    year,
    duration, 
    genre,
    imdb_rating AS rating, 
    imdb_votes AS votes,
    director,
    actors,
    IF (awards IS NOT NULL, awards, "unknown") AS awards,
    country,
    language,
    included_in_guardian_list,
    included_in_1001_book,
    rank_afc_2007, 
    rank_afc_1998, 
    rank_wga_2005, 
    rank_ss_2012
  FROM cleaning_greatest_film3
  WHERE 
    duration IS NOT NULL 
    AND imdb_votes IS NOT NULL
    AND director IS NOT NULL
    AND actors IS NOT NULL
    AND language IS NOT NULL
),


--- # 5. Add columns with the genre present/ not present: 
cleaning_greatest_film5 AS(  
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
  FROM cleaning_greatest_film4
)

-- rank_Afc_2007: 1105 nulls
-- rank_afc_1998: 1104
-- rank_wga_2005: 1102
-- rank_ss_2012: 1154
-- we still have to clean the awards, but we can do it afterwards. If we don't need the column, there is no need to remove the nulls, if we want to use it, we can either clean it. Same with the ranks. 

--- 5. Clean the title column: Remove leading and trailing whitespace,commas, dashes, apostrophes, parenthesis
SELECT
    *,
    LOWER(REGEXP_REPLACE(REGEXP_REPLACE(TRIM(title), r'[^a-zA-Z0-9 ]', ''), r'\s+',' ')) AS title_title
FROM cleaning_greatest_film5 