--- 1 Subqueries selecting the columns that we want to merge from each table and adding a columns with 1/0 if present of not in the category general audience, cinema, netflix of bfi content. 

WITH
general AS (
  SELECT DISTINCT 
    title
    , LOWER( ---lower all letters
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
    , rating
    , votes
    , year
    , ((rating * votes) + (MIN(votes) OVER () * AVG(rating) OVER ())) 
        / NULLIF((votes + MIN(votes) OVER ()), 0) AS weighted_score --- calculate weighted score (protected)
    , genre_action AS action
    , genre_adventure AS adventure
    , genre_animation AS animation
    , genre_comedy AS comedy
    , genre_crime AS crime
    , genre_documentary AS documentary
    , genre_drama AS drama
    , genre_family AS family
    , genre_fantasy AS fantasy
    , genre_history AS history
    , genre_horror AS horror
    , genre_music AS music
    , genre_mystery AS mystery
    , genre_romance AS romance
    , genre_scifi AS scifi
    , genre_tv_movie tv_movie
    , genre_thriller AS thriller
    , genre_war AS war
    , genre_western AS western
    , genre_biography AS biography
    , genre_film_noir AS film_noir
    , 1 AS in_general --- adds a column with 1 if the movie is present in this table
  FROM {{ ref('ba_list_statistcis_imdb_merged_bygenre') }}
),

cinema AS (
  SELECT DISTINCT 
    title
    , LOWER( ---lower all letters
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
    , rating
    , votes
    , year
    , ((rating * votes) + (MIN(votes) OVER () * AVG(rating) OVER ())) 
        / NULLIF((votes + MIN(votes) OVER ()), 0) AS weighted_score --- calculate weighted score (protected)
    , genre_action AS action
    , genre_adventure AS adventure
    , genre_animation AS animation
    , genre_comedy AS comedy
    , genre_crime AS crime
    , genre_documentary AS documentary
    , genre_drama AS drama
    , genre_family AS family
    , genre_fantasy AS fantasy
    , genre_history AS history
    , genre_horror AS horror
    , genre_music AS music
    , genre_mystery AS mystery
    , genre_romance AS romance
    , genre_scifi AS scifi
    , genre_tv_movie tv_movie
    , genre_thriller AS thriller
    , genre_war AS war 
    , 0 AS biography
    , genre_western AS western 
    , 0 AS film_noir --- we add a 0 because this category was not present
    , 1 AS in_cinema --- adds a column with 1 if the movie is present in this table
  FROM {{ ref('ba_total_cinema_box_office_join') }}
  WHERE rating IS NOT NULL --- there were some null values in the table
    AND votes IS NOT NULL --- there were some null values in the table
),

netflix AS (
  SELECT DISTINCT 
    title
    , LOWER( ---lower all letters
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
    , vote_average AS rating
    , vote_count AS votes
    , ((vote_average * vote_count) + (MIN(vote_count) OVER () * AVG(vote_average) OVER ())) 
        / NULLIF((vote_count + MIN(vote_count) OVER ()), 0) AS weighted_score --- calculate weighted score (protected)
    , EXTRACT (YEAR FROM release_date) AS year
    , action
    , adventure
    , animation
    , comedy
    , crime
    , documentary
    , drama
    , family
    , fantasy
    , history
    , horror
    , music
    , mystery
    , romance
    , science_fiction  AS scifi
    , tv_movie
    , thriller
    , war
    , 0 AS biography
    , western
    , 0 AS film_noir --- we add a 0 because this category was not present
    , 1 AS in_netflix --- adds a column with 1 if the movie is present in this table
  FROM {{ ref('cleaning_netflix_movies') }}
),

bfi AS (
  SELECT DISTINCT 
    title
    , LOWER( ---lower all letters
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
    , vote_average AS rating
    , vote_count AS votes
    , ((vote_average * vote_count) + (MIN(vote_count) OVER () * AVG(vote_average) OVER ())) 
        / NULLIF((vote_count + MIN(vote_count) OVER ()), 0) AS weighted_score --- calculate weighted score (protected)
    , EXTRACT (YEAR FROM release_date) AS year
    , action
    , adventure
    , animation
    , comedy
    , crime
    , documentary
    , drama
    , family
    , fantasy
    , history
    , horror
    , music
    , mystery
    , romance
    , science_fiction AS scifi
    , tv_movie
    , thriller
    , war
    , 0 AS biography
    , western
    , 0 AS film_noir --- we add a 0 because this category was not present
    , 1 AS in_bfi --- adds a column with 1 if the movie is present in this table
  FROM {{ ref('cleaning_bfi_movies') }}
),


--- 2. Select which columns to merge and in case of match, which table should be priorities to fill the title, year, weighted score and genre
merged_1 AS (
  SELECT 
    COALESCE(g.norm_title, c.norm_title, n.norm_title, b.norm_title) AS title
    , COALESCE(g.year, c.year, n.year, b.year) AS year
    , g.weighted_score AS score_general
    , c.weighted_score AS score_cinema
    , n.weighted_score AS score_netflix
    , b.weighted_score AS score_bfi
    , COALESCE(g.action, c.action, n.action, b.action) AS action
    , COALESCE(g.adventure, c.adventure, n.adventure, b.adventure) AS adventure
    , COALESCE(g.animation, c.animation, n.animation, b.animation) AS animation
    , COALESCE(g.comedy, c.comedy, n.comedy, b.comedy) AS comedy
    , COALESCE(g.crime, c.crime, n.crime, b.crime) AS crime
    , COALESCE(g.documentary, c.documentary, n.documentary, b.documentary) AS documentary
    , COALESCE(g.drama, c.drama, n.drama, b.drama) AS drama
    , COALESCE(g.family, c.family, n.family, b.family) AS family
    , COALESCE(g.fantasy, c.fantasy, n.fantasy, b.fantasy) AS fantasy
    , COALESCE(g.history, c.history, n.history, b.history) AS history
    , COALESCE(g.horror, c.horror, n.horror, b.horror) AS horror
    , COALESCE(g.music, c.music, n.music, b.music) AS music
    , COALESCE(g.mystery, c.mystery, n.mystery, b.mystery) AS mystery
    , COALESCE(g.romance, c.romance, n.romance, b.romance) AS romance
    , COALESCE(g.scifi, c.scifi, n.scifi, b.scifi) AS scifi
    , COALESCE(g.tv_movie, c.tv_movie, n.tv_movie, b.tv_movie) AS tv_movie
    , COALESCE(g.thriller, c.thriller, n.thriller, b.thriller) AS thriller
    , COALESCE(g.war, c.war, n.war, b.war) AS war
    , COALESCE(g.western, c.western, n.western, b.western) AS western
    , COALESCE(g.biography, c.biography, n.biography, b.biography) AS biography
    , COALESCE(g.film_noir, c.film_noir, n.film_noir, b.film_noir) AS film_noir
    , IFNULL(g.in_general, 0) AS in_general
    , IFNULL(c.in_cinema, 0) AS in_cinema
    , IFNULL(n.in_netflix, 0) AS in_netflix
    , IFNULL(b.in_bfi, 0) AS in_bfi
  FROM general g
  FULL OUTER JOIN cinema c ON g.title = c.title AND g.year = c.year
  FULL OUTER JOIN netflix n ON COALESCE(g.title, c.title) = n.title AND COALESCE(g.year, c.year) = n.year
  FULL OUTER JOIN bfi b ON COALESCE(g.title, c.title, n.title) = b.title AND COALESCE(g.year, c.year, n.year) = b.year
)

--- 3. Create the table
SELECT *,
       -- Number of sources the movie appears in
       in_general + in_cinema + in_netflix + in_bfi AS source_count
FROM merged_1