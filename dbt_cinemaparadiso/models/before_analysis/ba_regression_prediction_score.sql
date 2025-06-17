--- 1 Subqueries selecting the columns that we want to merge from each table and adding a columns with 1/0 if present of not in the category cinema, netflix of bfi. 

WITH list AS(
SELECT 
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
    , CAST(year AS INT64) AS year
    , language
    , duration
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
    , genre_biography AS biography
    , genre_western AS western 
    , genre_film_noir AS film_noir
    , null AS worldwide_gross
    , ((rating * votes) + (MIN(votes) OVER () * AVG(rating) OVER ())) 
        / NULLIF((votes + MIN(votes) OVER ()), 0) AS weighted_score --- calculate 
    , 1 AS in_list 
  FROM {{ ref('cleaned_list_movies') }}
), 

stats AS (
  SELECT 
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
    , CAST(year AS INT64) AS year
    , null AS language
    , duration
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
    , genre_biography AS biography
    , genre_western AS western 
    , genre_film_noir AS film_noir
    , worldwide_gross
    , ((rating * votes) + (MIN(votes) OVER () * AVG(rating) OVER ())) 
        / NULLIF((votes + MIN(votes) OVER ()), 0) AS weighted_score --- calculate 
    , 1 AS in_stats 
  FROM {{ ref('cleaned_movie_statistics') }}
), 

ibmb AS (
  SELECT 
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
    , CAST(year AS INT64) AS year
    , null AS language
    , duration
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
    , genre_biography AS biography
    , genre_western AS western 
    , genre_film_noir AS film_noir
    , null AS worldwide_gross
    , ((rating * votes) + (MIN(votes) OVER () * AVG(rating) OVER ())) 
        / NULLIF((votes + MIN(votes) OVER ()), 0) AS weighted_score --- calculate 
    , 1 AS in_ibmb 
  FROM {{ ref('cleaned_imdb_movies') }}
), 

cinema AS (
  SELECT 
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
    , CAST(year AS INT64) AS year
    , null AS language
    , null AS duration
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
    , 0 AS film_noir
    , null AS worldwide_gross
    , ((rating * votes)
      + (MIN(votes) OVER () * AVG(rating) OVER ()))
    / NULLIF((votes + MIN(votes) OVER ()), 0) AS weighted_score
    , 1 AS in_cinema 
  FROM {{ ref('ba_total_cinema_box_office_join') }}
),

netflix AS (
  SELECT 
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
    , EXTRACT (YEAR FROM release_date) AS year
    , language_name AS language
    , null AS duration
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
    , 0 AS film_noir 
    , null AS worldwide_gross
    , ((vote_average * vote_count) + (MIN(vote_count) OVER () * AVG(vote_average) OVER ())) 
        / NULLIF((vote_count + MIN(vote_count) OVER ()), 0) AS weighted_score 
    , 1 AS in_netflix
  FROM {{ ref('cleaning_netflix_movies') }}
), 

bfi AS (
  SELECT 
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
    , EXTRACT (YEAR FROM release_date) AS year
    , language_name AS language
    , null AS duration
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
    , 0 AS film_noir  
    , null AS worldwide_gross
    , ((vote_average * vote_count) + (MIN(vote_count) OVER () * AVG(vote_average) OVER ())) 
        / NULLIF((vote_count + MIN(vote_count) OVER ()), 0) AS weighted_score 
    , 1 AS in_bfi 
  FROM {{ ref('cleaning_bfi_movies') }}
),


-- 2. Merge all tables with FULL OUTER JOINs on norm_title & year
merged_1 AS (
  SELECT
    -- master title & year
    COALESCE(l.norm_title, s.norm_title, i.norm_title, c.norm_title, n.norm_title, b.norm_title) AS norm_title
    , COALESCE(l.year, s.year, i.year, c.year, n.year, b.year) AS year
    , COALESCE(l.duration, s.duration, i.duration, c.duration, n.duration, b.duration) AS duration
    , COALESCE(l.worldwide_gross, s.worldwide_gross, i.worldwide_gross, c.worldwide_gross, n.worldwide_gross, b.worldwide_gross) AS worldwide_gross
    , COALESCE(s.language,  c.language, l.language, i.language, n.language, b.language) AS language
    -- weighted scores
    , c.weighted_score AS score_cinema
    , n.weighted_score AS score_netflix
    , b.weighted_score AS score_bfi
    , s.weighted_score AS score_stats
    , i.weighted_score AS score_ibmb
    -- genres (take first non-null in your priority)
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS action
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS adventure
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS animation
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS comedy
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS crime
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS documentary
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS drama
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS family
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS fantasy
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS history
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS horror
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS music
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS mystery
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS romance
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS scifi
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS tv_movie
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS thriller
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS war
    , COALESCE(c.action, n.action, b.action, s.action, i.action, l.action) AS western
    , COALESCE(s.action, i.action, l.action, c.action, n.action, b.action) AS biography
    , COALESCE(s.action, i.action, l.action, c.action, n.action, b.action) AS film_noir
    -- presence flags
    , IFNULL(l.in_list,    0) AS in_list
    , IFNULL(s.in_stats,   0) AS in_stats
    , IFNULL(i.in_ibmb,    0) AS in_ibmb
    , IFNULL(c.in_cinema,  0) AS in_cinema
    , IFNULL(n.in_netflix, 0) AS in_netflix
    , IFNULL(b.in_bfi,     0) AS in_bfi

  FROM list l
  FULL OUTER JOIN stats s
    ON l.norm_title = s.norm_title
   AND l.year = s.year
  FULL OUTER JOIN ibmb i
    ON COALESCE(l.norm_title, s.norm_title) = i.norm_title
   AND COALESCE(l.year, s.year) = i.year
  FULL OUTER JOIN cinema c
    ON COALESCE(l.norm_title, s.norm_title, i.norm_title) = c.norm_title
   AND COALESCE(l.year, s.year, i.year) = c.year
  FULL OUTER JOIN netflix n
    ON COALESCE(l.norm_title, s.norm_title, i.norm_title, c.norm_title) = n.norm_title
   AND COALESCE(l.year, s.year, i.year, c.year) = n.year
  FULL OUTER JOIN bfi b
    ON COALESCE(l.norm_title, s.norm_title, i.norm_title, c.norm_title, n.norm_title) = b.norm_title
   AND COALESCE(l.year, s.year, i.year, c.year, n.year) = b.year
)


SELECT * FROM merged_1 