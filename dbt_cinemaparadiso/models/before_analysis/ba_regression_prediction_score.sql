---1 Subqueries selecting the columns that we want to merge from each table and adding a columns with 1/0 if present of not in the category cinema, netflix of bfi. 

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
    , year
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
  FROM cinemaparadiso-462409.cinema_paradiso.cleaned_list_movies
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
    , year
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
  FROM cinemaparadiso-462409.cinema_paradiso.cleaned_movie_statistics
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
    , year
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
  FROM cinemaparadiso-462409.cinema_paradiso.cleaned_imdb_movies
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
    , year
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
    , weighted_score
    , 1 AS in_cinema 
  FROM cinemaparadiso-462409.cinema_paradiso.fa_total_cinema_box_office_join
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
  FROM cinemaparadiso-462409.cinema_paradiso.cleaning_netflix_movies
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
  FROM cinemaparadiso-462409.cinema_paradiso.cleaning_bfi_movies
)



--- 2. Select which columns to merge and in case of match, which table should be priorities to fill the title, year, weighted score and genre

merged_1 AS (
  SELECT 
    COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS title
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS year
    , c.weighted_score AS score_cinema
    , n.weighted_score AS score_netflix
    , b.weighted_score AS score_bfi
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS action
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS adventure
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS animation
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS comedy
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS crime
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS documentary
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS drama
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS family
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS fantasy
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS history
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS horror
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS music
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS mystery
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS romance
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS scifi
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS tv_movie
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS thriller
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS war
    , COALESCE(n.norm_title, b.norm_title, l.norm_title, s.norm_title, i.norm_title, c.norm_title) AS western
    , COALESCE(l.norm_title, s.norm_title, i.norm_title, n.norm_title, b.norm_title, c.norm_title) AS biography
    , COALESCE(l.norm_title, s.norm_title, i.norm_title, n.norm_title, b.norm_title, c.norm_title) AS film_noir
    , IFNULL(c.in_cinema, 0) AS in_cinema
    , IFNULL(n.in_netflix, 0) AS in_netflix
    , IFNULL(b.in_bfi, 0) AS in_bfi
  FROM list l
  FULL OUTER JOIN stats s ON g.title = c.title AND g.year = c.year
  FULL OUTER JOIN ibmb i ON g.title = c.title AND g.year = c.year
  FULL OUTER JOIN cinema c ON g.title = c.title AND g.year = c.year
  FULL OUTER JOIN netflix n ON COALESCE(g.title, c.title) = n.title AND COALESCE(g.year, c.year) = n.year
  FULL OUTER JOIN bfi b ON COALESCE(g.title, c.title, n.title) = b.title AND COALESCE(g.year, c.year, n.year) = b.year
)
