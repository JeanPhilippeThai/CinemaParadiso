--- 1. Filter out movies shorter then 70 min and splited the genre column in individual columns (1 present, 0 abscent)
--- 2. Added the weighted score.
SELECT 
  *,
  ((rating * votes) + (MIN(votes) OVER () * AVG(rating) OVER ())) / 
      (votes + MIN(votes) OVER ()) AS weighted_score , 
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
      CASE WHEN CONTAINS_SUBSTR(genre, 'Film-Noir') THEN 1 ELSE 0 END AS genre_film_noir
FROM {{ ref('ba_list_statistics_imdb_merged') }}
WHERE duration > 70

