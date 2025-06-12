--- year and duration are classified in different segments and added the weighted score
SELECT 
    title
    , CASE
        WHEN SAFE_CAST(year AS INT64) < 2000 THEN 'Classic'
        WHEN SAFE_CAST(year AS INT64) BETWEEN 2000 AND 2010 THEN 'Early Modern'
        WHEN SAFE_CAST(year AS INT64) BETWEEN 2011 AND 2016 THEN 'Modern'
        WHEN SAFE_CAST(year AS INT64) > 2016 THEN 'Contemporary'
      END AS year_category
    , CASE
          WHEN duration BETWEEN 71 AND 92 THEN 'Short'
          WHEN duration BETWEEN 93 AND 102 THEN 'Medium-Short'
          WHEN duration BETWEEN 103 AND 119 THEN 'Medium-Long'
          WHEN duration BETWEEN 120 AND 180 THEN 'Long'
          WHEN duration > 180 THEN 'Extra Long'
          ELSE 'Unknown'
    END AS duration_category
    , rating
    , votes
    , genre_action
    , genre_adventure
    , genre_animation
    , genre_comedy
    , genre_crime
    , genre_documentary
    , genre_drama
    , genre_family
    , genre_fantasy
    , genre_history
    , genre_horror
    , genre_musical
    , genre_music
    , genre_mystery
    , genre_romance
    , genre_scifi
    , genre_thriller
    , genre_war
    , genre_western
    , genre_biography
    , genre_film_noir
    , ((rating * votes) + (MIN(votes) OVER () * AVG(rating) OVER ())) / 
        (votes + MIN(votes) OVER ()) AS weighted_score
FROM cinemaparadiso-462409.cinema_paradiso.fa_list_statistics_imdb_merged_bygenre


