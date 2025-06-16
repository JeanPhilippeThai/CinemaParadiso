WITH total AS (select cinema.* except(duration),
movie.director AS director,
COALESCE(cinema.duration, movie.duration) AS duration
FROM {{ ref('fa_total_cinema_box_office_join') }} AS cinema
left join {{ ref('cleaning_movie_statistics') }} AS movie
on 
LOWER( ---lower all letters
        REGEXP_REPLACE(
          TRIM(
            TRANSLATE(cinema.title,
              'áàäâãåéèëêíìïîóòöôõúùüûñçÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÑÇ#&@*!?',
              'aaaaaaeeeeiiiiooooouuuuncAAAAAAEEEEIIIIOOOOOUUUUNC      ' ---remove all the accents from all letters
            )
          ),
          r'[^a-zA-Z0-9\s]', '' ---remove all special characters
        )
      ) 
  =LOWER( ---lower all letters
        REGEXP_REPLACE(
          TRIM(
            TRANSLATE(movie.title,
              'áàäâãåéèëêíìïîóòöôõúùüûñçÁÀÄÂÃÅÉÈËÊÍÌÏÎÓÒÖÔÕÚÙÜÛÑÇ#&@*!?',
              'aaaaaaeeeeiiiiooooouuuuncAAAAAAEEEEIIIIOOOOOUUUUNC      ' ---remove all the accents from all letters
            )
          ),
          r'[^a-zA-Z0-9\s]', '' ---remove all special characters
        )
      ) )
      SELECT *,
      CASE 
          WHEN duration >= 180 THEN "Extra long(>=180)"
          WHEN duration >= 121 AND duration < 180 THEN "long (120-179)"
          WHEN duration >= 104 AND duration < 121 THEN "Medium long (103-119)"
          WHEN duration >= 93 AND duration < 104 THEN "Medium short (93-102)"
          WHEN duration < 93 THEN "Short (=<92)"
          ELSE "unknown"
      END AS duration_classification
      FROM total 