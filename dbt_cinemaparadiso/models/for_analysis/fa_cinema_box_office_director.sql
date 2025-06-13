select cinema.* except(duration),
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
      ) 