WITH avg_gross AS (
  SELECT AVG(worldwide_gross) AS avg_worldwide_gross
  FROM {{ ref('fa_total_cinema_box_office_join') }}
),
filtered_movies AS (
  SELECT 
    ceb.title,
    ceb.rating,
    ceb.votes,
    ceb.worldwide_gross,
    ceb.weighted_score
  FROM {{ ref('fa_total_cinema_box_office_join') }} ceb
  CROSS JOIN avg_gross
  WHERE ceb.worldwide_gross < avg_gross.avg_worldwide_gross
     OR ceb.rating > 8.5
)
SELECT *
FROM filtered_movies
ORDER BY rating DESC