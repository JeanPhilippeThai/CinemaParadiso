WITH avg_gross AS (
  SELECT AVG(worldwide_gross) AS avg_worldwide_gross
  FROM {{ ref('cleaned_enhanced_box_office') }}
),
filtered_movies AS (
  SELECT 
    ceb.title,
    ceb.imdb_rating,
    ceb.vote_count,
    ceb.worldwide_gross,
    ceb.weighted_score
  FROM {{ ref('cleaned_enhanced_box_office') }} ceb
  CROSS JOIN avg_gross
  WHERE ceb.worldwide_gross < avg_gross.avg_worldwide_gross
     OR ceb.imdb_rating > 8.5
)
SELECT *
FROM filtered_movies
ORDER BY imdb_rating DESC