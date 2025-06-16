SELECT
  language_name,
  COUNT(*) AS movie_count,
  ROUND(AVG(weighted_score), 2) AS weighted_score,
  ROUND(AVG(worldwide_gross)) AS avg_worldwide_gross
FROM {{ ref('fa_total_cinema_box_office_join') }}
WHERE rating IS NOT NULL
GROUP BY language_name
ORDER BY movie_count DESC