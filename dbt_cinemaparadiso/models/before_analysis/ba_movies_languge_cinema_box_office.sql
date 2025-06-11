SELECT
  original_language,
  COUNT(*) AS movie_count,
  ROUND(AVG(imdb_rating), 2) AS avg_rating,
  ROUND(AVG(worldwide_gross)) AS avg_worldwide_gross
FROM {{ ref('cleaned_enhanced_box_office') }}
WHERE imdb_rating IS NOT NULL
GROUP BY original_language
ORDER BY movie_count DESC