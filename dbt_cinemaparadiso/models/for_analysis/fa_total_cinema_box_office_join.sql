SELECT 
    *,
  (
    (rating * votes) + (MIN(votes) OVER () * AVG(rating) OVER ())
  ) / NULLIF((votes + MIN(votes) OVER ()), 0) AS weighted_score
FROM {{ ref('ba_total_cinema_box_office_join') }}