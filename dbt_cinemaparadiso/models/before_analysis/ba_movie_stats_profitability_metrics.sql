SELECT 
  title_title,
  year,
  production_budget,
  domestic_gross,
  worldwide_gross,
  -- Calculate profit margins
  (domestic_gross - production_budget) as domestic_profit,
  (worldwide_gross - production_budget) as worldwide_profit,
  -- Calculate ROI (Return on Investment)
  ROUND((domestic_gross / production_budget), 2) as domestic_roi,
  ROUND((worldwide_gross / production_budget), 2) as worldwide_roi,
  -- Profitability categories
  CASE 
    WHEN (worldwide_gross / production_budget) >= 3.0 THEN 'Highly Profitable'
    WHEN (worldwide_gross / production_budget) >= 2.0 THEN 'Profitable' 
    WHEN (worldwide_gross / production_budget) >= 1.0 THEN 'Break Even'
    ELSE 'Loss'
  END as profitability_category,
  rating,  -- Changed from movie_averageRating
  votes,   -- Changed from movie_numerOfVotes
  -- Budget efficiency score (quality per dollar spent)
  ROUND((rating * votes) / production_budget, 4) as quality_efficiency_score,
  -- Domestic vs International performance
  ROUND((domestic_gross / worldwide_gross) * 100, 1) as domestic_percentage,
  -- Engagement to budget ratio
  ROUND(votes / (production_budget / 1000000), 2) as votes_per_million_budget

FROM {{ ref('cleaned_movie_statistics') }}
WHERE production_budget > 0 AND worldwide_gross > 0
ORDER BY worldwide_roi DESC



