SELECT
  title,
  year,
  
  -- Era classification
  CASE
    WHEN year < 2000 THEN "Classic"
    WHEN year >= 2000 AND year < 2010 THEN "Early modern"
    WHEN year >= 2010 AND year < 2020 THEN "Modern"
    WHEN year >= 2020 THEN "Contemporary"
    ELSE "Unknown"
  END AS era,
  
  -- Platform detection
  CASE
    WHEN in_cinema = 1 THEN "Cinema"
    WHEN in_netflix = 1 THEN "Netflix"
    WHEN in_bfi = 1 THEN "BFI"
    WHEN in_general = 1 THEN "General"
    ELSE "Other"
  END AS platform

FROM {{ref ('fa_genreal_cinema_bfi_netflix_merged')}}