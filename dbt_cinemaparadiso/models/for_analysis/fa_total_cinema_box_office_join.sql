SELECT 
    *,
  (
    (rating * votes) + (MIN(votes) OVER () * AVG(rating) OVER ())
  ) / NULLIF((votes + MIN(votes) OVER ()), 0) AS weighted_score,
  CASE original_language
    WHEN 'ar' THEN 'Arabic'
    WHEN 'cn' THEN 'Chinese'
    WHEN 'cs' THEN 'Czech'
    WHEN 'da' THEN 'Danish'
    WHEN 'de' THEN 'German'
    WHEN 'el' THEN 'Greek'
    WHEN 'en' THEN 'English'
    WHEN 'es' THEN 'Spanish'
    WHEN 'et' THEN 'Estonian'
    WHEN 'fa' THEN 'Persian'
    WHEN 'fi' THEN 'Finnish'
    WHEN 'fr' THEN 'French'
    WHEN 'hi' THEN 'Hindi'
    WHEN 'id' THEN 'Indonesian'
    WHEN 'it' THEN 'Italian'
    WHEN 'ja' THEN 'Japanese'
    WHEN 'kn' THEN 'Kannada'
    WHEN 'ko' THEN 'Korean'
    WHEN 'ml' THEN 'Malayalam'
    WHEN 'nl' THEN 'Dutch'
    WHEN 'no' THEN 'Norwegian'
    WHEN 'pa' THEN 'Punjabi'
    WHEN 'pl' THEN 'Polish'
    WHEN 'pt' THEN 'Portuguese'
    WHEN 'ru' THEN 'Russian'
    WHEN 'sr' THEN 'Serbian'
    WHEN 'sv' THEN 'Swedish'
    WHEN 'ta' THEN 'Tamil'
    WHEN 'te' THEN 'Telugu'
    WHEN 'th' THEN 'Thai'
    WHEN 'tl' THEN 'Tagalog'
    WHEN 'tr' THEN 'Turkish'
    WHEN 'uk' THEN 'Ukrainian'
    WHEN 'vi' THEN 'Vietnamese'
    WHEN 'zh' THEN 'Chinese'
    ELSE 'Unknown'
  END AS language_name
FROM {{ ref('ba_total_cinema_box_office_join') }}