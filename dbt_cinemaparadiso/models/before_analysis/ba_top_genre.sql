SELECT 'Action' AS genre, SUM(genre_action) AS count FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'Adventure', SUM(genre_adventure) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'Animation', SUM(genre_animation) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'Comedy', SUM(genre_comedy) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'Crime', SUM(genre_crime) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'Documentary', SUM(genre_documentary) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'Drama', SUM(genre_drama) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'Family', SUM(genre_family) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'Fantasy', SUM(genre_fantasy) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'History', SUM(genre_history) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'Horror', SUM(genre_horror) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'Music', SUM(genre_music) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'Mystery', SUM(genre_mystery) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'Romance', SUM(genre_romance) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'Sci-Fi', SUM(genre_scifi) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'TV Movie', SUM(genre_tv_movie) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'Thriller', SUM(genre_thriller) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'War', SUM(genre_war) FROM {{ ref('cleaned_enhanced_box_office') }} UNION ALL
SELECT 'Western', SUM(genre_western) FROM {{ ref('cleaned_enhanced_box_office') }}
ORDER BY count DESC