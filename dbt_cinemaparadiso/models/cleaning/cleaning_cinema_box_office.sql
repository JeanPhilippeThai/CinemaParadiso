CREATE OR REPLACE VIEW cinemaparadiso-462409.cinema_paradiso.cleaned_cinema_box_office AS
WITH renamed_and_coalesced AS (
  SELECT
    Movie AS title,
    Year AS movie_year,
    Score AS score,
    `Adjusted Score` AS adjusted_score,
    COALESCE(Director, 'unknown') AS director,
    COALESCE('Cast', 'unknown') AS movie_cast, -- Renamed to avoid keyword 'Cast'
    `Box Office Collection` AS box_office_collection, -- Renamed and handled space
    time_minute,
    Votes AS imdb_votes -- Renamed for clarity
  FROM
    `cinemaparadiso-462409.cinema_paradiso.raw_cinema_box_office` -- Added backticks for robustness
),
final_cleaning AS (
  SELECT
    *,
    LOWER(TRIM(title)) AS cleaned_movie_title -- Corrected alias from 'movie_title' to 'title'
  FROM renamed_and_coalesced
)
SELECT *
FROM final_cleaning;