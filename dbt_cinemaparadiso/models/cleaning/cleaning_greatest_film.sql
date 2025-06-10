--- remove the unnecessary columns (writer, actors, plot, poster)
with cleaned_1 as (
SELECT index, imdb_id, rank_afc_2007, rank_afc_1998, rank_wga_2005, rank_ss_2012, 
       included_in_guardian_list, included_in_1001_book, title, year, rated, released, 
       runtime, genre, director, language, country, awards, metascore, actors,
       imdb_rating, imdb_votes, alternate_title
  FROM {{ source('raw_bigquery_dataset', 'raw_greatest_films') }}
),



---query that remove the duplicates#
cleaned_2 as(
SELECT *, ROW_NUMBER() OVER (PARTITION BY title, year ORDER BY title) AS row_num
  FROM `cleaned_1`

)

select 
included_in_guardian_list
,included_in_1001_book
,title
, rank_afc_2007
, rank_afc_1998
, rank_wga_2005
, rank_ss_2012
, year
, rated
, runtime
, genre
, director
, awards 
, country
, imdb_rating
, imdb_votes
, released
,actors
,language
from cleaned_2


