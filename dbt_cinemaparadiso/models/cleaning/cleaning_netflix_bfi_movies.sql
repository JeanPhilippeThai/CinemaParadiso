with cte as(
  SELECT * FROM {{ ref('cleaning_bfi_movies')}}
  union all
  select * from {{ ref('cleaning_netflix_movies') }}
),
rn_table as(
  select
    *,
    row_number() over (partition by title order by id) as rn
  from cte
),
agg_table as(
  select
    title,
    COALESCE(round(AVG(vote_average * vote_count / NULLIF(vote_count, 0)), 2), 0) as vote_average,
    sum(vote_count) as vote_count,
    avg(popularity) as popularity,
    min(release_date) as release_date,
    MAX(action) as action,
    MAX(adventure) as adventure,
    MAX(animation) as animation,
    MAX(comedy) as comedy,
    MAX(crime) as crime,
    MAX(documentary) as documentary,
    MAX(drama) as drama,
    MAX(family) as family,
    MAX(fantasy) as fantasy,
    MAX(history) as history,
    MAX(horror) as horror,
    MAX(music) as music,
    MAX(mystery) as mystery,
    MAX(romance) as romance,
    MAX(science_fiction) as science_fiction,
    MAX(tv_movie) as tv_movie,
    MAX(thriller) as thriller,
    MAX(war) as war,
    MAX(western) as western
  from rn_table
  group by
    title
)
select
  a.*,
  r.id,
  r.language_name,
from rn_table r
join agg_table a
  on r.title = a.title
where r.rn=1

