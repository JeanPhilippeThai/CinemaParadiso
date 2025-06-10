select
    *
from
    {{ ref('cleaned_cinema_ticket_sales') }}
limit 10