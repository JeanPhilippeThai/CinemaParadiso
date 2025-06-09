SELECT
  Ticket_ID as rodrigo_ticket_id,
  age,
  Ticket_Price
FROM {{ source('raw_cinema_tickets_sales', 'bfi_movies') }}
