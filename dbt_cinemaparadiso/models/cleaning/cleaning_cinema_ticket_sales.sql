SELECT
  Ticket_ID as rodrigo_ticket_id,
  age,
  Ticket_Price
FROM {{ source('raw_bigquery_dataset', 'raw_cinema_ticket_sales') }}

