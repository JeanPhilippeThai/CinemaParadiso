SELECT
  Ticket_ID,
  Age,
  Ticket_Price,
  Movie_Genre,
  Seat_Type,
  SAFE_CAST(
    CASE 
      WHEN LOWER(Number_of_Person) = 'alone' THEN '1'
      ELSE Number_of_Person
    END AS INT64
  ) AS Number_of_Person,
  
  Purchase_Again
From
{{ source('raw_bigquery_dataset', 'raw_cinema_ticket_sales') }}