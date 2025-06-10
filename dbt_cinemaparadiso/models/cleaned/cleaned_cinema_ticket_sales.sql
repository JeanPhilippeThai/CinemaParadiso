SELECT
    Ticket_ID as ticket_id,
    Age as age,
    Ticket_Price as ticket_price,
    Movie_Genre as movie_genre,
    Seat_Type as seat_type,
    Number_of_Person as number_of_person,
    Purchase_Again as purchase_again
From
    {{ ref('cleaning_cinema_ticket_sales') }}