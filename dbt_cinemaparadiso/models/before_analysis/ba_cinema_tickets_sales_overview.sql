WITH general_stats AS (
    SELECT 
        'General Overview' AS Metric,
        COUNT(*) AS total_records, 
        COUNT(DISTINCT Ticket_ID) AS unique_ticket_ids, 
        AVG(Ticket_Price) AS avg_ticket_price, 
        MIN(Age) AS min_age, 
        MAX(Age) AS max_age,
        CAST(NULL AS STRING) AS category, 
        CAST(NULL AS STRING) AS seat_type, 
        CAST(NULL AS INT64) AS total_tickets_sold, 
        CAST(NULL AS FLOAT64) AS avg_persons
    FROM `cinemaparadiso-462409.cinema_paradiso.cleaned_cinema_ticket_sales`
),

popular_genres AS (
    SELECT 
        'Popular Genre' AS Metric, 
        CAST(NULL AS INT64) AS total_records, 
        CAST(NULL AS INT64) AS unique_ticket_ids, 
        CAST(NULL AS FLOAT64) AS avg_ticket_price, 
        CAST(NULL AS INT64) AS min_age, 
        CAST(NULL AS INT64) AS max_age,
        Movie_Genre AS category, 
        CAST(NULL AS STRING) AS seat_type, 
        COUNT(*) AS total_tickets_sold, 
        CAST(NULL AS FLOAT64) AS avg_persons
    FROM `cinemaparadiso-462409.cinema_paradiso.cleaned_cinema_ticket_sales`
    GROUP BY Movie_Genre
    ORDER BY total_tickets_sold DESC
    LIMIT 5
),

seat_preferences AS (
    SELECT 
        'Seat Preferences' AS Metric, 
        CAST(NULL AS INT64) AS total_records, 
        CAST(NULL AS INT64) AS unique_ticket_ids, 
        CAST(NULL AS FLOAT64) AS avg_ticket_price, 
        CAST(NULL AS INT64) AS min_age, 
        CAST(NULL AS INT64) AS max_age,
        CAST(NULL AS STRING) AS category, 
        Seat_Type AS seat_type, 
        COUNT(*) AS total_tickets_sold, 
        CAST(NULL AS FLOAT64) AS avg_persons
    FROM `cinemaparadiso-462409.cinema_paradiso.cleaned_cinema_ticket_sales`
    GROUP BY Seat_Type
    ORDER BY total_tickets_sold DESC
),

group_size_stats AS (
    SELECT 
        'Group Size Stats' AS Metric, 
        CAST(NULL AS INT64) AS total_records, 
        CAST(NULL AS INT64) AS unique_ticket_ids, 
        CAST(NULL AS FLOAT64) AS avg_ticket_price, 
        CAST(NULL AS INT64) AS min_age, 
        CAST(NULL AS INT64) AS max_age,
        CAST(NULL AS STRING) AS category, 
        CAST(NULL AS STRING) AS seat_type, 
        CAST(NULL AS INT64) AS total_tickets_sold, 
        AVG(Number_of_Person) AS avg_persons
    FROM `cinemaparadiso-462409.cinema_paradiso.cleaned_cinema_ticket_sales`
)

SELECT * FROM general_stats
UNION ALL
SELECT * FROM popular_genres
UNION ALL
SELECT * FROM seat_preferences
UNION ALL
SELECT * FROM group_size_stats

