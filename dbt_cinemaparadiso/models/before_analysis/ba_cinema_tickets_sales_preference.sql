SELECT 
  CASE 
    WHEN Age BETWEEN 18 AND 25 THEN '18-25'
    WHEN Age BETWEEN 26 AND 35 THEN '26-35'
    WHEN Age BETWEEN 36 AND 45 THEN '36-45'
    WHEN Age BETWEEN 46 AND 60 THEN '46-60'
  END AS Age_Group,
  Seat_Type,
  COUNT(*) AS total_tickets_sold,
  AVG(Ticket_Price) AS avg_ticket_price
FROM `cinemaparadiso-462409.cinema_paradiso.cleaned_cinema_ticket_sales`
GROUP BY Age_Group, Seat_Type
ORDER BY Age_Group, total_tickets_sold DESC;
