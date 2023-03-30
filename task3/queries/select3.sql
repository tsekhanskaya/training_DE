-- 3. Вывести категорию фильмов, на которую потратили больше всего денег.

SELECT c.name AS category_name, SUM(p.amount) AS total_spent
FROM payment AS p
    JOIN rental AS r ON p.rental_id = r.rental_id
    JOIN inventory AS i ON r.inventory_id = i.inventory_id
    JOIN film AS f ON i.film_id = f.film_id
    JOIN film_category AS fc ON f.film_id = fc.film_id
    JOIN category AS c ON fc.category_id = c.category_id
GROUP BY c.name
ORDER BY total_spent DESC
LIMIT 1;

--  category_name | total_spent
-- ---------------+-------------
--  Sports        |     5314.21
-- (1 row)
