-- 7. Вывести категорию фильмов, у которой самое большое кол-во часов суммарной
-- аренды в городах (customer.address_id в этом city), и которые начинаются на букву “a”.
-- То же самое сделать для городов в которых есть символ “-”. Написать все в одном запросе.

SELECT c.name
     , EXTRACT(HOUR FROM SUM(r.return_date - r.rental_date)) AS total_rental_hours
FROM category AS c
    JOIN film_category AS fc ON fc.category_id = c.category_id
    JOIN film AS f ON f.film_id = fc.film_id
    JOIN inventory AS i ON i.film_id = f.film_id
    JOIN rental AS r ON r.inventory_id = i.inventory_id
    JOIN customer AS cust ON cust.customer_id = r.customer_id
    JOIN address AS a ON a.address_id = cust.address_id
    JOIN city AS ci ON ci.city_id = a.city_id
WHERE (ci.city LIKE 'a%') OR (ci.city LIKE '%-%')
GROUP BY c.name
ORDER BY SUM(r.return_date - r.rental_date) DESC
LIMIT 1;

--   name   | total_rental_hours
-- ---------+--------------------
--  Foreign |                736
-- (1 row)
