-- 2. Вывести 10 актеров, чьи фильмы большего всего арендовали, отсортировать по убыванию.

SELECT actor.first_name, actor.last_name, COUNT(*) AS rental_count
FROM actor
    JOIN film_actor ON actor.actor_id = film_actor.actor_id
    JOIN inventory ON film_actor.film_id = inventory.film_id
    JOIN rental ON inventory.inventory_id = rental.inventory_id
GROUP BY actor.actor_id
ORDER BY rental_count DESC
LIMIT 10;

--  first_name |  last_name  | rental_count
-- ------------+-------------+--------------
--  GINA       | DEGENERES   |          753
--  MATTHEW    | CARREY      |          678
--  MARY       | KEITEL      |          674
--  ANGELA     | WITHERSPOON |          654
--  WALTER     | TORN        |          640
--  HENRY      | BERRY       |          612
--  JAYNE      | NOLTE       |          611
--  VAL        | BOLGER      |          605
--  SANDRA     | KILMER      |          604
--  SEAN       | GUINESS     |          599
-- (10 rows)
