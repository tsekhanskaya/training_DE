-- 5. Вывести топ 3 актеров, которые больше всего появлялись в фильмах
-- в категории “Children”. Если у нескольких актеров одинаковое кол-во фильмов, вывести всех.

SELECT actor_name, film_count
FROM (
  SELECT a.first_name || ' ' || a.last_name AS actor_name, COUNT(*) AS film_count, RANK() OVER (ORDER BY COUNT(*) DESC) AS rank
  FROM actor AS a
      JOIN film_actor AS fa ON a.actor_id = fa.actor_id
      JOIN film_category AS fc ON fa.film_id = fc.film_id
      JOIN category AS c ON fc.category_id = c.category_id
  WHERE c.name = 'Children'
  GROUP BY a.actor_id
) subquery
WHERE rank <= 3;

--   actor_name   | film_count
-- ---------------+------------
--  HELEN VOIGHT  |          7
--  MARY TANDY    |          5
--  RALPH CRUZ    |          5
--  WHOOPI HURT   |          5
--  KEVIN GARLAND |          5
-- (5 rows)

