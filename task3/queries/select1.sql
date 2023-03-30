-- 1. Вывести количество фильмов в каждой категории, отсортировать по убыванию.

SELECT category.name, COUNT(*) as count_films
FROM category
    JOIN film_category ON category.category_id = film_category.category_id
    JOIN film ON film_category.film_id = film.film_id
GROUP BY category.name
ORDER BY count_films DESC;

--     name     | count_films
-- -------------+-------------
--  Sports      |          74
--  Foreign     |          73
--  Family      |          69
--  Documentary |          68
--  Animation   |          66
--  Action      |          64
--  New         |          63
--  Drama       |          62
--  Sci-Fi      |          61
--  Games       |          61
--  Children    |          60
--  Comedy      |          58
--  Travel      |          57
--  Classics    |          57
--  Horror      |          56
--  Music       |          51
-- (16 rows)
