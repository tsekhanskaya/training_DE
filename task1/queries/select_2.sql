SELECT rooms.id,
       rooms.name,
       ROUND(AVG(EXTRACT(YEAR FROM age(CURRENT_DATE, students.birthday)))::numeric, 2)::float AS avg_age
FROM rooms
JOIN students ON rooms.id = students.room_id
GROUP BY rooms.id, rooms.name
ORDER BY avg_age ASC
LIMIT 5;
