SELECT rooms.name AS room_name,
       CAST(MAX(students.birthday) - MIN(students.birthday) AS CHAR) AS age_diff
FROM rooms
    JOIN students ON rooms.id = students.room_id  GROUP BY rooms.name
ORDER BY age_diff DESC
LIMIT 5;
