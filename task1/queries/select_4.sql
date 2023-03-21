SELECT DISTINCT rooms.name
FROM rooms
    JOIN students ON rooms.id = students.room_id
WHERE EXISTS (
    SELECT *
    FROM students AS s1
        JOIN students AS s2 ON s1.room_id = s2.room_id
    WHERE s1.sex <> s2.sex AND s1.room_id = students.room_id);
