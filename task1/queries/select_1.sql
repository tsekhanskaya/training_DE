SELECT rooms.name,
       COUNT(students.id) AS student_count
FROM rooms
    LEFT JOIN students ON rooms.id = students.room_id
GROUP BY rooms.id;
