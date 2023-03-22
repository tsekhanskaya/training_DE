CREATE INDEX students_room_id_idx ON students (room_id);
CREATE INDEX students_sex_room_id_idx ON students (sex, room_id);
CREATE INDEX students_room_id_birthday_idx ON students (room_id, birthday);
CREATE INDEX rooms_id_name_idx ON rooms (id, name);