SELECT a.ear_tag, a.electronic_id, b.name
  FROM cattle.animal AS a, cattle.movements AS m, cattle.breeds AS b
  WHERE a.id = m.animal_id AND m.moved_to_pasture_id = 430668 AND m.movement_date = '2015-11-27' AND a.breed_id = b.id;

