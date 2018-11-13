SELECT a.ear_tag, a.electronic_id, b.name, pc.result
  FROM cattle.animal AS a, cattle.movements AS m, cattle.breeds AS b, cattle.pregnancy_check AS pc
  WHERE a.id = m.animal_id AND m.moved_to_pasture_id = 463317 AND a.breed_id = b.id AND (a.id = pc.animal_id AND pc.check_date = '2016-04-22');

