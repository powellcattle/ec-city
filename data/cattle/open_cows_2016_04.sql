SELECT a.ear_tag, a.electronic_id, a.brand, pc.result
  FROM cattle.animal AS a,  cattle.pregnancy_check AS pc
  WHERE a.status = 'ACTIVE' AND a.id = pc.animal_id AND  pc.check_method='ULTRASOUND' AND pc.result='OPEN' AND pc.check_date = '2016-04-22'
  ORDER BY a.ear_tag;

