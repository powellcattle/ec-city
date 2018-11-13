SELECT a.ear_tag AS "EAR TAG", a.birth_date AS "DOB", a1.ear_tag AS "DAM EAR_TAG"
  FROM cattle.animal AS a, cattle.pasture AS p, cattle.animal AS a1
  WHERE a.status='ACTIVE' AND a.animal_type='CALF' AND (a.pasture_id = p.id) AND a.dam_animal_id=a1.id AND
  a.birth_date < '2016-08-01'
  ORDER BY a.ear_tag_prefix
