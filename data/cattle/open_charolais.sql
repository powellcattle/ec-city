SELECT a.ear_tag AS "EAR TAG"
  FROM cattle.animal AS a, cattle.breeds AS b
  WHERE a.status='ACTIVE' AND a.animal_type='COW' AND (a.breed_id = b.id AND b.name = 'CHAROLAIS') AND current_breeding_status != 'PREGNANT'
  ORDER BY a.ear_tag_prefix, a.ear_tag_year_desig, a.ear_tag_color;
