SELECT a.ear_tag, b.name, a.reg_num, a.name, a.purchase_date, a.birth_date, a.animal_type
  FROM cattle.animal AS a, cattle.breed AS b
  WHERE 
  a.breed_id = b.id AND 
  b.name = 'CHAROLAIS' AND 
  status = 'ACTIVE' AND 
  a.sex != 'BULL' AND 
  (date_part('year',a.purchase_date) IN (2014,2015) OR (date_part('year',birth_date) IN (2014,2015) AND birth_date < '2015-11-01'))
