SELECT a.ear_tag AS "TAG", a.conception_method AS "METHOD", a1.name AS "SIRE", a1.reg_num AS "SIRE REG", a2.ear_tag AS "RECIP", a3.name AS "GENETIC DAM", a3.reg_num AS "GENETIC DAM REG",
a.birth_date AS "DOB", a.birth_weight AS "BW", c.breeding_date AS "BREED START", c.breeding_end_date AS "BREED END"
FROM cattle.animal AS a, cattle.breed AS b, cattle.animal AS a1, cattle.animal AS a2, cattle.animal AS a3, cattle.breeding AS c
  WHERE  
  a.breed_id = b.id AND b.name = 'CHAROLAIS' AND 
  a.animal_type='CALF' AND 
  a.status = 'ACTIVE' AND 
  a.birth_year = 2016 AND 
  a.conception_method = 'AI' AND
  a.sire_animal_id = a1.id AND
  a.dam_animal_id = a2.id AND
  a.real_dam_animal_id = a3.id AND
  a3.id = c.animal_id AND
  a1.id = c.bull_animal_id
  ORDER BY a.ear_tag;
