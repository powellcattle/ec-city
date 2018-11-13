SELECT a.ear_tag AS "TAG", a.conception_method AS "METHOD", a1.name AS "SIRE", a1.reg_num AS "SIRE REG", a2.ear_tag AS "RECIP", a3.name AS "GENETIC DAM", a3.reg_num AS "GENETIC DAM REG",
a.birth_date AS "DOB", a.birth_weight AS "BW", c.breeding_date AS "BREED START", c.breeding_end_date AS "BREED END"
FROM cattle.animal AS a, cattle.breed AS b, cattle.animal AS a1, cattle.animal AS a2, cattle.animal AS a3, cattle.breeding AS c, cattle.embryos AS e
  WHERE  
  a.breed_id = b.id AND b.name = 'CHAROLAIS' AND 
  a.animal_type='CALF' AND 
  a.status = 'ACTIVE' AND 
  a.birth_year = 2015 AND 
  a.conception_method = 'ET' AND
  (a.dam_animal_id = c.animal_id AND c.embryo_id = e.id) AND
  e.sire_id = a1.id AND
  e.dam_id = a2.id
  ORDER BY a.ear_tag;
