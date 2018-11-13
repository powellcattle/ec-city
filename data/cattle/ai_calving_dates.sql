SELECT a.ear_tag AS "VID", p.breeding_date AS "AI Date", p.breeding_date + 285 AS "Calving AI", p.breeding_date + 285 + 21 AS "Calving Cleanup"
  FROM cattle.animal AS a, cattle.breeding AS p
  WHERE a.id = p.animal_id AND
  breeding_method = 'AI' AND date_part('year',p.breeding_date) = 2015
  ORDER BY a.ear_tag ASC;
