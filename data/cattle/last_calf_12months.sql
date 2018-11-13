SELECT a.ear_tag, a.electronic_id, (EXTRACT(year from AGE(a.last_calving_date))*12 + EXTRACT(month from AGE(a.last_calving_date))) AS months
  FROM cattle.animal AS a
  WHERE a.status = 'ACTIVE' AND (EXTRACT(year from AGE(a.last_calving_date))*12 + EXTRACT(month from AGE(a.last_calving_date))) > 12;

