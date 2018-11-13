SELECT distinct(cow.ear_tag) AS EAR_TAG, (EXTRACT(epoch FROM (SELECT (NOW() - cow.last_calving_date)))/86400)::int AS DAYS
  FROM cattle.cow AS cow, cattle.pregnancy_check AS preg
  WHERE 
  cow.id = preg.animal_id AND 
  cow.current_breeding_status != 'PREGNANT' AND 
  cow.last_calving_date IS NOT NULL AND (EXTRACT(epoch FROM (SELECT (NOW() - cow.last_calving_date)))/86400)::int > 365;
