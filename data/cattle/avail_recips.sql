SELECT ear_tag, breed, breeding_type, current_breeding_status, dob, last_breeding_date, last_calving_date, (last_calving_date + integer '60') AS available
  FROM cattle.cow
  WHERE ((((last_calving_date + integer '60') <= (DATE('2015-04-01'))) OR (last_calving_date IS NULL)) AND current_breeding_status = 'OPEN')  AND (breed != 'CHAROLAIS' AND breed != 'BRAHMAN');
