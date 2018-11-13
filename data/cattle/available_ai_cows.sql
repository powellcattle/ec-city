SELECT ear_tag, breed, breeding_type, coat_color_dna,current_breeding_status, estimated_calving_date, last_breeding_date, last_calving_date, last_calving_date + integer '60' AS available
  FROM cattle.cow
  WHERE ((last_calving_date + integer '60' <= (DATE('2015-04-27')) AND current_breeding_status = 'OPEN' AND breed = 'CHAROLAIS')) OR (current_breeding_status = 'OPEN' AND breed = 'CHAROLAIS');
