SELECT ear_tag, breed, breeding_type, coat_color_dna,current_breeding_status, estimated_calving_date, last_breeding_date, last_calving_date, last_calving_date + integer '60' AS available
  FROM cattle.cow
  WHERE breed = 'CHAROLAIS' AND breeding_type = 'FLUSH' AND ((last_calving_date + integer '60') <= (DATE('2015-03-30')));
