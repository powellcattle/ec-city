SELECT ear_tag, breed, breeding_type, coat_color_dna,current_breeding_status, estimated_calving_date, last_breeding_date, last_calving_date
  FROM cattle.cow
  WHERE (last_calving_date + integer '45' <= (DATE('2015-05-01')) AND current_breeding_status = 'OPEN' AND breed != 'CHAROLAIS') 
