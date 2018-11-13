SELECT ear_tag, breed, breeding_type, estimated_calving_date
  FROM cattle.cow
  WHERE current_breeding_status = 'PREGNANT'
  ORDER BY estimated_calving_date, ear_tag;
