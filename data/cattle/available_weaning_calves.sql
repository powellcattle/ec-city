SELECT ear_tag, dob, sex, date_part('day',now() - dob)::int AS days_old
  FROM cattle.calf
  WHERE breed = 'CHAROLAIS' AND dob < date_trunc('day', now() - interval '140 day')
  ORDER BY days_old DESC
