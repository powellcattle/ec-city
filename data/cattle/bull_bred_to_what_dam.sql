SELECT bull.ear_tag AS tag, (SELECT dam.ear_tag FROM cattle.animal AS dam WHERE breedings.animal_id = dam.id)
FROM cattle.animal bull, cattle.breeding breedings, cattle.breeds breed
WHERE bull.id = breedings.bull_animal_id 
AND DATE_PART('year',breedings.breeding_date) = 2016
AND (bull.breed_id = breed.id AND breed.name = 'CHAROLAIS')


