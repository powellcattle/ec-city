SELECT a.ear_tag AS "VID", pc.check_date AS "Check Date", pc.expected_due_date AS "Due Date"
FROM cattle.animal AS a, cattle.pregnancy_check AS pc
WHERE a.id = pc.animal_id AND pc.result = 'PREGNANT' AND (pc.expected_due_date > '2016-02-01' AND pc.expected_due_date < '2016-03-01')
ORDER BY pc.expected_due_date, a.ear_tag;
