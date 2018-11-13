select ear_tag, count(*) as a
from cattle.animal 
where status = 'ACTIVE'
group by ear_tag
having (count(*) > 1)
