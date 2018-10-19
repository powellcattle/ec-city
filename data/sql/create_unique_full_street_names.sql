CREATE TABLE address.unique_full_street_names AS
SELECT DISTINCT(st_name) as streetname, objectid as id, st_predir as st_predir, st_fullname as full_address, st_type as st_postype
	FROM sde.starmap 
	WHERE 'EL CAMPO' = city