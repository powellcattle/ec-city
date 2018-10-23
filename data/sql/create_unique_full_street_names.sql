CREATE TABLE address.unique_full_street_names AS 
SELECT DISTINCT(st_fullname) AS full_address, st_name AS streetname, st_predir AS st_predir, st_type AS st_postype
FROM sde.starmap
WHERE 'EL CAMPO' = city;
ALTER TABLE address.unique_full_street_names ADD CONSTRAINT full_street_names_pkey PRIMARY KEY (full_address);