DROP VIEW IF EXISTS address.view_unique_street_names;
CREATE VIEW address.view_unique_street_nams AS
SELECT DISTINCT(st_name) AS st_name, st_full_name as st_full_name, st_prefix AS st_prefix, st_type AS st_type
FROM address.address_911
WHERE 'EL CAMPO' = city;