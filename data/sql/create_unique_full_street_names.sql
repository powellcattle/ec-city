DROP TABLE IF EXISTS address.unique_street_names;
CREATE TABLE address.unique_street_names AS
SELECT DISTINCT(st_name) AS st_name, st_full_name as st_full_name, st_prefix AS st_prefix, st_type AS st_type, fuzzy
                            as fuzzy
FROM address.address_911
WHERE 'EL CAMPO' = city;

SELECT objectid, shape, pwid, pwname, st_predir, st_name, st_type, st_fullname, from_addr_l, to_addr_l, from_addr_r, to_addr_r, source, global_id, city
	FROM sde.starmap;


DROP TABLE IF EXISTS address.unique_addresses;
CREATE TABLE address.unique_addresses AS
SELECT DISTINCT(add_address) AS add_address, st_name as st_name, st_full_name as st_full_name, st_prefix AS
                                st_prefix, st_type AS st_type, fuzzy as fuzzy
FROM address.address_911
WHERE 'EL CAMPO' = city;