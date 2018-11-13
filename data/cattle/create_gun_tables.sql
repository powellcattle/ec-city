SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET search_path = cattle, pg_catalog;
SET default_tablespace = '';
SET default_with_oids = false;


DROP TYPE IF EXISTS pistol_action_enum CASCADE;
CREATE TYPE pistol_action_enum AS ENUM ('revolver','semi-auto');
DROP TYPE IF EXISTS pistol_caliper_enum CASCADE;
CREATE TYPE pistol_caliper_enum AS ENUM ('.22lr','.380','9mm','.45colt','.45acp','.357','38special','.40');
DROP TYPE IF EXISTS rifle_action_enum CASCADE;
CREATE TYPE rifle_action_enum AS ENUM ('bolt-action','semi-auto','lever','breach');
DROP TYPE IF EXISTS rifle_caliper_enum CASCADE;
CREATE TYPE rifle_caliper_enum AS ENUM ('.22lr','.223','.270','22mag','7mm mag','.243');
DROP TYPE IF EXISTS item_condition_enum CASCADE;
CREATE TYPE item_condition_enum AS ENUM ('excellent','good','fair','poor');
DROP TYPE IF EXISTS optic_type_enum CASCADE;
CREATE TYPE optic_type_enum AS ENUM ('scope','thermo-scope','nightvision-scope','nightvision-monoculor');


DROP TABLE IF EXISTS inventory.item CASCADE;

CREATE TABLE inventory.item
(
  serial_number character varying(50) NOT NULL,
  manufacture character varying(50) NOT NULL,
  model character varying(25) NOT NULL,
  condition item_condition_enum NULL,
  notes character varying(250),
  purchase_date date,
  purchase_cost money,
  CONSTRAINT item_pkey PRIMARY KEY (serial_number)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE inventory.item
  OWNER TO postgres;

DROP TABLE IF EXISTS inventory.optics CASCADE;
CREATE TABLE inventory.optics
(
  optic_type optic_type_enum NOT NULL,
  magnification character varying(10) NULL
) INHERITS(inventory.item)
WITH (
  OIDS=FALSE
);
ALTER TABLE inventory.optics
  OWNER TO postgres;

DROP TABLE IF EXISTS inventory.pistol CASCADE;
CREATE TABLE inventory.pistol
(
  pistol_action pistol_action_enum NOT NULL,
  caliper pistol_caliper_enum NOT NULL,
  barrel_length character varying(10),
  handle_style character varying(10),
  finish character varying(20) NOT NULL
) INHERITS(inventory.item)
WITH (
  OIDS=FALSE
);
ALTER TABLE inventory.item
  OWNER TO postgres;

DROP TABLE IF EXISTS inventory.rifle CASCADE;
CREATE TABLE inventory.rifle
(
  rifle_action rifle_action_enum NOT NULL,
  caliper rifle_caliper_enum NOT NULL,
  barrel_length character varying(10),
  stock_style character varying(10),
  finish character varying(20) NOT NULL
) INHERITS(inventory.item)
WITH (
  OIDS=FALSE
);
ALTER TABLE inventory.rifle
  OWNER TO postgres;

DROP TABLE IF EXISTS inventory.shotgun CASCADE;
CREATE TABLE inventory.shotgun
(
) INHERITS(inventory.pistol)
WITH (
  OIDS=FALSE
);
ALTER TABLE inventory.shotgun
  OWNER TO postgres;

