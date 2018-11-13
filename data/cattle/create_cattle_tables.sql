SET statement_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SET check_function_bodies = false;
SET client_min_messages = warning;
SET search_path = cattle, pg_catalog;
SET default_tablespace = '';
SET default_with_oids = false;

DROP TABLE IF EXISTS cattle.contact CASCADE;
CREATE TABLE cattle.contact
(
  id integer NOT NULL,
  name character varying(50) NOT NULL,
  CONSTRAINT cattle_contact_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE contact OWNER TO postgres;

DROP TABLE IF EXISTS cattle.calf CASCADE;
CREATE TABLE cattle.calf
(
  id integer NOT NULL,
  sex character varying(10) NOT NULL,
  breed character varying(20) NULL,
  coat_color_dna character varying(10) NULL,
  dob date NULL,
  ear_tag varchar(50) NOT NULL,
  birth_weight integer NULL,
  weaning_weight integer NULL,
  yearling_weight integer NULL,
  adj_birth_weight integer NULL,
  adj_weaning_weight integer NULL,
  adj_yearling_weight integer NULL,  
  CONSTRAINT calf_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE cattle.calf
  OWNER TO postgres;

DROP TABLE IF EXISTS cattle.cow CASCADE;
CREATE TABLE cattle.cow
(
  id integer NOT NULL,
  breed character varying(20),
  breeding_type character varying(10),
  coat_color_dna character varying(10),
  current_breeding_status character varying(10),
  dob date,
  ear_tag character varying(50),
  estimated_calving_date date,
  last_breeding_date date,
  last_calving_date date,
  contact_id integer,
  CONSTRAINT cow_pkey PRIMARY KEY (id)
)
WITH (
  OIDS=FALSE
);
ALTER TABLE cattle.cow
  OWNER TO postgres;

DROP TABLE IF EXISTS cattle.breeding CASCADE;

CREATE TABLE cattle.breeding
(
  id integer NOT NULL,
  animal_id integer NOT NULL,
  bull_animal_id integer,
  breeding_method character(2),
  breeding_date date NOT NULL,
  breeding_end_date date,
  estimated_calving_date date NOT NULL,
  cleanup boolean NOT NULL,
  embryo_id integer,
  pregnancy_check_id integer,
  CONSTRAINT breeding_pkey PRIMARY KEY (id),
  CONSTRAINT breeding_animal_id_fkey FOREIGN KEY (animal_id)
      REFERENCES cattle.cow (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE CASCADE
)
WITH (
  OIDS=FALSE
);
ALTER TABLE cattle.breeding
  OWNER TO postgres;

DROP TABLE IF EXISTS cattle.pregnancy_check CASCADE;
CREATE TABLE cattle.pregnancy_check
(
  id integer NOT NULL,
  animal_id integer NOT NULL,
  check_date date NOT NULL,
  check_method character varying(12) NOT NULL,
  result character varying(10) NOT NULL,
  ultrasound_sex character varying(8),
  expected_due_date date,
  CONSTRAINT pregnancy_check_pkey PRIMARY KEY (id),
  CONSTRAINT pregnancy_check_animal_id_fkey FOREIGN KEY (animal_id)
      REFERENCES cattle.cow (id) MATCH SIMPLE
      ON UPDATE NO ACTION ON DELETE NO ACTION
)
WITH (
  OIDS=FALSE
);
ALTER TABLE cattle.pregnancy_check
  OWNER TO postgres;

