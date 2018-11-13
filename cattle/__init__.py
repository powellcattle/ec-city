#!/usr/bin/python
# -*- coding: utf-8 -*-
import inspect
import logging

import ec_psql_util

__author__ = 'spowell'

import csv
import psycopg2
import sys

import ec_hashmap
import ec_util

SQL_INSERT_BULL = "INSERT INTO cattle.bull(" \
                  "id," \
                  "sire_id," \
                  "dam_id," \
                  "real_dam_id," \
                  "breed, " \
                  "breeding_type, " \
                  "coat_color_dna, " \
                  "current_breeding_status," \
                  "dob, " \
                  "ear_tag, " \
                  "contact_id)" \
                  "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
SQL_DROP_BULL = "DROP TABLE IF EXISTS cattle.bull CASCADE"
SQL_CREATE_BULL = "CREATE TABLE cattle.bull(" \
                  "id integer NOT NULL," \
                  "sire_id integer," \
                  "dam_id integer," \
                  "real_dam_id integer," \
                  "breed character varying(20)," \
                  "breeding_type character varying(10)," \
                  "coat_color_dna character varying(10)," \
                  "current_breeding_status character varying(10)," \
                  "dob date," \
                  "ear_tag character varying(50)," \
                  "contact_id integer," \
                  "CONSTRAINT bull_pkey PRIMARY KEY (id))"

SQL_INSERT_CALF = "INSERT INTO cattle.calf(" \
                  "id," \
                  "sire_id," \
                  "dam_id," \
                  "real_dam_id," \
                  "sex," \
                  "breed," \
                  "coat_color_dna," \
                  "dob," \
                  "ear_tag," \
                  "birth_weight," \
                  "weaning_weight," \
                  "yearling_weight," \
                  "adj_birth_weight," \
                  "adj_weaning_weight," \
                  "adj_yearling_weight)" \
                  "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
SQL_DROP_CALF = "DROP TABLE IF EXISTS cattle.calf CASCADE"
SQL_CREATE_CALF = "CREATE TABLE cattle.calf(" \
                  "id integer NOT NULL," \
                  "sire_id integer," \
                  "dam_id integer," \
                  "real_dam_id integer," \
                  "sex character varying(10)," \
                  "breed character varying(20)," \
                  "coat_color_dna character varying(10)," \
                  "dob date," \
                  "ear_tag character varying(50) NOT NULL," \
                  "birth_weight integer," \
                  "weaning_weight integer," \
                  "yearling_weight integer," \
                  "adj_birth_weight integer," \
                  "adj_weaning_weight integer," \
                  "adj_yearling_weight integer," \
                  "CONSTRAINT calf_pkey PRIMARY KEY (id))"

SQL_INSERT_BREED_COMPOSITIONS = "INSERT INTO cattle.breed_compositions(" \
                                "id," \
                                "animal_id," \
                                "breed_id," \
                                "percentage)" \
                                "VALUES (%s,%s,%s,%s)"
SQL_DROP_BREED_COMPOSITIONS = "DROP TABLE IF EXISTS cattle.breed_compositions CASCADE"
SQL_CREATE_BREED_COMPOSITIONS = "CREATE TABLE cattle.breed_compositions(" \
                                "id INTEGER NOT NULL," \
                                "animal_id INTEGER NOT NULL," \
                                "breed_id INTEGER NOT NULL," \
                                "percentage INTEGER," \
                                "CONSTRAINT cattle_breed_compositions_pkey PRIMARY KEY (id))"

SQL_INSERT_BREEDS = "INSERT INTO cattle.breeds(" \
                    "id," \
                    "name," \
                    "gestation_period," \
                    "breed_association_id) " \
                    "VALUES (%s,%s,%s,%s)"
SQL_DROP_BREEDS = "DROP TABLE IF EXISTS cattle.breeds CASCADE"
SQL_CREATE_BREEDS = "CREATE TABLE cattle.breeds(" \
                    "id INTEGER NOT NULL," \
                    "name VARCHAR(30) NOT NULL," \
                    "gestation_period INTEGER NOT NULL," \
                    "breed_association_id INTEGER," \
                    "CONSTRAINT cattle_breeds_pkey PRIMARY KEY (id))"

SQL_INSERT_COW = "INSERT INTO cattle.cow(" \
                 "id," \
                 "active," \
                 "sire_id," \
                 "dam_id," \
                 "real_dam_id," \
                 "breed, " \
                 "breeding_type, " \
                 "coat_color_dna, " \
                 "current_breeding_status," \
                 "dob, " \
                 "ear_tag, " \
                 "estimated_calving_date, " \
                 "last_breeding_date, " \
                 "last_calving_date," \
                 "contact_id)" \
                 "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
SQL_DROP_COW = "DROP TABLE IF EXISTS cattle.cow CASCADE"
SQL_CREATE_COW = "CREATE TABLE cattle.cow(" \
                 "id integer NOT NULL," \
                 "active character varying(20)," \
                 "sire_id integer," \
                 "dam_id integer," \
                 "real_dam_id integer," \
                 "breed character varying(20)," \
                 "breeding_type character varying(10)," \
                 "coat_color_dna character varying(10)," \
                 "current_breeding_status character varying(10)," \
                 "dob date," \
                 "ear_tag character varying(50)," \
                 "estimated_calving_date date," \
                 "last_breeding_date date," \
                 "last_calving_date date," \
                 "contact_id integer," \
                 "CONSTRAINT cow_pkey PRIMARY KEY (id))"

SQL_INSERT_BREEDING = "INSERT INTO cattle.breeding(" \
                      "id," \
                      "animal_id," \
                      "bull_animal_id," \
                      "breeding_method," \
                      "breeding_date," \
                      "breeding_end_date," \
                      "estimated_calving_date," \
                      "cleanup," \
                      "embryo_id," \
                      "pregnancy_check_id)" \
                      "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
SQL_DROP_BREEDING = "DROP TABLE cattle.breeding CASCADE"
SQL_CREATE_BREEDING = "CREATE TABLE cattle.breeding(" \
                      "id integer NOT NULL," \
                      "animal_id integer NOT NULL," \
                      "bull_animal_id integer," \
                      "breeding_method character(2)," \
                      "breeding_date date NOT NULL," \
                      "breeding_end_date date," \
                      "estimated_calving_date date," \
                      "cleanup boolean NOT NULL," \
                      "embryo_id integer," \
                      "pregnancy_check_id integer," \
                      "CONSTRAINT breeding_pkey PRIMARY KEY (id))"

SQL_INSERT_CONTACT = "INSERT INTO cattle.contact(" \
                     "id," \
                     "name)" \
                     "VALUES (%s,%s)"
SQL_DROP_CONTACT = "DROP TABLE IF EXISTS cattle.contact CASCADE"
SQL_CREATE_CONTACT = "CREATE TABLE cattle.contact(" \
                     "id integer NOT NULL," \
                     "name character varying(50) NOT NULL," \
                     "CONSTRAINT cattle_contact_pkey PRIMARY KEY (id))"

SQL_INSERT_PASTURE = "INSERT INTO cattle.pasture(" \
                     "id," \
                     "acres," \
                     "animal_count," \
                     "name)" \
                     "VALUES (%s,%s,%s,%s)"
SQL_DROP_PASTURE = "DROP TABLE IF EXISTS cattle.pasture CASCADE"
SQL_CREATE_PASTURE = "CREATE TABLE cattle.pasture(" \
                     "id integer NOT NULL," \
                     "acres FLOAT," \
                     "animal_count integer NOT NULL," \
                     "name character varying(50) NOT NULL," \
                     "CONSTRAINT cattle_pasture_pkey PRIMARY KEY (id))"

SQL_INSERT_TREATMENTS = "INSERT INTO cattle.treatments(" \
                        "id," \
                        "animal_id," \
                        "treatment_date," \
                        "medication) " \
                        "VALUES (%s,%s,%s,%s)"
SQL_DROP_TREATMENTS = "DROP TABLE IF EXISTS cattle.treatments CASCADE"
SQL_CREATE_TREATMENTS = "CREATE TABLE cattle.treatments(" \
                        "id INTEGER NOT NULL," \
                        "animal_id INTEGER NOT NULL," \
                        "treatment_date DATE," \
                        "medication VARCHAR(30)," \
                        "CONSTRAINT cattle_treatment_pkey PRIMARY KEY (id))"

SQL_SELECT_TREATMENTS = "SELECT " \
                        "t.id," \
                        "t.animal_id," \
                        "t.treatment_date," \
                        "t.medication " \
                        "FROM cattle.treatments AS t, cattle.animal a " \
                        "WHERE a.id = t.animal_id "


class Treatments(object):
    def __init__(self):
        self.id = None
        self.animal_id = None
        self.treatment_date = None
        self.medication = None


SQL_INSERT_EPDS = "INSERT INTO cattle.epds(" \
                  "id," \
                  "animal_id," \
                  "epd_reporting_period," \
                  "epd_type," \
                  "ced_epd," \
                  "ced_acc," \
                  "bw_epd," \
                  "bw_acc," \
                  "ww_epd," \
                  "ww_acc," \
                  "yw_epd," \
                  "yw_acc," \
                  "milk_epd," \
                  "milk_acc, " \
                  "mww_epd," \
                  "mww_acc) " \
                  "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
SQL_DROP_EPDS = "DROP TABLE IF EXISTS cattle.epds CASCADE"
SQL_CREATE_EPDS = "CREATE TABLE cattle.epds(" \
                  "id INTEGER NOT NULL," \
                  "animal_id INTEGER NOT NULL," \
                  "epd_reporting_period VARCHAR(20)," \
                  "epd_type VARCHAR(20)," \
                  "ced_epd FLOAT," \
                  "ced_acc FLOAT," \
                  "bw_epd FLOAT," \
                  "bw_acc FLOAT," \
                  "ww_epd FLOAT," \
                  "ww_acc FLOAT," \
                  "yw_epd FLOAT," \
                  "yw_acc FLOAT," \
                  "milk_epd FLOAT," \
                  "milk_acc FLOAT," \
                  "mww_epd FLOAT," \
                  "mww_acc FLOAT," \
                  "CONSTRAINT cattle_epds_pkey PRIMARY KEY (id))"

SQL_SELECT_EPDS = "SELECT " \
                  "e.id," \
                  "e.animal_id," \
                  "e.epd_reporting_period," \
                  "e.epd_type," \
                  "e.ced_epd," \
                  "e.ced_acc," \
                  "e.bw_epd," \
                  "e.bw_acc," \
                  "e.ww_epd," \
                  "e.ww_acc," \
                  "e.yw_epd," \
                  "e.yw_acc," \
                  "e.milk_epd," \
                  "e.milk_acc," \
                  "e.mww_epd," \
                  "e.mww_acc " \
                  "FROM cattle.epds AS e, cattle.animal a " \
                  "WHERE a.id = e.animal_id "


class EPD(object):
    def __init__(self):
        self.id = None
        self.animal_id = None
        self.epd_reporting_period = None
        self.epd_type = None
        self.ced_epd = None
        self.ced_acc = None
        self.bw_epd = None
        self.bw_acc = None
        self.ww_epd = None
        self.ww_acc = None
        self.yw_epd = None
        self.yw_acc = None
        self.milk_epd = None
        self.milk_acc = None
        self.mww_epd = None
        self.mww_acc = None


SQL_SELECT_ANIMAL = "SELECT " \
                    "a.id," \
                    "a.ear_tag," \
                    "a.ear_tag_loc," \
                    "a.tattoo_left," \
                    "a.tattoo_right," \
                    "a.brand," \
                    "a.brand_loc," \
                    "a.name," \
                    "a.reg_num," \
                    "a.reg_num_2," \
                    "a.other_id," \
                    "a.other_id_loc," \
                    "a.electronic_id," \
                    "a.animal_type," \
                    "a.sex," \
                    "a.breed_id," \
                    "a.horn_status," \
                    "a.color_markings," \
                    "a.ocv_tattoo," \
                    "a.ocv_number," \
                    "a.status," \
                    "a.pasture_id," \
                    "a.sire_animal_id," \
                    "a.dam_animal_id," \
                    "a.genetic_dam_animal_id," \
                    "a.real_dam_animal_id," \
                    "a.sire_legacy_id," \
                    "a.dam_legacy_id," \
                    "a.genetic_dam_legacy_id," \
                    "a.breeder_contact_id," \
                    "a.birth_date," \
                    "a.birth_year," \
                    "a.weaning_date," \
                    "a.yearling_date," \
                    "a.percent_dam_weight," \
                    "a.conception_method," \
                    "a.grafted_calf," \
                    "a.purchase_date," \
                    "a.purchased," \
                    "a.purchased_from_contact_id," \
                    "a.purchase_price," \
                    "a.sale_ticket_id," \
                    "a.sale_price," \
                    "a.sale_weight," \
                    "a.marketing_cost," \
                    "a.reason_for_sale," \
                    "a.death_date," \
                    "a.cause_of_death," \
                    "a.asking_price," \
                    "a.marketing_comments," \
                    "a.mppa," \
                    "a.avg_calving_interval," \
                    "a.avg_post_partum_interval," \
                    "a.last_breeding_date," \
                    "a.last_calving_date," \
                    "a.current_breeding_status," \
                    "a.next_calving_date," \
                    "a.pelvic_area," \
                    "a.pelvic_horizontal," \
                    "a.pelvic_vertical," \
                    "a.comments," \
                    "a.donor_cow," \
                    "a.ai_bull," \
                    "a.promote_date," \
                    "a.demote_date," \
                    "a.birth_weight," \
                    "a.weaning_weight," \
                    "a.yearling_weight," \
                    "a.adj_birth_weight," \
                    "a.adj_weaning_weight," \
                    "a.adj_yearling_weight," \
                    "a.last_tip_to_tip," \
                    "a.last_total_horn," \
                    "a.last_base," \
                    "a.last_composite," \
                    "a.last_horn_measure_date," \
                    "a.last_weight," \
                    "a.last_height," \
                    "a.last_bcs," \
                    "a.last_weight_date," \
                    "a.embryo_recovery_date," \
                    "a.nait_number," \
                    "a.nlis_number," \
                    "a.last_treatment_date," \
                    "a.withdrawal_date," \
                    "a.castration_date," \
                    "a.next_booster_date," \
                    "b.name," \
                    "bc.percentage " \
                    "FROM cattle.animal AS a "


class Animal(object):
    def __init__(self):
        self.animal_id = None
        self.ear_tag = None
        self.reg_num = None
        self.animal_type = None
        self.animal_sex = None
        self.birth_year = None
        self.breed = None
        self.horn_status = None
        self.real_dam_id = None
        self.sire_id = None
        self.name = None
        self.brand = None
        self.eid = None
        self.color_markings = None
        self.breed = None
        self.breed_percentage = None
        self.conception_method = None
        self.breeder = None
        self.dob = None
        self.dow = None
        self.doy = None

    def setDOB(self, _dob):
        self.dob = _dob

    def getDOB(self):
        return self.dob

    def setName(self, _name):
        self.name = _name

    def getName(self):
        return self.name

    def setRealDamId(self, _id):
        self.real_dam_id = _id

    def getRealDamId(self):
        return self.real_dam_id

    def setSireId(self, _id):
        self.sire_id = _id

    def getSireId(self):
        return self.sire_id

    def setAnimalId(self, _id):
        self.animal_id = _id

    def getAnimalId(self):
        return self.animal_id

    def setRegNum(self, _reg_num):
        self.reg_num = _reg_num

    def getRegNum(self):
        return self.reg_num

    def setEarTag(self, _earTag):
        self.ear_tag = _earTag

    def getEarTag(self):
        return self.ear_tag

    def setAnimalType(self, _animalType):
        self.animal_type = _animalType

    def getAnimalType(self):
        return self.animal_type

    def setAnimalSex(self, _animalSex):
        if _animalSex is not None:
            self.animal_sex = _animalSex.upper()
        else:
            self.animal_sex = None

    def getAnimalSex(self):
        return (self.animal_sex)

    def setBirthYear(self, _birthYear):
        self.birth_year = _birthYear

    def getBirthYear(self):
        return self.birth_year

    def setBreed(self, _breed):
        self.breed = _breed

    def getBreed(self):
        return self.breed

    @staticmethod
    def isBornFilter(_birth_year, _year_filter):
        if _birth_year is None:
            return True
        elif _birth_year <= _year_filter:
            return True
        else:
            return False

    @staticmethod
    def isPurchasedFilter(_purchase_year, _year_filter):
        if _purchase_year is None:
            return True
        elif _purchase_year <= _year_filter:
            return True
        else:
            return False

    @staticmethod
    def isDeathFilter(_death_year, _year_filter):
        if _death_year is None:
            return True
        elif _death_year < _year_filter:
            return False
        else:
            return False

    @staticmethod
    def isSoldFilter(_sold_year, _year_filter):
        if _sold_year is None:
            return True
        elif _sold_year < _year_filter:
            return False
        else:
            return True

    def __str__(self):
        return "Instance:\nId: {0}, Ear Tag: {1}, Type: {2}, Sex: {3}, YOB: {4}, Breed:{5}".format(self.animal_id,
                                                                                                   self.ear_tag,
                                                                                                   self.animal_type,
                                                                                                   self.animal_sex,
                                                                                                   self.birth_year,
                                                                                                   self.breed)


class AnimalSold(Animal):
    def __init__(self):
        super(AnimalSold, self).__init__()
        self.year_sold = None
        self.amount = None

    def setYearSold(self, _yearSold):
        self.year_sold = _yearSold

    def getYearSold(self):
        return self.year_sold

    def setSaleAmount(self, _amount):
        self.amount = _amount

    def getSaleAmount(self):
        return self.amount

    def __iter__(self):
        return self

    def __str__(self):
        return "{0}, Year Sold: {1}, Sale Amount: {2}".format(super(AnimalSold, self).__str__(), self.year_sold,
                                                              self.amount)


class AnimalDead(Animal):
    def __init__(self):
        super(AnimalDead, self).__init__()
        self.death_year = None

    def setDeathYear(self, _deathYear):
        self.death_year = _deathYear

    def getDeathYear(self):
        return self.death_year

    def __str__(self):
        return "{0}, Year Died: {1}".format(super(AnimalDead, self).__str__(), self.death_year)


class AnimalPurchased(Animal):
    def __init__(self):
        super(AnimalPurchased, self).__init__()
        self.amount = None
        self.seller_name = None
        self.purchase_year = None

    def setPurchaseAmount(self, _amount):
        self.amount = _amount

    def getPurchasedAmount(self):
        return self.amount

    def setSellerName(self, _name):
        self.seller_name = _name

    def getSellerName(self):
        return self.seller_name

    def setPurchaseYear(self, _year):
        self.purchase_year = _year

    def getPurchaseYear(self):
        return self.purchase_year

    def __str__(self):
        return "{0}, Purchase Amount: {1}, Purchase Year: {2}, Seller: {3}".format(
            super(AnimalPurchased, self).__str__(), self.amount,
            self.purchase_year, self.seller_name)


class AnimalRaised(Animal):
    def __init__(self):
        super(AnimalRaised, self).__init__()

    def __str__(self):
        return "{0}".format(super(AnimalRaised, self).__str__())


def load_breedings(_file_name):
    con = None

    try:
        con = psycopg2.connect(database='rafter',
                               user='postgres',
                               password='postgres',
                               host='localhost')

        cur = con.cursor()
        cur.execute(SQL_DROP_BREEDING)
        con.commit()
        cur.execute(SQL_CREATE_BREEDING)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                id = ec_util.to_pos_int_or_none(row[0])  # A
                animal_id = ec_util.to_pos_int_or_none(row[1])
                bull_animal_id = ec_util.to_pos_int_or_none(row[2])
                breeding_method = ec_util.to_upper_or_none(row[3])
                breeding_date = ec_util.to_date_or_none(row[4])
                breeding_end_date = ec_util.to_date_or_none(row[5])
                estimated_calving_date = ec_util.to_date_or_none(row[7])
                cleanup = ec_util.to_upper_or_none(row[8])
                embryo_id = ec_util.to_pos_int_or_none(row[9])
                pregnancy_check_id = ec_util.to_pos_int_or_none(row[17])

                cur.execute(SQL_INSERT_BREEDING, (
                    id,
                    animal_id,
                    bull_animal_id,
                    breeding_method,
                    breeding_date,
                    breeding_end_date,
                    estimated_calving_date,
                    cleanup,
                    embryo_id,
                    pregnancy_check_id))
                con.commit()

    except psycopg2.DatabaseError as e:
        print('Error %s' % e)
        sys.exit(1)

    else:
        sys.exit(0)

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def load_embryos(_file_name):
    con = None
    SQL_INSERT_EMBRYOS = "INSERT INTO cattle.embryos(" \
                         "id," \
                         "sire_id," \
                         "dam_id)" \
                         "VALUES (%s,%s,%s)"
    SQL_DROP_EMBRYOS = "DROP TABLE IF EXISTS cattle.embryos CASCADE"
    SQL_CREATE_EMBRYOS = "CREATE TABLE cattle.embryos(" \
                         "id integer NOT NULL," \
                         "sire_id integer NOT NULL," \
                         "dam_id integer NOT NULL," \
                         "CONSTRAINT cattle_embryos_pkey PRIMARY KEY (id))"

    try:
        con = ec_psql_util.psql_connection(_host="spowell-home", _database='rafter', _user='postgres', _password='postgres')

        cur = con.cursor()
        cur.execute(SQL_DROP_EMBRYOS)
        con.commit()
        cur.execute(SQL_CREATE_EMBRYOS)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                id = ec_util.to_pos_int_or_none(row[0])  # A
                dam_id = ec_util.to_pos_int_or_none(row[2])  # C
                sire_id = ec_util.to_pos_int_or_none(row[3])  # D
                cur.execute(SQL_INSERT_EMBRYOS, (
                    id,
                    sire_id,
                    dam_id))
                con.commit()

    except Exception as e:
        logging.error("{} {}".format(inspect.stack()[0][3], e))

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def load_movements(_file_name):
    con = None
    SQL_INSERT_MOVEMENTS = "INSERT INTO cattle.movements(" \
                           "id," \
                           "animal_id," \
                           "moved_from_pasture_id," \
                           "moved_to_pasture_id," \
                           "movement_date)" \
                           "VALUES (%s,%s,%s,%s,%s)"
    SQL_DROP_MOVEMENTS = "DROP TABLE IF EXISTS cattle.movements CASCADE"
    SQL_CREATE_MOVEMENTS = "CREATE TABLE cattle.movements(" \
                           "id integer NOT NULL," \
                           "animal_id integer NOT NULL," \
                           "moved_from_pasture_id integer," \
                           "moved_to_pasture_id integer," \
                           "movement_date date," \
                           "CONSTRAINT cattle_movement_pkey PRIMARY KEY (id))"

    try:
        con = ec_psql_util.psql_connection(_host="spowell-home", _database='rafter', _user='postgres', _password='postgres')

        cur = con.cursor()
        cur.execute(SQL_DROP_MOVEMENTS)
        con.commit()
        cur.execute(SQL_CREATE_MOVEMENTS)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                id = ec_util.to_pos_int_or_none(row[0])  # A
                animal_id = ec_util.to_pos_int_or_none(row[1])  # B
                moved_from_pasture_id = ec_util.to_pos_int_or_none(row[2])  # C
                moved_to_pasture_id = ec_util.to_pos_int_or_none(row[3])  # D
                movement_date = ec_util.to_date_or_none(row[4])  # E
                cur.execute(SQL_INSERT_MOVEMENTS, (
                    id,
                    animal_id,
                    moved_from_pasture_id,
                    moved_to_pasture_id,
                    movement_date))
                con.commit()

    except Exception as e:
        logging.error("{} {}".format(inspect.stack()[0][3], e))

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def create_tru_test(_csv_file):
    con = None
    SQL_SELECT = "SELECT " \
                 "a.ear_tag AS VID," \
                 "a.electronic_id AS EID," \
                 "b.name AS BREED," \
                 "a.brand AS LID," \
                 "a.animal_type AS TYPE," \
                 "a.color_markings AS COLOR," \
                 "a.sex as SEX " \
                 "FROM cattle.animal AS a, cattle.breeds AS b " \
                 "WHERE a.breed_id = b.id AND a.status = 'ACTIVE' AND a.ear_tag NOT LIKE '%NEED%'"

    f = None
    try:
        f = open(_csv_file, 'wt')
        writer = csv.writer(f)
        writer.writerow(("VID", "EID", "LID", "Breed", "Sex", "Color"))
        con = ec_psql_util.psql_connection(_database='rafter', _user='postgres', _password='postgres')
        cur = con.cursor()

        cur.execute(SQL_SELECT)
        rows = cur.fetchall()

        for row in rows:
            vid = row[0]
            eid = row[1]
            breed = row[2]
            lid = row[3]
            color = row[5]

            animal_type = row[4]
            sex = row[6]

            if animal_type == 'CALF':
                if sex == 'HEIFER':
                    sex = 'HEIFER CALF'
                elif sex == "STEER":
                    sex = 'STEER CALF'
                elif sex == 'BULL':
                    sex = 'BULL CALF'
                else:
                    sex = 'UNKNOWN CALF'
            else:
                sex = animal_type

            writer.writerow((vid, eid, sex, breed, lid, color))

    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()
        f.close()


def create_allflex(_csv_file):
    con = None
    delim = '-'
    SQL_SELECT = "SELECT " \
                 "a.ear_tag AS VID," \
                 "a.electronic_id AS EID," \
                 "b.name AS BREED," \
                 "a.brand AS LID," \
                 "a.animal_type AS TYPE," \
                 "a.color_markings AS COLOR," \
                 "a.sex as SEX, " \
                 "a.horn_status AS HORN_STATUS " \
                 "FROM cattle.animal AS a, cattle.breeds AS b " \
                 "WHERE a.breed_id = b.id AND a.status = 'ACTIVE' AND a.electronic_id IS NOT NULL AND a.ear_tag NOT LIKE '%NEED%'"

    f = None
    try:
        f = open(_csv_file, 'wt')
        writer = csv.writer(f)
        writer.writerow(("EID", "VID", "LID", "OTH"))
        con = ec_psql_util.psql_connection(_database='rafter', _user='postgres', _password='postgres')
        cur = con.cursor()

        cur.execute(SQL_SELECT)
        rows = cur.fetchall()

        for row in rows:
            vid = row[0]
            eid = row[1]
            brd = row[2]
            lid = row[3]
            col = row[5]
            hst = row[7]

            # build composite field
            oth = None
            if abv_breed(brd) is not None:
                oth = abv_breed(brd)
            if abv_color(col) is not None:
                if oth is not None:
                    oth = oth + delim + abv_color(col)
                else:
                    oth = abv_color(col)
            if abv_horned(hst) is not None:
                if oth is not None:
                    oth = oth + delim + abv_horned(hst)
                else:
                    oth = abv_horned(hst)

            writer.writerow((eid, vid, lid, oth))

    except psycopg2.DatabaseError as e:
        print('Error %s' % e)
        sys.exit(1)

    finally:
        if con:
            con.close()
        f.close()


def abv_color(_color):
    color = None

    if _color is None:
        return color

    _color = _color.upper()
    if _color == "BLACK W/MOTTLED WHITE FACE":
        color = "BWF"
    elif _color == "BLACK W/WHITE MOTTLED THROAT":
        color = "BWF"
    elif _color == "BRINDLE":
        color = "BRD"
    elif _color == "DARK GREY":
        color = "DG"
    elif _color == "LIGHT RED":
        color = "LR"
    elif _color == "RED & WHITE FACE":
        color = "RWF"
    elif _color == "RED W/MOTTLED FACE":
        color = "RMF"
    elif _color == "SOLID BLACK":
        color = "B"
    elif _color == "SOLID BLACK W/WHITE FACE":
        color = "BWF"
    elif _color == "SOLID GREY":
        color = "G"
    elif _color == "SOLID RED":
        color = "R"
    elif _color == "SOLID WHITE":
        color = "W"
    elif _color == "SOLID YELLOW":
        color = "Y"
    return color


def abv_breed(_breed):
    breed = None

    if _breed is None:
        return breed

    _breed = _breed.upper()
    if _breed == "ANGUS":
        breed = "AS"
    elif _breed == "BRAHMAN":
        breed = "BN"
    elif _breed == "BRANGUS":
        breed = "BS"
    elif _breed == "BRANGUS F1":
        breed = "B1"
    elif _breed == "CHAROLAIS":
        breed = "CH"
    elif _breed == "COMMERCIAL":
        breed = "CM"
    elif _breed == "HEREFORD":
        breed = "HD"
    return breed


def abv_horned(_horned):
    horned = None

    if _horned is None:
        return horned

    _horned = _horned.upper()
    if _horned == "HORNED":
        horned = "H"
    elif _horned == "POLLED":
        horned = "P"
    elif _horned == "SCURRED":
        horned = "S"
    elif _horned == "DEHORNED":
        horned = "B1"
    elif _horned == "CHAROLAIS":
        horned = "D"
    return horned


def load_contacts(_file_name):
    con = None

    try:
        con = ec_psql_util.psql_connection(_host="spowell-home", _database='rafter', _user='postgres', _password='postgres')

        cur = con.cursor()
        cur.execute(SQL_DROP_CONTACT)
        con.commit()
        cur.execute(SQL_CREATE_CONTACT)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                contact_id = ec_util.to_pos_int_or_none(row[0])  # A
                name = ec_util.to_upper_or_none(row[8])

                cur.execute(SQL_INSERT_CONTACT, (
                    contact_id,
                    name))
                con.commit()

    except Exception as e:
        logging.error("{} {}".format(inspect.stack()[0][3], e))

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def load_pastures(_file_name):
    con = None

    try:
        con = ec_psql_util.psql_connection(_host="spowell-home", _database='rafter', _user='postgres', _password='postgres')

        cur = con.cursor()
        cur.execute(SQL_DROP_PASTURE)
        con.commit()
        cur.execute(SQL_CREATE_PASTURE)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                id = ec_util.to_pos_int_or_none(row[0])  # A
                name = ec_util.to_upper_or_none(row[3])  # D
                acres = ec_util.to_float_or_none(row[5])  # F
                animal_count = ec_util.to_pos_int_or_none(row[7])  # H

                cur.execute(SQL_INSERT_PASTURE, (
                    id,
                    acres,
                    animal_count,
                    name))
                con.commit()

    except Exception as e:
        logging.error("{} {}".format(inspect.stack()[0][3], e))

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def load_epds(_file_name):
    con = None

    try:
        con = ec_psql_util.psql_connection(_database='rafter', _user='postgres', _password='postgres')

        cur = con.cursor()
        cur.execute(SQL_DROP_EPDS)
        con.commit()
        cur.execute(SQL_CREATE_EPDS)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                id = ec_util.to_pos_int_or_none(row[0])  # A
                animal_id = ec_util.to_pos_int_or_none(row[1])  # B
                epd_reporting_period = ec_util.to_upper_or_none(row[2])  # C
                epd_type = ec_util.to_upper_or_none((row[3]))  # D
                ced_epd = ec_util.to_float_or_none(row[4])  # E
                ced_acc = ec_util.to_float_or_none(row[33])  # AH
                bw_epd = ec_util.to_float_or_none(row[5])  # F
                bw_acc = ec_util.to_float_or_none(row[34])  # AI
                ww_epd = ec_util.to_float_or_none(row[6])  # G
                ww_acc = ec_util.to_float_or_none(row[35])  # AJ
                yw_epd = ec_util.to_float_or_none(row[7])  # H
                yw_acc = ec_util.to_float_or_none(row[36])  # AK
                milk_epd = ec_util.to_float_or_none(row[10])  # K
                milk_acc = ec_util.to_float_or_none(row[39])  # AN
                mww_epd = ec_util.to_float_or_none(row[12])  # K
                mww_acc = ec_util.to_float_or_none(row[41])  # AN

                cur.execute(SQL_INSERT_EPDS, (
                    id,
                    animal_id,
                    epd_reporting_period,
                    epd_type,
                    ced_epd,
                    ced_acc,
                    bw_epd,
                    bw_acc,
                    ww_epd,
                    ww_acc,
                    yw_epd,
                    yw_acc,
                    milk_epd,
                    milk_acc,
                    mww_epd,
                    mww_acc))
                con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def load_treatments(_file_name):
    con = None

    try:
        con = ec_psql_util.psql_connection(_database='rafter', _user='postgres', _password='postgres')

        cur = con.cursor()
        cur.execute(SQL_DROP_TREATMENTS)
        con.commit()
        cur.execute(SQL_CREATE_TREATMENTS)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                id = ec_util.to_pos_int_or_none(row[0])  # A
                animal_id = ec_util.to_pos_int_or_none(row[1])  # B
                treatment_date = ec_util.to_date_or_none(row[2])  # C
                medication = ec_util.to_upper_or_none((row[6]))  # D

                cur.execute(SQL_INSERT_TREATMENTS, (
                    id,
                    animal_id,
                    treatment_date,
                    medication))
                con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def load_measurements(_file_name):
    con = None
    SQL_INSERT_MEASUREMENTS = "INSERT INTO cattle.measurements(" \
                              "id," \
                              "animal_id," \
                              "category," \
                              "measure_date," \
                              "age_at_measure," \
                              "weight," \
                              "adjusted_weight," \
                              "adg," \
                              "wda," \
                              "reference_weight," \
                              "gain," \
                              "scrotal)" \
                              "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    SQL_DROP_MEASUREMENTS = "DROP TABLE IF EXISTS cattle.measurements CASCADE"
    SQL_CREATE_MEASUREMENTS = "CREATE TABLE cattle.measurements(" \
                              "id INTEGER NOT NULL," \
                              "animal_id INTEGER NOT NULL," \
                              "category VARCHAR(20)," \
                              "measure_date DATE," \
                              "age_at_measure INTEGER," \
                              "weight INTEGER," \
                              "adjusted_weight INTEGER," \
                              "adg FLOAT," \
                              "wda FLOAT," \
                              "reference_weight INTEGER," \
                              "gain INTEGER," \
                              "scrotal FLOAT," \
                              "CONSTRAINT cattle_measurements_pkey PRIMARY KEY (id))"

    try:
        con = ec_psql_util.psql_connection(_database='rafter', _user='postgres', _password='postgres')

        cur = con.cursor()
        cur.execute(SQL_DROP_MEASUREMENTS)
        con.commit()
        cur.execute(SQL_CREATE_MEASUREMENTS)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                id = ec_util.to_pos_int_or_none(row[0])  # A
                animal_id = ec_util.to_pos_int_or_none(row[1])  # B
                category = ec_util.to_upper_or_none(row[2])  # C
                measure_date = ec_util.to_date_or_none((row[3]))  # D
                age_at_measure = ec_util.to_pos_int_or_none(row[4])  # E
                weight = ec_util.to_pos_int_or_none(row[5])  # F
                adjusted_weight = ec_util.to_pos_int_or_none(row[6])  # G
                adg = ec_util.to_float_or_none(row[8])  # I
                wda = ec_util.to_float_or_none(row[10])  # K
                reference_weight = ec_util.to_pos_int_or_none(row[19])  # T
                gain = ec_util.to_pos_int_or_none(row[20])  # U
                scrotal = ec_util.to_float_or_none(row[16])  # Q

                cur.execute(SQL_INSERT_MEASUREMENTS, (
                    id,
                    animal_id,
                    category,
                    measure_date,
                    age_at_measure,
                    weight,
                    adjusted_weight,
                    adg,
                    wda,
                    reference_weight,
                    gain,
                    scrotal))
                con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def load_breed_compositions(_file_name):
    con = None

    try:
        con = ec_psql_util.psql_connection(_host="spowell-home", _database='rafter', _user='postgres', _password='postgres')
        cur = con.cursor()
        cur.execute(SQL_DROP_BREED_COMPOSITIONS)
        con.commit()
        cur.execute(SQL_CREATE_BREED_COMPOSITIONS)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                id = ec_util.to_pos_int_or_none(row[0])  # A
                animal_id = ec_util.to_pos_int_or_none(row[1])  # B
                breed_id = ec_util.to_pos_int_or_none(row[2])  # C
                percentage = ec_util.to_float_or_none(row[3])  # D
                cur.execute(SQL_INSERT_BREED_COMPOSITIONS, (
                    id,
                    animal_id,
                    breed_id,
                    percentage))

            con.commit()

    except Exception as e:
        logging.error("{} {}".format(inspect.stack()[0][3], e))

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def load_breeds(_file_name):
    con = None
    csvfile = None
    try:
        con = ec_psql_util.psql_connection(_host="spowell-home", _database='rafter', _user='postgres', _password='postgres')
        cur = con.cursor()
        cur.execute(SQL_DROP_BREEDS)
        con.commit()
        cur.execute(SQL_CREATE_BREEDS)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                id = ec_util.to_pos_int_or_none(row[0])  # A
                name = ec_util.to_upper_or_none(row[2])
                gestation_period = ec_util.to_pos_int_or_none(row[3])
                breed_association_id = ec_util.to_pos_int_or_none(row[5])
                cur.execute(SQL_INSERT_BREEDS, (
                    id,
                    name,
                    gestation_period,
                    breed_association_id))

            con.commit()

    except Exception as e:
        logging.error("{} {}".format(inspect.stack()[0][3], e))

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def load_calves(_file_name):
    con = None
    csvfile = None

    try:
        # breeding_forms = load_animal_custom_fields("BREEDING FORM")
        color_DNAs = load_animal_custom_fields("COAT COLOR DNA")
        breeds = load_breeds_map()
        con = ec_psql_util.psql_connection(_database='rafter', _user='postgres', _password='postgres')

        cur = con.cursor()
        cur.execute(SQL_DROP_CALF)
        con.commit()
        cur.execute(SQL_CREATE_CALF)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                active = ec_util.to_upper_or_none(row[25])  # Z
                if active != "ACTIVE":
                    continue
                animal_type = ec_util.to_upper_or_none(row[17])
                if animal_type != "CALF":
                    continue

                id = ec_util.to_pos_int_or_none(row[0])  # A
                sire_id = ec_util.to_pos_int_or_none(row[27])  # AB
                dam_id = ec_util.to_pos_int_or_none(row[28])  # AC
                real_dam_id = ec_util.to_pos_int_or_none(row[30])  # AE
                # breed
                breed_key = ec_util.to_pos_int_or_none(row[20])  # U
                breed = ec_hashmap.get(breeds, breed_key)

                coat_color_dna = ec_hashmap.get(color_DNAs, id)
                if coat_color_dna:
                    if "ED/ED" in coat_color_dna:
                        coat_color_dna = "ED/ED"
                    elif "ED/E" in coat_color_dna:
                        coat_color_dna = "ED/E"
                    elif "NOT TESTED" in coat_color_dna:
                        coat_color_dna = "NOT TESTED"
                    else:
                        coat_color_dna = None

                ear_tag = ec_util.to_upper_or_none(row[3])  # D
                if ear_tag is None:
                    continue
                sex = ec_util.to_upper_or_none(row[18])
                dob_date = ec_util.to_upper_or_none(row[35])
                birth_weight = ec_util.to_pos_int_or_none(row[69])
                weaning_weight = ec_util.to_pos_int_or_none(row[70])
                yearling_weight = ec_util.to_pos_int_or_none(row[71])
                adj_birth_weight = ec_util.to_pos_int_or_none(row[72])
                adj_weaning_weight = ec_util.to_pos_int_or_none(row[73])
                adj_yearling_weight = ec_util.to_pos_int_or_none(row[74])

                # seller_id = ec_util.to_pos_int_or_none(row[43])  # AR
                # current_breeding_status = ec_util.to_upper_or_none(row[59])  # BH
                # last_calving_date = ec_util.to_date_or_none(row[58])
                # estimated_calving_date = ec_util.to_date_or_none(row[60])
                # last_breeding_date = ec_util.to_date_or_none(row[58])
                # contact_id = ec_util.to_pos_int_or_none(row[43])  # AR
                # dob = ec_util.to_date_or_none(row[35])

                cur.execute(SQL_INSERT_CALF, (
                    id,
                    sire_id,
                    dam_id,
                    real_dam_id,
                    sex,
                    breed,
                    coat_color_dna,
                    dob_date,
                    ear_tag,
                    birth_weight,
                    weaning_weight,
                    yearling_weight,
                    adj_birth_weight,
                    adj_weaning_weight,
                    adj_yearling_weight))
                con.commit()

    except psycopg2.DatabaseError as e:
        print('Error %s' % e)
        sys.exit(1)

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def load_cows(_file_name):
    con = None
    csvfile = None

    try:
        # breedings = cattle.load_breedings()
        breeding_forms = load_animal_custom_fields("BREEDING FORM")
        color_DNAs = load_animal_custom_fields("COAT COLOR DNA")
        breeds = load_breeds_map()

        con = ec_psql_util.psql_connection(_database='rafter', _user='postgres', _password='postgres')

        cur = con.cursor()
        cur.execute(SQL_DROP_COW)
        con.commit()
        cur.execute(SQL_CREATE_COW)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                active = ec_util.to_upper_or_none(row[25])  # Z
                if active != "ACTIVE" and active != "REFERENCE":
                    continue
                id = ec_util.to_pos_int_or_none(row[0])  # A
                sire_id = ec_util.to_pos_int_or_none(row[27])  # AB
                dam_id = ec_util.to_pos_int_or_none(row[28])  # AC
                real_dam_id = ec_util.to_pos_int_or_none(row[30])  # AE
                # breed
                breed_key = ec_util.to_pos_int_or_none(row[20])  # U
                breed = ec_hashmap.get(breeds, breed_key)
                breeding_type = ec_hashmap.get(breeding_forms, id)
                if breeding_type:
                    if "FLUSH" in breeding_type:
                        breeding_type = "FLUSH"
                    elif "AI" in breeding_type:
                        breeding_type = "AI"
                    elif "NS" in breeding_type:
                        breeding_type = "NS"
                    elif "RECIPIENT" in breeding_type:
                        breeding_type = "RECIPIENT"
                    else:
                        breeding_type = None

                coat_color_dna = ec_hashmap.get(color_DNAs, id)
                if coat_color_dna:
                    if "ED/ED" in coat_color_dna:
                        coat_color_dna = "ED/ED"
                    elif "ED/E" in coat_color_dna:
                        coat_color_dna = "ED/E"
                    elif "NOT TESTED" in coat_color_dna:
                        coat_color_dna = "NOT TESTED"
                    else:
                        coat_color_dna = None

                ear_tag = ec_util.to_upper_or_none(row[3])  # D
                if ear_tag is None:
                    continue
                animal_type = ec_util.to_upper_or_none(row[17])
                sex = ec_util.to_upper_or_none(row[18])
                dob_date = ec_util.to_upper_or_none(row[35])
                dob_year = None
                if dob_date:
                    dob_year = dob_date.split("-")[0]

                seller_id = ec_util.to_pos_int_or_none(row[43])  # AR
                current_breeding_status = ec_util.to_upper_or_none(row[59])  # BH
                last_calving_date = ec_util.to_date_or_none(row[58])  # BG
                estimated_calving_date = ec_util.to_date_or_none(row[60])  # BI
                last_breeding_date = ec_util.to_date_or_none(row[58])
                contact_id = ec_util.to_pos_int_or_none(row[43])  # AR
                dob = ec_util.to_date_or_none(row[35])

                cur.execute(SQL_INSERT_COW, (
                    id,
                    active,
                    sire_id,
                    dam_id,
                    real_dam_id,
                    breed,
                    breeding_type,
                    coat_color_dna,
                    current_breeding_status,
                    dob,
                    ear_tag,
                    estimated_calving_date,
                    last_breeding_date,
                    last_calving_date,
                    contact_id))
                con.commit()

    except psycopg2.DatabaseError as e:
        print('Error %s' % e)
        sys.exit(1)

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def load_animals(_file_name):
    con = None
    SQL_INSERT_ANIMAL = "INSERT INTO cattle.animal(" \
                        "id," \
                        "ear_tag," \
                        "ear_tag_prefix," \
                        "ear_tag_year_desig," \
                        "ear_tag_color," \
                        "ear_tag_loc," \
                        "tattoo_left," \
                        "tattoo_right," \
                        "brand," \
                        "brand_loc," \
                        "name," \
                        "reg_num," \
                        "reg_num_2," \
                        "other_id," \
                        "other_id_loc," \
                        "electronic_id," \
                        "animal_type," \
                        "sex," \
                        "breed_id," \
                        "horn_status," \
                        "color_markings," \
                        "ocv_tattoo," \
                        "ocv_number," \
                        "status," \
                        "pasture_id," \
                        "sire_animal_id," \
                        "dam_animal_id," \
                        "genetic_dam_animal_id," \
                        "real_dam_animal_id," \
                        "sire_legacy_id," \
                        "dam_legacy_id," \
                        "genetic_dam_legacy_id," \
                        "breeder_contact_id," \
                        "birth_date," \
                        "birth_year," \
                        "weaning_date," \
                        "yearling_date," \
                        "percent_dam_weight," \
                        "conception_method," \
                        "grafted_calf," \
                        "purchase_date," \
                        "purchased," \
                        "purchased_from_contact_id," \
                        "purchase_price," \
                        "sale_ticket_id," \
                        "sale_price," \
                        "sale_weight," \
                        "marketing_cost," \
                        "reason_for_sale," \
                        "death_date," \
                        "cause_of_death," \
                        "asking_price," \
                        "marketing_comments," \
                        "mppa," \
                        "avg_calving_interval," \
                        "avg_post_partum_interval," \
                        "last_breeding_date," \
                        "last_calving_date," \
                        "current_breeding_status," \
                        "next_calving_date," \
                        "pelvic_area," \
                        "pelvic_horizontal," \
                        "pelvic_vertical," \
                        "comments," \
                        "donor_cow," \
                        "ai_bull," \
                        "promote_date," \
                        "demote_date," \
                        "birth_weight," \
                        "weaning_weight," \
                        "yearling_weight," \
                        "adj_birth_weight," \
                        "adj_weaning_weight," \
                        "adj_yearling_weight," \
                        "last_tip_to_tip," \
                        "last_total_horn," \
                        "last_base," \
                        "last_composite," \
                        "last_horn_measure_date," \
                        "last_weight," \
                        "last_height," \
                        "last_bcs," \
                        "last_weight_date," \
                        "embryo_recovery_date," \
                        "nait_number," \
                        "nlis_number," \
                        "last_treatment_date," \
                        "withdrawal_date," \
                        "castration_date," \
                        "next_booster_date) " \
                        "VALUES (" \
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s," \
                        "%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"

    SQL_DROP_ANIMAL = "DROP TABLE IF EXISTS cattle.animal CASCADE"
    SQL_CREATE_ANIMAL = "CREATE TABLE cattle.animal(" \
                        "id INTEGER NOT NULL," \
                        "ear_tag VARCHAR(50)," \
                        "ear_tag_prefix INTEGER," \
                        "ear_tag_year_desig VARCHAR(10)," \
                        "ear_tag_color VARCHAR(50)," \
                        "ear_tag_loc VARCHAR(2)," \
                        "tattoo_left VARCHAR(25)," \
                        "tattoo_right VARCHAR(25)," \
                        "brand VARCHAR(25)," \
                        "brand_loc VARCHAR(2)," \
                        "name VARCHAR(50)," \
                        "reg_num VARCHAR(25)," \
                        "reg_num_2 VARCHAR(25)," \
                        "other_id VARCHAR(25)," \
                        "other_id_loc VARCHAR(25)," \
                        "electronic_id VARCHAR(25)," \
                        "animal_type VARCHAR(10) NOT NULL," \
                        "sex VARCHAR(6)," \
                        "breed_id INTEGER," \
                        "horn_status VARCHAR(12)," \
                        "color_markings VARCHAR(50)," \
                        "ocv_tattoo VARCHAR(25)," \
                        "ocv_number VARCHAR(25)," \
                        "status VARCHAR(9)," \
                        "pasture_id INTEGER," \
                        "sire_animal_id INTEGER," \
                        "dam_animal_id INTEGER," \
                        "genetic_dam_animal_id INTEGER," \
                        "real_dam_animal_id INTEGER," \
                        "sire_legacy_id INTEGER," \
                        "dam_legacy_id INTEGER," \
                        "genetic_dam_legacy_id INTEGER," \
                        "breeder_contact_id INTEGER," \
                        "birth_date DATE," \
                        "birth_year INTEGER," \
                        "weaning_date DATE," \
                        "yearling_date DATE," \
                        "percent_dam_weight REAL," \
                        "conception_method VARCHAR(2)," \
                        "grafted_calf BOOLEAN," \
                        "purchase_date DATE," \
                        "purchased BOOLEAN," \
                        "purchased_from_contact_id INTEGER," \
                        "purchase_price REAL," \
                        "sale_ticket_id INTEGER," \
                        "sale_price REAL," \
                        "sale_weight REAL," \
                        "marketing_cost REAL," \
                        "reason_for_sale VARCHAR(100)," \
                        "death_date DATE," \
                        "cause_of_death VARCHAR(50)," \
                        "asking_price REAL," \
                        "marketing_comments VARCHAR(100)," \
                        "mppa REAL," \
                        "avg_calving_interval INTEGER," \
                        "avg_post_partum_interval INTEGER," \
                        "last_breeding_date DATE," \
                        "last_calving_date DATE," \
                        "current_breeding_status VARCHAR(20)," \
                        "next_calving_date DATE," \
                        "pelvic_area REAL," \
                        "pelvic_horizontal REAL," \
                        "pelvic_vertical REAL," \
                        "comments VARCHAR(100)," \
                        "donor_cow BOOLEAN," \
                        "ai_bull BOOLEAN," \
                        "promote_date DATE," \
                        "demote_date DATE," \
                        "birth_weight REAL," \
                        "weaning_weight REAL," \
                        "yearling_weight REAL," \
                        "adj_birth_weight REAL," \
                        "adj_weaning_weight REAL," \
                        "adj_yearling_weight REAL," \
                        "last_tip_to_tip REAL," \
                        "last_total_horn REAL," \
                        "last_base REAL," \
                        "last_composite REAL," \
                        "last_horn_measure_date REAL," \
                        "last_weight INTEGER," \
                        "last_height INTEGER," \
                        "last_bcs INTEGER," \
                        "last_weight_date DATE," \
                        "embryo_recovery_date DATE," \
                        "nait_number INTEGER," \
                        "nlis_number INTEGER," \
                        "last_treatment_date DATE," \
                        "withdrawal_date DATE," \
                        "castration_date DATE," \
                        "next_booster_date DATE," \
                        "CONSTRAINT animal_pkey PRIMARY KEY (id))"

    row = None
    csvfile = None

    try:
        # breedings = cattle.load_breedings()
        breeding_forms = load_animal_custom_fields("BREEDING FORM")
        color_DNAs = load_animal_custom_fields("COAT COLOR DNA")
        breeds = load_breeds_map()

        tag_year_colors = ['WHITE', 'BLUE', 'ORANGE', 'YELLOW', 'RED', 'PURPLE', 'CAN']

        con = ec_psql_util.psql_connection(_host="spowell-home", _database='rafter', _user='postgres', _password='postgres')

        cur = con.cursor()
        cur.execute(SQL_DROP_ANIMAL)
        con.commit()
        cur.execute(SQL_CREATE_ANIMAL)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                active = ec_util.to_upper_or_none(row[25])  # Z
                # if active != "ACTIVE" and active != "REFERENCE":
                #     continue
                id = ec_util.to_pos_int_or_none(row[0])  # A
                ear_tag = ec_util.to_upper_or_none(row[3])  # D

                if ear_tag is not None:
                    tag_number = ''
                    number_idx = 0
                    ear_tag_search = ear_tag
                    for i, c in enumerate(ear_tag_search):
                        if i == 0 and c.isdigit():
                            tag_number += c
                        elif c.isdigit() is False:
                            number_idx = i
                            break
                        elif i > 0 and tag_number is not None:
                            tag_number += c

                    if tag_number != '':
                        tag_number = int(tag_number)
                        tag_year = ''
                        year_idx = 0
                        tag_color = ''

                        ear_tag_search = ear_tag[number_idx:]
                        for i, c in enumerate(ear_tag_search):
                            if c == '-':
                                tag_color = ear_tag_search[i + 1:]
                                break
                            else:
                                tag_year += c

                        found_color = False
                        for color in tag_year_colors:
                            if tag_color == color:
                                found_color = True
                                break

                        if not found_color:
                            if not tag_color.__contains__('-'):
                                tag_color = ear_tag
                                tag_year = None
                                tag_number = None

                    else:
                        tag_number = None
                        tag_year = None
                        tag_color = ear_tag
                else:
                    tag_number = None
                    tag_year = None
                    tag_color = None

                ear_tag_loc = ec_util.to_upper_or_none(row[4])  # E
                tattoo_left = ec_util.to_upper_or_none(row[5])  # F
                tattoo_right = ec_util.to_upper_or_none(row[6])  # G
                brand = ec_util.to_upper_or_none(row[7])  # H
                brand_loc = ec_util.to_upper_or_none(row[8])  # I
                name = ec_util.to_upper_or_none(row[9])  # J
                reg_num = ec_util.to_upper_or_none(row[12])  # M
                reg_num_2 = ec_util.to_upper_or_none(row[13])  # N
                other_id = ec_util.to_upper_or_none((row[14]))  # O
                other_id_loc = ec_util.to_upper_or_none(row[15])  # P
                electronic_id = ec_util.to_upper_or_none(row[16])  # Q
                animal_type = ec_util.to_upper_or_none(row[17])  # R
                sex = ec_util.to_upper_or_none(row[18])  # S
                breed_id = ec_util.to_pos_long_or_none(row[20])  # U
                horn_status = ec_util.to_upper_or_none(row[21])  # V
                color_markings = ec_util.to_upper_or_none(row[22])  # W
                ocv_tattoo = ec_util.to_upper_or_none(row[23])  # X
                ocv_number = ec_util.to_upper_or_none(row[24])  # Y
                status = ec_util.to_upper_or_none(row[25])  # Z
                pasture_id = ec_util.to_pos_long_or_none(row[26])  # AA
                sire_animal_id = ec_util.to_pos_long_or_none(row[27])  # AB
                dam_animal_id = ec_util.to_pos_long_or_none(row[28])  # AC
                genetic_dam_animal_id = ec_util.to_pos_long_or_none(row[29])  # AD
                real_dam_animal_id = ec_util.to_pos_long_or_none(row[30])  # AE
                sire_legacy_id = ec_util.to_pos_long_or_none(row[31])  # AF
                dam_legacy_id = ec_util.to_pos_long_or_none(row[32])  # AG
                genetic_dam_legacy_id = ec_util.to_pos_long_or_none(row[33])  # AH
                breeder_contact_id = ec_util.to_pos_long_or_none(row[34])  # AI
                birth_date = ec_util.to_date_or_none(row[35])  # AJ
                birth_year = None
                if birth_date:
                    birth_year = birth_date.year

                weaning_date = ec_util.to_date_or_none(row[36])  # AK
                yearling_date = ec_util.to_date_or_none(row[37])  # AL
                percent_dam_weight = ec_util.to_float_or_none(row[38])  # AM
                conception_method = ec_util.to_upper_or_none(row[39])  # AN
                grafted_calf = ec_util.to_boolean_or_none(row[40])  # AO
                purchase_date = ec_util.to_date_or_none(row[41])  # AP
                purchased = ec_util.to_boolean_or_none(row[42])  # AQ
                purchased_from_contact_id = ec_util.to_pos_long_or_none(row[43])  # AR
                purchase_price = ec_util.to_float_or_none(row[44])  # AS
                sale_ticket_id = ec_util.to_pos_long_or_none(row[45])  # AT
                sale_price = ec_util.to_float_or_none(row[46])  # AU
                sale_weight = ec_util.to_float_or_none(row[47])  # AV
                marketing_cost = ec_util.to_float_or_none(row[48])  # AX
                reason_for_sale = ec_util.to_upper_or_none(row[49])  # AY
                death_date = ec_util.to_date_or_none(row[50])  # AZ
                cause_of_death = ec_util.to_upper_or_none(row[51])  # AV
                asking_price = ec_util.to_float_or_none(row[52])  # BA
                marketing_comments = ec_util.to_upper_or_none(row[53])  # BB
                mppa = ec_util.to_float_or_none(row[54])  # BC
                avg_calving_interval = ec_util.to_pos_int_or_none(row[55])  # BD
                avg_post_partum_interval = ec_util.to_pos_int_or_none(row[56])  # BE
                last_breeding_date = ec_util.to_date_or_none(row[57])  # BF
                last_calving_date = ec_util.to_date_or_none(row[58])  # BG
                current_breeding_status = ec_util.to_upper_or_none(row[59])  # BH
                next_calving_date = ec_util.to_date_or_none(row[60])  # BI
                pelvic_area = ec_util.to_float_or_none(row[61])  # BJ
                pelvic_horizontal = ec_util.to_float_or_none(row[62])  # BK
                pelvic_vertical = ec_util.to_float_or_none(row[63])  # BL
                comments = ec_util.to_upper_or_none(row[64])  # BM
                donor_cow = ec_util.to_boolean_or_none(row[65])  # BN
                ai_bull = ec_util.to_boolean_or_none(row[66])  # BO
                promote_date = ec_util.to_date_or_none(row[67])  # BP
                demote_date = ec_util.to_date_or_none(row[68])  # BQ
                birth_weight = ec_util.to_pos_int_or_none(row[69])  # BR
                weaning_weight = ec_util.to_pos_int_or_none(row[70])  # BS
                yearling_weight = ec_util.to_pos_int_or_none(row[71])  # BT
                adj_birth_weight = ec_util.to_pos_int_or_none(row[72])  # BU
                adj_weaning_weight = ec_util.to_pos_int_or_none(row[73])  # BV
                adj_yearling_weight = ec_util.to_pos_int_or_none(row[74])  # BW
                last_tip_to_tip = ec_util.to_pos_int_or_none(row[75])  # BX
                last_total_horn = ec_util.to_pos_int_or_none(row[76])  # BY
                last_base = ec_util.to_pos_int_or_none(row[77])  # BZ
                last_composite = ec_util.to_pos_int_or_none(row[78])  # CA
                last_horn_measure_date = ec_util.to_date_or_none(row[79])  # CB
                last_weight = ec_util.to_pos_int_or_none(row[80])  # CC
                last_height = ec_util.to_pos_int_or_none(row[81])  # CD
                last_bcs = ec_util.to_pos_int_or_none(row[82])  # CE
                last_weight_date = ec_util.to_date_or_none(row[83])  # CF
                embryo_recovery_date = ec_util.to_date_or_none(row[86])  # CI
                nait_number = ec_util.to_pos_int_or_none(row[87])  # CJ
                nlis_number = ec_util.to_pos_int_or_none(row[88])  # CK
                last_treatment_date = ec_util.to_date_or_none(row[89])  # CL
                withdrawal_date = ec_util.to_date_or_none(row[90])  # CM
                castration_date = ec_util.to_date_or_none(row[91])  # CN
                next_booster_date = ec_util.to_date_or_none(row[92])  # CO
                cur.execute(SQL_INSERT_ANIMAL, (
                    id,
                    ear_tag,

                    tag_number,
                    tag_year,
                    tag_color,

                    ear_tag_loc,
                    tattoo_left,
                    tattoo_right,
                    brand,
                    brand_loc,
                    name,
                    reg_num,
                    reg_num_2,
                    other_id,
                    other_id_loc,
                    electronic_id,
                    animal_type,
                    sex,
                    breed_id,
                    horn_status,
                    color_markings,
                    ocv_tattoo,
                    ocv_number,
                    status,
                    pasture_id,
                    sire_animal_id,
                    dam_animal_id,
                    genetic_dam_animal_id,
                    real_dam_animal_id,
                    sire_legacy_id,
                    dam_legacy_id,
                    genetic_dam_legacy_id,
                    breeder_contact_id,
                    birth_date,
                    birth_year,
                    weaning_date,
                    yearling_date,
                    percent_dam_weight,
                    conception_method,
                    grafted_calf,
                    purchase_date,
                    purchased,
                    purchased_from_contact_id,
                    purchase_price,
                    sale_ticket_id,
                    sale_price,
                    sale_weight,
                    marketing_cost,
                    reason_for_sale,
                    death_date,
                    cause_of_death,
                    asking_price,
                    marketing_comments,
                    mppa,
                    avg_calving_interval,
                    avg_post_partum_interval,
                    last_breeding_date,
                    last_calving_date,
                    current_breeding_status,
                    next_calving_date,
                    pelvic_area,
                    pelvic_horizontal,
                    pelvic_vertical,
                    comments,
                    donor_cow,
                    ai_bull,
                    promote_date,
                    demote_date,
                    birth_weight,
                    weaning_weight,
                    yearling_weight,
                    adj_birth_weight,
                    adj_weaning_weight,
                    adj_yearling_weight,
                    last_tip_to_tip,
                    last_total_horn,
                    last_base,
                    last_composite,
                    last_horn_measure_date,
                    last_weight,
                    last_height,
                    last_bcs,
                    last_weight_date,
                    embryo_recovery_date,
                    nait_number,
                    nlis_number,
                    last_treatment_date,
                    withdrawal_date,
                    castration_date,
                    next_booster_date))
        con.commit()

    except Exception as e:
        logging.error("{} {}".format(inspect.stack()[0][3], e))

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def load_bulls(_file_name):
    con = None
    csvfile = None

    try:
        breeding_forms = load_animal_custom_fields("BREEDING FORM")
        color_DNAs = load_animal_custom_fields("COAT COLOR DNA")
        breeds = load_breeds_map()

        con = ec_psql_util.psql_connection(_database='rafter', _user='postgres', _password='postgres')

        cur = con.cursor()
        cur.execute(SQL_DROP_BULL)
        con.commit()
        cur.execute(SQL_CREATE_BULL)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                active = ec_util.to_upper_or_none(row[25])  # Z
                if active != "ACTIVE" and active != "REFERENCE":
                    continue
                id = ec_util.to_pos_int_or_none(row[0])  # A
                sire_id = ec_util.to_pos_int_or_none(row[27])  # AB
                dam_id = ec_util.to_pos_int_or_none(row[28])  # AC
                real_dam_id = ec_util.to_pos_int_or_none(row[30])  # AE
                # breed
                breed_key = ec_util.to_pos_int_or_none(row[20])  # U
                breed = ec_hashmap.get(breeds, breed_key)
                breeding_type = ec_hashmap.get(breeding_forms, id)
                if breeding_type:
                    if "FLUSH" in breeding_type:
                        breeding_type = "FLUSH"
                    elif "AI" in breeding_type:
                        breeding_type = "AI"
                    elif "NS" in breeding_type:
                        breeding_type = "NS"
                    elif "RECIPIENT" in breeding_type:
                        breeding_type = "RECIPIENT"
                    else:
                        breeding_type = None

                coat_color_dna = ec_hashmap.get(color_DNAs, id)
                if coat_color_dna:
                    if "ED/ED" in coat_color_dna:
                        coat_color_dna = "ED/ED"
                    elif "ED/E" in coat_color_dna:
                        coat_color_dna = "ED/E"
                    elif "NOT TESTED" in coat_color_dna:
                        coat_color_dna = "NOT TESTED"
                    else:
                        coat_color_dna = None

                ear_tag = ec_util.to_upper_or_none(row[3])  # D
                animal_type = ec_util.to_upper_or_none(row[17])
                sex = ec_util.to_upper_or_none(row[18])
                dob_date = ec_util.to_upper_or_none(row[35])
                dob_year = None
                if dob_date:
                    dob_year = dob_date.split("-")[0]

                seller_id = ec_util.to_pos_int_or_none(row[43])  # AR
                current_breeding_status = ec_util.to_upper_or_none(row[59])  # BH
                last_calving_date = ec_util.to_date_or_none(row[58])  # BG
                estimated_calving_date = ec_util.to_date_or_none(row[60])  # BI
                last_breeding_date = ec_util.to_date_or_none(row[58])
                contact_id = ec_util.to_pos_int_or_none(row[43])  # AR
                dob = ec_util.to_date_or_none(row[35])

                cur.execute(SQL_INSERT_BULL, (
                    id,
                    sire_id,
                    dam_id,
                    real_dam_id,
                    breed,
                    breeding_type,
                    coat_color_dna,
                    current_breeding_status,
                    dob,
                    ear_tag,
                    contact_id))
                con.commit()

    except psycopg2.DatabaseError as e:
        print('Error %s' % e)
        sys.exit(1)

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def load_sale_tickets():
    sale_tickets = ec_hashmap.new()

    try:
        with open("../data/cattle/sale_tickets.csv", "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                key = ec_util.to_pos_int_or_none(row[0])
                sale_date = ec_util.to_upper_or_none(row[6])
                if sale_date:
                    sale_year = int(sale_date.split("-")[0])
                    sale_month = int(sale_date.split("-")[1])
                    sale_day = int(sale_date.split("-")[2])
                ec_hashmap.set(sale_tickets, key, [sale_year, sale_month, sale_day])

    finally:
        if csvfile:
            csvfile.close()

    return sale_tickets


def load_animal_custom_fields(_field_name=None):
    if _field_name is None:
        return

    custom_fields = dict()

    try:
        with open("data/cattle/animal_custom_fields.csv", "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader)
            for row in reader:
                field_name = ec_util.to_upper_or_none(row[2])
                if field_name == _field_name.upper():
                    key = ec_util.to_pos_int_or_none(row[1])
                    value = ec_util.to_upper_or_none(row[3])
                    custom_fields[key] = value
    except Exception as e:
        logging.error("{} {}".format(inspect.stack()[0][3], e))

    finally:
        if csvfile:
            csvfile.close()

    return custom_fields


def load_breeds_map():
    breeds = dict()

    try:
        with open("data/cattle/breeds.csv", "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                key = ec_util.to_pos_int_or_none(row[0])
                name = ec_util.to_upper_or_none(row[2])
                breeds[key] = name

    except Exception as e:
        logging.error("{} {}".format(inspect.stack()[0][3], e))

    finally:
        if csvfile:
            csvfile.close()

    return breeds


def hashmap_contacts():
    contacts = ec_hashmap.new()

    csvfile = None
    try:
        with open("../data/cattle/contacts.csv", "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                key = ec_util.to_pos_int_or_none(row[0])
                name = ec_util.to_upper_or_none(row[8])
                ec_hashmap.set(contacts, key, name)

    finally:
        if csvfile:
            csvfile.close()

    return contacts


def write_dead(_dead, _year):
    csvfile = None
    try:
        file_name = "../data/cattle/rc_dead_" + str(_year) + ".csv"
        with open(file_name, "w") as csvfile:
            wr = csv.writer(csvfile, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)

            row = ["ID", "EAR TAG", "TYPE", "SEX", "DOB", "BREED", "YEAR DEATH"]
            wr.writerow(row)

            for death in _dead:
                if int(death.death_year) == _year:
                    row = [death.animal_id, death.ear_tag, death.animal_type, death.animal_sex, death.birth_year,
                           death.breed, death.death_year]
                    wr.writerow(row)
    finally:
        if csvfile:
            csvfile.close()


def write_sold(_sales, _year):
    csvfile = None
    try:
        file_name = "../data/cattle/rc_sales_" + str(_year) + ".csv"
        with open(file_name, "w") as csvfile:
            wr = csv.writer(csvfile, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)

            row = ["ID", "EAR TAG", "TYPE", "SEX", "DOB", "BREED", "SOLD YEAR", "SALE AMOUNT"]

            wr.writerow(row)

            for sale in _sales:
                if int(sale.year_sold) == _year:
                    row = [sale.animal_id, sale.ear_tag, sale.animal_type, sale.animal_sex, sale.birth_year, sale.breed,
                           sale.year_sold, sale.amount]
                    wr.writerow(row)
    finally:
        if csvfile:
            csvfile.close()


def write_purchased(_purchases, _year):
    csvfile = None
    try:
        file_name = "../data/cattle/rc_purchased_" + str(_year) + ".csv"
        with open(file_name, "w") as csvfile:
            wr = csv.writer(csvfile, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)

            row = ["ID", "EAR TAG", "TYPE", "SEX", "DOB", "BREED", "PURCHASE YEAR", "PURCHASE AMOUNT", "SELLER"]
            wr.writerow(row)

            for purchase in _purchases:
                if int(purchase.purchase_year) == _year:
                    row = [purchase.animal_id, purchase.ear_tag, purchase.animal_type, purchase.animal_sex,
                           purchase.birth_year, purchase.breed, purchase.purchase_year, purchase.amount,
                           purchase.seller_name]
                    wr.writerow(row)
    finally:
        if csvfile:
            csvfile.close()


def write_inventory(_inventories, _year):
    csvfile = None
    try:
        file_name = "../data/cattle/rc_inventory_" + str(_year) + ".csv"
        with open(file_name, "w") as csvfile:
            wr = csv.writer(csvfile, delimiter="\t", quotechar='"', quoting=csv.QUOTE_MINIMAL)

            row = ["ID", "EAR TAG", "TYPE", "SEX", "DOB", "BREED"]
            wr.writerow(row)

            for inventory in _inventories:
                row = [inventory.animal_id, inventory.ear_tag, inventory.animal_type, inventory.animal_sex,
                       inventory.birth_year, inventory.breed]
                wr.writerow(row)
    finally:
        if csvfile:
            csvfile.close()


def load_breedings(_file_name):
    con = None
    SQL_INSERT_BREEDING = "INSERT INTO cattle.breeding(" \
                          "id," \
                          "animal_id, " \
                          "bull_animal_id, " \
                          "breeding_method, " \
                          "breeding_date," \
                          "breeding_end_date, " \
                          "estimated_calving_date, " \
                          "cleanup, " \
                          "embryo_id," \
                          "pregnancy_check_id)" \
                          "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    SQL_DROP_BREEDING = "DROP TABLE IF EXISTS cattle.breeding CASCADE"
    SQL_CREATE_BREEDING = "CREATE TABLE cattle.breeding (" \
                          "id integer NOT NULL," \
                          "animal_id integer NOT NULL," \
                          "bull_animal_id integer," \
                          "breeding_method character(2)," \
                          "breeding_date date NOT NULL," \
                          "breeding_end_date date," \
                          "estimated_calving_date date," \
                          "cleanup boolean NOT NULL," \
                          "embryo_id integer," \
                          "pregnancy_check_id integer," \
                          "CONSTRAINT breeding_pkey PRIMARY KEY (id))"

    try:
        con = ec_psql_util.psql_connection(_host="spowell-home", _database='rafter', _user='postgres', _password='postgres')

        cur = con.cursor()
        cur.execute(SQL_DROP_BREEDING)
        con.commit()
        cur.execute(SQL_CREATE_BREEDING)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                # id
                id = ec_util.to_pos_int_or_none(row[0])
                animal_id = ec_util.to_pos_int_or_none(row[1])
                bull_animal_Id = ec_util.to_pos_int_or_none(row[2])
                breeding_method = ec_util.to_upper_or_none(row[3])
                breeding_date = ec_util.to_date_or_none(row[4])
                breeding_end_date = ec_util.to_date_or_none(row[5])
                days_exposed = ec_util.to_pos_int_or_none(row[6])
                estimated_calving_date = ec_util.to_date_or_none(row[7])
                cleanup = ec_util.to_boolean_or_none(row[8])
                embryo_id = ec_util.to_pos_int_or_none(row[9])
                embryo_cl_side = ec_util.to_upper_or_none(row[10])
                pregnancy_check_id = ec_util.to_pos_int_or_none(row[17])

                cur.execute(SQL_INSERT_BREEDING, (
                    id,
                    animal_id,
                    bull_animal_Id,
                    breeding_method,
                    breeding_date,
                    breeding_end_date,
                    estimated_calving_date,
                    cleanup,
                    embryo_id,
                    pregnancy_check_id))
                con.commit()


    except Exception as e:
        logging.error("{} {}".format(inspect.stack()[0][3], e))

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def load_pregnancy_check(_file_name):
    con = None
    SQL_INSERT_PREG = "INSERT INTO cattle.pregnancy_check(" \
                      "id," \
                      "animal_id," \
                      "check_date," \
                      "check_method," \
                      "result," \
                      "ultrasound_sex," \
                      "expected_due_date)" \
                      "VALUES (%s,%s,%s,%s,%s,%s,%s)"
    SQL_DROP_PREG = "DROP TABLE IF EXISTS cattle.pregnancy_check CASCADE"
    SQL_CREATE_PREG = "CREATE TABLE cattle.pregnancy_check(" \
                      "id integer NOT NULL," \
                      "animal_id integer NOT NULL," \
                      "check_date date NOT NULL," \
                      "check_method character varying(12) NOT NULL," \
                      "result character varying(10) NOT NULL," \
                      "ultrasound_sex character varying(8)," \
                      "expected_due_date date," \
                      "CONSTRAINT pregnancy_check_pkey PRIMARY KEY (id))"

    try:
        con = ec_psql_util.psql_connection(_host="spowell-home", _database='rafter', _user='postgres', _password='postgres')

        cur = con.cursor()
        cur.execute(SQL_DROP_PREG)
        con.commit()
        cur.execute(SQL_CREATE_PREG)
        con.commit()

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                # id
                id = ec_util.to_pos_int_or_none(row[0])
                animal_id = ec_util.to_pos_int_or_none(row[1])
                check_date = ec_util.to_date_or_none(row[2])
                check_method = ec_util.to_upper_or_none(row[3])
                result = ec_util.to_upper_or_none(row[4])
                sex = ec_util.to_upper_or_none(row[5])
                expected_due_date = ec_util.to_date_or_none(row[7])

                cur.execute(SQL_INSERT_PREG, (
                    id,
                    animal_id,
                    check_date,
                    check_method,
                    result,
                    sex,
                    expected_due_date))
                con.commit()

    except Exception as e:
        logging.error("{} {}".format(inspect.stack()[0][3], e))

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()


def set_row_to_animal(_row):
    animal = None
    if _row is not None:
        animal = Animal()
        animal.setAnimalId(_row[0])
        animal.setEarTag(str(_row[1]))
        animal.setAnimalSex(ec_util.to_upper_or_none(_row[13]))
        animal.setSireId(_row[22])
        animal.setRealDamId(_row[25])
        animal.setName(ec_util.to_upper_or_none(_row[7]))
        animal.setRegNum(ec_util.to_upper_or_none(_row[8]))
        animal.animal_type = ec_util.to_upper_or_none(_row[13])
        animal.color_markings = ec_util.to_upper_or_none(_row[17])
        animal.horn_status = ec_util.to_upper_or_none(_row[16])
        animal.eid = ec_util.to_upper_or_none(_row[12])
        animal.conception_method = ec_util.to_upper_or_none(_row[35])
        animal.dob = _row[30]
        animal.dow = _row[32]
        animal.doy = _row[33]
        animal.breed = _row[87]
        animal.breed_percentage = _row[88]
        if (_row[5] is not None):
            animal.brand = _row[5]
            animal.brand_loc = _row[6]
            if (_row[6] is not None):
                animal.brand = _row[5] + "/" + _row[6]

    return animal


def lookup_animal_by_id(_animalid):
    con = None
    if _animalid is None:
        return None

    try:
        con = ec_psql_util.psql_connection(_database='rafter', _user='postgres', _password='postgres')
        cur = con.cursor()

        sql = SQL_SELECT_ANIMAL + \
              " LEFT OUTER JOIN cattle.breeds AS b ON a.breed_id = b.id" + \
              " LEFT OUTER JOIN cattle.breed_compositions AS bc ON (a.id = bc.animal_id AND a.breed_id = bc.breed_id) " + \
              " WHERE a.id = " + str(_animalid)

        cur.execute(sql)
        row = cur.fetchone()
        animal = set_row_to_animal(row)
        con.commit()
        return (animal)

    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def lookup_animal_by_ear_tag(_ear_tag):
    con = None

    try:
        con = ec_psql_util.psql_connection(_database='rafter', _user='postgres', _password='postgres')
        cur = con.cursor()
        s = SQL_SELECT_ANIMAL + \
            " LEFT OUTER JOIN cattle.breeds AS b ON a.breed_id = b.id" + \
            " LEFT OUTER JOIN cattle.breed_compositions AS bc ON (a.id = bc.animal_id AND a.breed_id = bc.breed_id) " + \
            " WHERE a.ear_tag='" + str(_ear_tag) + "'"
        cur.execute(s)
        row = cur.fetchone()
        animal = set_row_to_animal(row)
        con.commit()

        return (animal)

    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def create_customer_report(_ear_tag):
    con = None
    source = 'http://cgenregistry.cloudapp.net/CCA/graphics'

    try:
        animal = lookup_animal_by_ear_tag(_ear_tag)
        if animal is None:
            return

        s = lookup_animal_by_id(animal.getSireId())
        d = None
        d = lookup_animal_by_id(animal.getRealDamId())
        if s is not None and s.getSireId() is not None:
            ss = lookup_animal_by_id(s.getSireId())
        dss = None
        if ss is not None and ss.getRealDamId() is not None:
            dss = lookup_animal_by_id(ss.getRealDamId())
        sss = None
        if ss is not None and ss.getSireId() is not None:
            sss = lookup_animal_by_id(ss.getSireId())
        ds = None
        if s is not None and s.getSireId() is not None:
            ds = lookup_animal_by_id(s.getRealDamId())
        sds = None
        if ds is not None and ds.getSireId() is not None:
            sds = lookup_animal_by_id(ds.getSireId())
        dds = None
        if ds is not None and ds.getRealDamId() is not None:
            dds = lookup_animal_by_id(ds.getRealDamId())
        sd = None
        if d is not None and d.getSireId() is not None:
            sd = lookup_animal_by_id(d.getSireId())
        ssd = None
        if sd is not None and sd.getSireId() is not None:
            ssd = lookup_animal_by_id(sd.getSireId())
        dsd = None
        if sd is not None and sd.getRealDamId() is not None:
            dsd = lookup_animal_by_id(sd.getRealDamId())
        dd = None
        if d is not None and d.getRealDamId() is not None:
            dd = lookup_animal_by_id(d.getRealDamId())
        sdd = None
        if dd is not None and dd.getSireId() is not None:
            sdd = lookup_animal_by_id(dd.getSireId())
        ddd = None
        if dd is not None and dd.getRealDamId() is not None:
            ddd = lookup_animal_by_id(dd.getRealDamId())

        h = ''
        title = ""
        if animal.getName() is None:
            title = str(animal.eid)
        else:
            title = str(animal.getName())

        # General
        h += '<table align="center" cellspacing="0" cellpadding="1" border="0" style="border-color:#6fa527;border-width:1px;border-style:ridge;width:394px;border-collapse:collapse;">'
        h += '<tr align="left">'
        h += '<td colspan="3" style="font-weight:bold;color:White;background-color:#6fa527;border-color:#6fa527;border-width:2px;border-style:ridge;">'
        h += 'General: ' + title
        h += '</td>'
        h += '</tr>'
        h += create_general_html_row(animal)
        h += '</table>'
        h += '&nbsp;'
        # h += '<p style="page-break-before:always">'
        # Pedigree
        h += '<table align="center" cellspacing="0" cellpadding="2" border="0" style="border-color:#6fa527;border-width:2px;border-style:ridge;width:786px;border-collapse:collapse;">'
        h += '<tr>'
        h += '<td  align="left" colspan="4" style="font-weight:bold;color:White;background-color:#6fa527;border-color:#6fa527;border-width:2px;border-style:ridge;">'
        h += 'Pedigree: ' + title
        h += '</td>'
        h += '</tr>'
        h += '<tr>'
        h += '<td style="width:100px;"></td>'
        h += '<td style="width:100px;"></td>'
        h += '<td align="right" style="width:100px;">'
        h += '<IMG alt="" src="' + source + '/sirebar.gif">'
        h += '</td>'
        h += '<td style="width:486px;" align="left" >'
        h += '<a id="sssreg" href="#">'
        if sss is not None and sss.getRegNum() is not None:
            h += sss.getRegNum()
        h += '</a>'
        h += '<span id="sssname">  '
        if sss is not None and sss.getName() is not None:
            h += sss.getName()
        h += '</span>'
        h += '</td>'
        h += '</tr>'
        h += '<tr>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td align="right" style="width:100px;">'
        h += '<IMG alt="" src="' + source + '/sirebar.gif">'
        h += '</td>'
        h += '<td align="left" colspan="2" style="width:586px;">'
        h += '<a id="ssreg" href="#">'
        if ss is not None and ss.getRegNum() is not None:
            h += ss.getRegNum()
        else:
            h += ''
        h += '</a>'
        h += '<span id="ssname">  '
        if ss is not None and ss.getName() is not None:
            h += ss.getName()
        else:
            h += ''
        h += '</span>'
        h += '</td>'
        h += '</tr>'
        h += '<tr>'
        h += '<td style="width:100px;"></td><td style="width:100px;"></td><td align="right" style="width:100px;">'
        h += '<IMG alt="" src="' + source + '/dambar.gif">'
        h += '</td>'
        h += '<td align="left" style="width:486px;">'
        h += '<a id="dssreg" href="#">'
        if dss is not None and dss.getRegNum() is not None:
            h += dss.getRegNum()
        else:
            h += ''
        h += '</a>'
        h += '<span id="dssname">'
        if dss is not None and dss.getName() is not None:
            h += dss.getName()
        h += '</span>'
        h += '</td>'
        h += '</tr><tr>'
        h += '<td align="center" style="width:100px;">Sire:</td>'
        h += '<td align="left" colspan="3" style="width:686px;">'
        h += '<a id="sreg" href="#">'
        if s is not None and s.getRegNum() is not None:
            h += s.getRegNum()
        h += '</a>'
        h += '<span id="sname">  '
        if s is not None and s.getName() is not None:
            h += s.getName()
        h += '</span>'
        h += '</td>'
        h += '</tr><tr>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td align="right" style="width:100px;">'
        h += '<IMG alt="" src="' + source + '/sirebar.gif">'
        h += '</td>'
        h += '<td align="left" style="width:486px;">'
        h += '<a id="sdsreg" href="">'
        if sds is not None and sds.getRegNum() is not None:
            h += sds.getRegNum()
        h += '</a>'
        h += '<span id="sdsname">  '
        if sds is not None and sds.getName() is not None:
            h += sds.getName()
        h += '</span>'
        h += '</td>'
        h += '</tr><tr>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td align="right" style="width:100px;">'
        h += '<IMG alt="" src="' + source + '/dambar.gif">'
        h += '</td>'
        h += '<td align="left" colspan="2" style="width:586px;">'
        h += '<a id="dsreg" href="#">'
        if ds is not None and ds.getRegNum() is not None:
            h += ds.getRegNum()
        h += '</a>'
        h += '<span id="dsname">  '
        if ds is not None and ds.getName() is not None:
            h += ds.getName()
        h += '</span>'
        h += '</td>'
        h += '</tr><tr>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td align="right" style="width:100px;">'
        h += '<IMG alt="" src="' + source + '/dambar.gif">'
        h += '</td>'
        h += '<td align="left" style="width:486px;">'
        h += '<a id="ddsreg" href="#">'
        if dds is not None and dds.getRegNum() is not None:
            h += dds.getRegNum()
        h += '</a>'
        h += '<span id="ddsname">  '
        if dds is not None and dds.getName() is not None:
            h += dds.getName()
        h += '</span>'
        h += '</td>'
        h += '</tr><tr>'
        h += '<td colspan="4" style="width:786px;"></td>'
        h += '</tr><tr>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td align="right" style="width:100px;">'
        h += '<IMG alt="" src="' + source + '/sirebar.gif">'
        h += '</td>'
        h += '<td align="left" style="width:486px;">'
        h += '<a id="ssdreg" href="#">'
        if ssd is not None and ssd.getRegNum() is not None:
            h += ssd.getRegNum()
        h += '</a>'
        h += '<span id="ssdname">  '
        if ssd is not None and ssd.getName() is not None:
            h += ssd.getName()
        h += '</span>'
        h += '</td>'
        h += '</tr><tr>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td align="right" style="width:100px;">'
        h += '<IMG alt="" src="' + source + '/sirebar.gif">'
        h += '</td>'
        h += '<td align="left" colspan="2" style="width:586px;">'
        h += '<a id="sdreg" href="#">'
        if sd is not None and sd.getRegNum() is not None:
            h += sd.getRegNum()
        h += '</a>'
        h += '<span id="sdname">  '
        if sd is not None and sd.getName() is not None:
            h += sd.getName()
        h += '</span>'
        h += '</td>'
        h += '</tr><tr>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td align="right" style="width:100px;">'
        h += '<IMG alt="" src="' + source + '/dambar.gif">'
        h += '</td>'
        h += '<td align="left" style="width:486px;">'
        h += '<a id="dsdreg" href="#">'
        if dsd is not None and dsd.getRegNum() is not None:
            h += dsd.getRegNum()
        h += '</a>'
        h += '<span id="dsdname">  '
        if dsd is not None and dsd.getName() is not None:
            h += dsd.getName()
        h += '</span>'
        h += '</td>'
        h += '</tr><tr>'
        h += '<td align="center" style="width:100px;">Dam:</td>'
        h += '<td align="left" colspan="3" style="width:686px;">'
        h += '<a id="dreg" href="#">'
        if d is not None and d.getRegNum() is not None:
            h += d.getRegNum()
        h += '</a>'
        h += '<span id="dname">  '
        if d is not None and d.getName() is not None:
            h += d.getName()
        h += '</span>'
        h += '</td>'
        h += '</tr><tr>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td align="right" style="width:100px;">'
        h += '<IMG alt="" src="' + source + '/sirebar.gif">'
        h += '</td>'
        h += '<td align="left" style="width:486px;">'
        h += '<a id="sddreg" href="#">'
        if sdd is not None and sdd.getRegNum() is not None:
            h += sdd.getRegNum()
        h += '</a>'
        h += '<span id="sddname">  '
        if sdd is not None and sdd.getName() is not None:
            h += sdd.getName()
        h += '</span>'
        h += '</td>'
        h += '</tr><tr>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td align="right" style="width:100px;">'
        h += '<IMG alt="" src="' + source + '/dambar.gif">'
        h += '</td>'
        h += '<td align="left" colspan="2" style="width:586px;">'
        h += '<a id="ddreg" href="#">'
        if dd is not None and dd.getRegNum() is not None:
            h += dd.getRegNum()
        h += '</a>'
        h += '<span id="ddname">  '
        if dd is not None and dd.getName() is not None:
            h += dd.getName()
        h += '</span>'
        h += '</td>'
        h += '</tr><tr>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td style="width:100px;">'
        h += '</td>'
        h += '<td align="right" style="width:100px;">'
        h += '<IMG alt="" src="' + source + '/dambar.gif">'
        h += '</td>'
        h += '<td align="left" style="width:486px;">'
        h += '<a id="dddreg" href="#">'
        if ddd is not None and ddd.getRegNum() is not None:
            h += ddd.getRegNum()
        h += '</a>'
        h += '<span id="dddname">  '
        if ddd is not None and ddd.getName() is not None:
            h += ddd.getName()
        h += '</span>'
        h += '</td>'
        h += '</tr>'
        h += '</table>'
        h += '&nbsp;'
        # h += '<p style="page-break-before:always">'

        # Measurements
        h += '<table align="center" cellspacing="0" cellpadding="2" border="0" style="border-color:#6fa527;border-width:2px;border-style:ridge;width:786px;border-collapse:collapse;">'
        h += '<tr>'
        h += '<td  align="left" colspan="4" style="font-weight:bold;color:White;background-color:#6fa527;border-color:#6fa527;border-width:2px;border-style:ridge;">'
        h += 'Measurements: ' + title
        h += '</td>'
        h += '</tr>'
        h += '<tr align="left" style="font-weight:bold;color:black;background-color:#f2f2f2;">'
        h += '<td>Date</td>'
        h += '<td>Weight</td>'
        h += '<td>Gain</td>'
        if animal.getAnimalSex() == "BULL":
            h += '<td>Scrotal</td>'
        else:
            h += '<td></td>'
        h += '</tr>'
        h += create_measurements_html_row(_ear_tag)
        h += '</table>'
        h += '&nbsp;'
        # h += '<p style="page-break-before:always">'

        # EPDs
        h += '<table align="center" cellspacing="0" cellpadding="1" border="0" style="border-color:#6fa527;border-width:1px;border-style:ridge;width:786px;border-collapse:collapse;">'
        h += '<tr>'
        h += '<td  align="left" colspan="3" style="font-weight:bold;color:White;background-color:#6fa527;border-color:#6fa527;border-width:2px;border-style:ridge;">'
        h += 'EPDs: ' + title
        h += '</td>'
        h += create_epds_html_row(animal.getAnimalId())
        h += '</table>'
        h += '&nbsp;'
        # h += '<p style="page-break-before:always">'

        # Treatments
        h += '<table align="center" cellspacing="0" cellpadding="1" border="0" style="border-color:#6fa527;border-width:1px;border-style:ridge;width:786px;border-collapse:collapse;">'
        h += '<tr>'
        h += '<td  align="left" colspan="4" style="font-weight:bold;color:White;background-color:#6fa527;border-color:#6fa527;border-width:2px;border-style:ridge;">'
        h += 'Treatments: ' + title
        h += '</td>'
        h += create_treatment_html_row(animal.getAnimalId())
        h += '</table>'
        h += '&nbsp;'

        return (h)

    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def find_epds(_id):
    con = None
    SQL_QUERY = SQL_SELECT_EPDS + " AND e.animal_id = " + str(_id)

    try:
        con = ec_psql_util.psql_connection(_database='rafter', _user='postgres', _password='postgres')
        cur = con.cursor()

        cur.execute(SQL_QUERY)
        rows = cur.fetchall()
        epds = []

        for row in rows:
            epd = EPD()
            epd.id = ec_util.to_pos_int_or_none(row[0])
            epd.animal_id = ec_util.to_pos_int_or_none(row[1])
            epd.epd_reporting_period = ec_util.to_upper_or_none(row[2])
            epd.epd_type = ec_util.to_upper_or_none(row[3])
            epd.ced_epd = ec_util.to_float_or_none(row[4])
            epd.ced_acc = ec_util.to_float_or_none(row[5])
            epd.bw_epd = ec_util.to_float_or_none(row[6])
            epd.bw_acc = ec_util.to_float_or_none(row[7])
            epd.ww_epd = ec_util.to_float_or_none(row[8])
            epd.ww_acc = ec_util.to_float_or_none(row[9])
            epd.yw_epd = ec_util.to_float_or_none(row[10])
            epd.yw_acc = ec_util.to_float_or_none(row[11])
            epd.milk_epd = ec_util.to_float_or_none(row[12])
            epd.milk_acc = ec_util.to_float_or_none(row[13])
            epd.mww_epd = ec_util.to_float_or_none(row[14])
            epd.mww_acc = ec_util.to_float_or_none(row[15])
            epds.append(epd)
        con.commit()
        return (epds)

    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def create_measurements_html_row(_eartag):
    con = None
    SQL_SELECT_MEASUREMENTS = "SELECT " \
                              "m.category," \
                              "m.measure_date," \
                              "m.age_at_measure," \
                              "m.weight," \
                              "m.adjusted_weight," \
                              "m.adg," \
                              "m.wda," \
                              "m.reference_weight," \
                              "m.gain," \
                              "m.scrotal" \
                              " FROM cattle.animal AS a, cattle.measurements AS m" \
                              " WHERE a.id = m.animal_id AND a.ear_tag = '" + _eartag + "' ORDER BY measure_date DESC;"

    try:
        con = ec_psql_util.psql_connection(_database='rafter', _user='postgres', _password='postgres')
        cur = con.cursor()

        cur.execute(SQL_SELECT_MEASUREMENTS)
        rows = cur.fetchall()

        html = ""
        tr_even_style = '<tr align="left" style="background-color:#f2f2f2;padding:8px;">'
        tr_odd_style = '<tr align="left" style="background-color:white;padding:8px;">'
        counter = 0

        for row in rows:
            category = ec_util.to_upper_or_none(row[0])
            measure_date = ec_util.to_date_or_none(row[1])
            age_at_measure = ec_util.to_pos_int_or_none(row[2])
            weight = ec_util.to_pos_int_or_none(row[3])
            adjusted_weight = ec_util.to_pos_int_or_none(row[4])
            adg = ec_util.to_float_or_none(row[5])
            wda = ec_util.to_float_or_none(row[6])
            reference_weight = ec_util.to_pos_int_or_none(row[7])
            gain = ec_util.to_pos_int_or_none(row[8])
            scrotal = ec_util.to_float_or_none(row[9])

            if category == "WORKING WEIGHT":
                continue

            counter += 1

            html_tr = None
            if counter % 2 == 0:
                html_tr = tr_even_style
            else:
                html_tr = tr_odd_style

            if category == 'BIRTH':
                html += html_tr
                html += '<td>' + measure_date.strftime("%b %d, %Y") + '</td>'
                html += '<td>' + str(weight) + ' lbs / ' + str(adjusted_weight) + ' adj</td>'
                html += '<td></td>'
                html += '<td> </td>'
                html += '</tr>'
                html += html_tr
                html += '<td>' + str(category).title() + '</td>'
                html += '<td></td>'
                html += '<td></td>'
                html += '<td></td>'
            else:
                html += html_tr
                html += '<td>' + measure_date.strftime("%b %d, %Y") + '</td>'
                if adjusted_weight is None:
                    html += '<td>' + str(weight) + ' lbs </td>'
                else:
                    html += '<td>' + str(weight) + ' lbs / ' + str(adjusted_weight) + ' adj</td>'
                html += '<td>' + str(gain) + ' lbs</td>'
                html += '<td> </td>'
                html += '</tr>'
                html += html_tr
                html += '<td>' + str(category).title() + ' - ' + str(age_at_measure) + ' days</td>'
                html += '<td>' + str(wda) + ' lbs/day</td>'
                html += '<td>' + str(adg) + ' lbs/day</td>'
                if scrotal is not None:
                    html += '<td>' + str(round(scrotal, 1)) + ' cm </td>'
                else:
                    html += '<td> </td>'
                html += '</tr>'

        return (html)

    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def create_epds_html_row(_animalid):
    con = None
    SQL_SELECT = SQL_SELECT_EPDS + " AND e.animal_id =" + str(_animalid)

    try:
        con = ec_psql_util.psql_connection(_database='rafter', _user='postgres', _password='postgres')
        cur = con.cursor()

        cur.execute(SQL_SELECT)
        if cur.rowcount == 0:
            return ""
        rows = cur.fetchall()

        h = ""

        for row in rows:
            epd_reporting_period = ec_util.to_upper_or_none(row[2])
            if epd_reporting_period is None:
                return ""
            h += '<tr align="left" style="background-color:#6fa527;color:White;">'
            h += '<td style="text-align:justify;padding:5px;font-weight:bold;">EPD Reporting Period</td>'
            h += '<td colspan="2" >' + epd_reporting_period + '</td>'
            h += '</tr>'
            epd_type = ec_util.to_upper_or_none(row[3])
            h += '<tr align="left" style="background-color:#6fa527;color:white;">'
            h += '<td style="text-align:justify;padding:5px;font-weight:bold;">EDP Type</td>'
            h += '<td colspan="2" >' + epd_type + '</td>'
            h += '</tr>'

            h += '<tr align="left" style="color:Black;background-color:#f2f2f2;font-weight: bold;">'
            h += '<td>EPD</td>'
            h += '<td>Value</td>'
            h += '<td>Acc</td>'
            h += '</tr>'

            ced = ec_util.to_float_or_none(row[4])
            h += '<tr align="left" style="background-color:white;">'
            h += '<td>Calving Ease Direct</td>'
            h += '<td>' + str(ced) + '</td>'
            ced_acc = ec_util.to_float_or_none(row[5])
            if epd_type == "PE" or ced_acc is None:
                h += '<td>PE</td>'
            else:
                h += '<td>' + str(ced_acc) + '</td>'
            h += '</tr>'

            bw_epd = ec_util.to_float_or_none(row[6])
            h += '<tr align="left" style="background-color:#f2f2f2;">'
            h += '<td>Birth Weight</td>'
            h += '<td>' + str(bw_epd) + '</td>'
            bw_acc = ec_util.to_float_or_none(row[7])
            if epd_type == "PE" or bw_acc is None:
                h += '<td>PE</td>'
            else:
                h += '<td>' + str(bw_acc) + '</td>'
            h += '</tr>'

            ww_epd = ec_util.to_float_or_none(row[8])
            h += '<tr align="left" style="background-color:white;">'
            h += '<td>Weaning Weight</td>'
            h += '<td>' + str(ww_epd) + '</td>'
            ww_acc = ec_util.to_float_or_none(row[9])
            if epd_type == "PE" or ww_acc is None:
                h += '<td>PE</td>'
            else:
                h += '<td>' + str(ww_acc) + '</td>'
            h += '</tr>'

            yw_epd = ec_util.to_float_or_none(row[10])
            h += '<tr align="left" style="background-color:#f2f2f2;">'
            h += '<td>Yearling Weight</td>'
            h += '<td>' + str(yw_epd) + '</td>'
            yw_acc = ec_util.to_float_or_none(row[11])
            if epd_type == "PE" or yw_acc is None:
                h += '<td>PE</td>'
            else:
                h += '<td>' + str(yw_acc) + '</td>'
            h += '</tr>'

            milk_epd = ec_util.to_float_or_none(row[12])
            h += '<tr align="left" style="background-color:white;">'
            h += '<td>Milk</td>'
            h += '<td>' + str(milk_epd) + '</td>'
            milk_acc = ec_util.to_float_or_none(row[13])
            if epd_type == "PE" or milk_acc is None:
                h += '<td>PE</td>'
            else:
                h += '<td>' + str(milk_acc) + '</td>'
            h += '</tr>'

            # mww_epd = ec_util.to_float_or_none(row[14])
            # h += '<tr align="left" style="background-color:#f2f2f2;">'
            # h += '<td>Maternal Weaning Weight</td>'
            # h += '<td>' + str(mww_epd) + '</td>'
            # mww_acc = ec_util.to_float_or_none(row[15])
            # h += '<td>' + str(mww_acc) + '</td>'
            # h += '</tr>'

        return (h)

    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def create_treatment_html_row(_animalid):
    con = None
    sql_select = SQL_SELECT_TREATMENTS + " AND t.animal_id =" + str(_animalid) + 'ORDER BY t.treatment_date DESC'

    try:
        con = ec_psql_util.psql_connection(_database='rafter', _user='postgres', _password='postgres')
        cur = con.cursor()

        cur.execute(sql_select)
        rows = cur.fetchall()

        tr_even_style = '<tr align="left" style="background-color:#f2f2f2;padding:8px;">'
        tr_odd_style = '<tr align="left" style="background-color:white;padding:8px;">'
        counter = 0

        h = ""
        h += '<tr align="left" style="color:Black;background-color:#f2f2f2;font-weight: bold;">'
        h += '<td>Treatment Date</td>'
        h += '<td>Medication</td>'
        h += '<td></td>'
        h += '<td></td>'
        h += '</tr>'

        for row in rows:
            counter += 1
            html_tr = None
            if counter % 2 == 0:
                html_tr = tr_even_style
            else:
                html_tr = tr_odd_style

            treatment_date = ec_util.to_date_or_none(row[2])
            h += html_tr
            if treatment_date is not None:
                h += '<td>' + treatment_date.strftime("%b %d, %Y") + '</td>'
            else:
                h += '<td></td>'
            medication = ec_util.to_upper_or_none(row[3])
            if medication == 'FOSGAARD':
                medication = 'FUSOGARD'
            h += '<td colspan="3">' + medication + '</td>'
            h += '</tr>'

        return h

    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def create_general_html_row(_animal):
    con = None

    try:
        h = ""
        h += '<tr style="color:black;background-color:white;text-align:left;">'
        h += '<td colspan="1" style="font-weight:bold;width:100px;">Ear Tag</td>'
        h += '<td colspan="3" style=width:100px;">' + _animal.getEarTag() + '</td>'
        h += '</tr>'
        h += '<tr style="color:black;background-color:white;text-align:left;">'
        h += '<td colspan="1" style="font-weight: bold">Brand</td>'
        h += '<td colspan="3" >' + str(_animal.brand) + '</td>'
        h += '</tr>'
        h += '<tr style="color:black;background-color:white;text-align:left;">'
        h += '<td colspan="1" style="font-weight: bold;">Name</td>'
        h += '<td colspan="3" >' + str(_animal.name) + '</td>'
        h += '</tr>'
        h += '<tr style="color:black;background-color:white;text-align:left;">'
        h += '<td colspan="1" style="font-weight: bold;">Reg Num</td>'
        h += '<td colspan="3" >' + str(_animal.reg_num) + '</td>'
        h += '</tr>'
        h += '<tr style="color:black;background-color:white;text-align:left;">'
        h += '<td colspan="1" style="font-weight: bold;">Electronic ID</td>'
        h += '<td colspan="3" >' + str(_animal.eid) + '</td>'
        h += '</tr>'
        h += '<tr style="color:black;background-color:white;text-align:left;">'
        h += '<td colspan="1" style="font-weight: bold;">Animal Type</td>'
        h += '<td colspan="3" >' + str(_animal.animal_type) + '</td>'
        h += '</tr>'
        h += '<tr style="color:black;background-color:white;text-align:left;">'
        h += '<td colspan="1" style="font-weight: bold;">Color Markings</td>'
        h += '<td colspan="3" >' + str(_animal.color_markings) + '</td>'
        h += '</tr>'
        h += '<tr style="color:black;background-color:white;text-align:left;">'
        h += '<td colspan="1" style="font-weight: bold;">Horn Status</td>'
        h += '<td colspan="3" >' + str(_animal.horn_status) + '</td>'
        h += '</tr>'
        h += '<tr style="color:black;background-color:white;text-align:left;">'
        h += '<td colspan="1" style="font-weight: bold;">Breed</td>'
        h += '<td colspan="3" >' + str(_animal.breed) + '</td>'
        h += '</tr>'
        h += '<tr style="color:black;background-color:white;text-align:left;">'
        date = None
        if _animal.dob is not None:
            date = _animal.dob.strftime("%b %d, %Y")
        h += '<td colspan="1" style="font-weight: bold;">Birth Date</td>'
        h += '<td colspan="3" >' + str(date) + '</td>'
        h += '</tr>'
        h += '<tr style="color:black;background-color:white;text-align:left;">'
        h += '<td colspan="1" style="font-weight: bold;">Weaning Date</td>'
        date = None
        if _animal.dow is not None:
            date = _animal.dow.strftime("%b %d, %Y")
        h += '<td colspan="3" >' + str(date) + '</td>'
        h += '</tr>'
        h += '<tr style="color:black;background-color:white;text-align:left;">'
        h += '<td colspan="1" style="font-weight: bold;">Yearling Date</td>'
        date = None
        if _animal.doy is not None:
            date = _animal.doy.strftime("%b %d, %Y")
        h += '<td colspan="3" >' + str(date) + '</td>'
        h += '</tr>'
        h += '<tr style="color:black;background-color:white;text-align:left;">'
        h += '<td colspan="1" style="font-weight: bold;">Conception Method</td>'
        h += '<td colspan="3" >' + str(_animal.conception_method) + '</td>'
        h += '</tr>'
        return h

    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def sort_tags(_file_name):
    con = None
    csvfile = None
    con = None
    sql_select = SQL_SELECT_ANIMAL + \
                 " LEFT OUTER JOIN cattle.pregnancy_check AS pc ON a.id = pc.animal_id" + \
                 " WHERE pc.check_date = '2016-04-22'"

    try:
        con = ec_psql_util.psql_connection(_database='rafter', _user='postgres', _password='postgres')
        cur = con.cursor()

        cur.execute(sql_select)
        rows = cur.fetchall()

        animal_inventory = []

        tag_sort = lambda x: (x[2], x[0], x[1])

        animal_inventory.sort(key=tag_sort)

        with open(_file_name, "r") as csvfile:
            reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(reader, None)
            for row in reader:
                active = ec_util.to_upper_or_none(row[25])  # Z

                if active != 'ACTIVE':
                    continue

                id = ec_util.to_pos_int_or_none(row[0])  # A
                # sire_id = ec_util.to_pos_int_or_none(row[27])  # AB
                # dam_id = ec_util.to_pos_int_or_none(row[28])  # AC
                # real_dam_id = ec_util.to_pos_int_or_none(row[30])  # AE
                # # breed
                # breed_key = ec_util.to_pos_int_or_none(row[20])  # U
                # breed = ec_hashmap.get(breeds, breed_key)
                # breeding_type = ec_hashmap.get(breeding_forms, id)
                # if breeding_type:
                #     if "FLUSH" in breeding_type:
                #         breeding_type = "FLUSH"
                #     elif "AI" in breeding_type:
                #         breeding_type = "AI"
                #     elif "NS" in breeding_type:
                #         breeding_type = "NS"
                #     elif "RECIPIENT" in breeding_type:
                #         breeding_type = "RECIPIENT"
                #     else:
                #         breeding_type = None
                #
                # coat_color_dna = ec_hashmap.get(color_DNAs, id)
                # if coat_color_dna:
                #     if "ED/ED" in coat_color_dna:
                #         coat_color_dna = "ED/ED"
                #     elif "ED/E" in coat_color_dna:
                #         coat_color_dna = "ED/E"
                #     elif "NOT TESTED" in coat_color_dna:
                #         coat_color_dna = "NOT TESTED"
                #     else:
                #         coat_color_dna = None

                ear_tag = ec_util.to_upper_or_none(row[3])  # D
                if ear_tag is None:
                    continue

                tag_number = ''
                number_idx = 0
                ear_tag_search = ear_tag
                for i, c in enumerate(ear_tag_search):
                    if i == 0 and c.isdigit():
                        tag_number += c
                    elif c.isdigit() is False:
                        number_idx = i
                        break
                    elif i > 0 and tag_number is not None:
                        tag_number += c

                tag_year = ''
                year_idx = 0
                tag_color = ''

                ear_tag_search = ear_tag[number_idx:]
                for i, c in enumerate(ear_tag_search):
                    if c == '-':
                        tag_color = ear_tag_search[i + 1:]
                        break
                    else:
                        tag_year += c

                # print tag_number
                # print tag_year
                # print tag_color
                # print ear_tag
                tag_list = []
                if tag_number == '':
                    tag_list.append(None)
                else:
                    tag_list.append(int(tag_number))

                tag_list.append(tag_year)
                tag_list.append(tag_color)
                # if tag_color != 'PURPLE':
                #     continue

                animal_inventory.append(tag_list)
                # animal_type = ec_util.to_upper_or_none(row[17])
                # sex = ec_util.to_upper_or_none(row[18])
                # dob_date = ec_util.to_upper_or_none(row[35])
                # dob_year = None
                # if dob_date:
                #     dob_year = dob_date.split("-")[0]
                #
                # seller_id = ec_util.to_pos_int_or_none(row[43])  # AR
                # current_breeding_status = ec_util.to_upper_or_none(row[59])  # BH
                # last_calving_date = ec_util.to_date_or_none(row[58])  # BG
                # estimated_calving_date = ec_util.to_date_or_none(row[60])  # BI
                # last_breeding_date = ec_util.to_date_or_none(row[58])
                # contact_id = ec_util.to_pos_int_or_none(row[43])  # AR
                # dob = ec_util.to_date_or_none(row[35])
                #
                # cur.execute(SQL_INSERT_COW, (
                #     id,
                #     active,
                #     sire_id,
                #     dam_id,
                #     real_dam_id,
                #     breed,
                #     breeding_type,
                #     coat_color_dna,
                #     current_breeding_status,
                #     dob,
                #     ear_tag,
                #     estimated_calving_date,
                #     last_breeding_date,
                #     last_calving_date,
                #     contact_id))
                # con.commit()

        animal_inventory.sort(key=tag_sort)

        for tag in animal_inventory:
            ear_tag = str(tag[0]) + str(tag[1]) + '-' + str(tag[2])
            print(ear_tag)
    except psycopg2.DatabaseError as e:
        print('Error %s' % e)
        sys.exit(1)

    finally:
        if con:
            con.close()
        if csvfile:
            csvfile.close()
