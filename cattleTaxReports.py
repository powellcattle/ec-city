#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import csv
import psycopg2

import ec_util
import ec_hashmap
import cattle
from cattle import AnimalPurchased
from cattle import AnimalDead
from cattle import AnimalSold
from cattle import AnimalRaised


con = None
SQL_INSERT_LIGHT = "INSERT INTO cattle.cow(geom,watts,lumens,owner,bulb,application,contract)" \
                   "VALUES (ST_Transform(ST_SetSRID(ST_MakePoint(%s,%s),4326),2278),%s,%s,%s,%s,%s,%s)"
SQL_DROP_COW = "DROP TABLE cattle.cow"
SQL_CREATE_LIGHT = "CREATE TABLE cattle.cow(" \
                   "light_id serial NOT NULL," \
                   "application character varying(40)," \
                   "bulb character varying(25)," \
                   "contract character varying(10)," \
                   "geom geometry(Geometry,2278)," \
                   "lumens integer NOT NULL," \
                   "owner character varying(35) NOT NULL," \
                   "watts integer," \
                   "CONSTRAINT light_pkey PRIMARY KEY (light_id))" \
                   "WITH (OIDS=FALSE)"
SQL_ALTER_LIGHT = "ALTER TABLE cattle.cow OWNER TO postgres"
SQL_GRANT_LIGHT = "GRANT ALL ON TABLE cattle.cow TO postgres"

try:
    con = psycopg2.connect(database='elc',
                           user='postgres',
                           password='postgres',
                           host='localhost')

    cur = con.cursor()
    # cur.execute(SQL_DROP_LIGHT)
    # con.commit()
    # cur.execute(SQL_CREATE_LIGHT)
    # con.commit()
    # cur.execute(SQL_ALTER_LIGHT)
    # con.commit()
    # cur.execute(SQL_GRANT_LIGHT)
    # con.commit()

    print("loading breeds")
    breeds = cattle.load_breeds()
    print("loading contacts")
    contacts = cattle.hashmap_contacts()
    print("loading sales")
    sales = cattle.load_sale_tickets()
    animals_dead = []
    animals_sold = []
    animals_purchased = []
    animals_inventory = []
    REPORT_YEAR = 2014

    with open("../data/cattle/animals.csv", "rb") as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(reader, None)
        for row in reader:
            # breed
            breed_key = ec_util.to_pos_int_or_none(row[20])  # U
            breed = ec_hashmap.get(breeds, breed_key)
            ear_tag = ec_util.to_upper_or_none(row[3])  # D
            id = ec_util.to_pos_int_or_none(row[0])
            active = ec_util.to_upper_or_none(row[25])
            animal_type = ec_util.to_upper_or_none(row[17])
            sex = ec_util.to_upper_or_none(row[18])
            dob_date = ec_util.to_upper_or_none(row[35])
            dob_year = None
            if dob_date:
                dob_year = dob_date.split("-")[0]

            if active == "REFERENCE":
                continue

            # dead
            if active == "DEAD":
                animal = AnimalDead()
                dod_date = ec_util.to_upper_or_none(row[50])
                dod_year = None
                if dod_date:
                    dod_year = dod_date.split("-")[0]
                animal.setDeathYear(dod_year)
                animal.setAnimalId(id)
                animal.setEarTag(ear_tag)
                animal.setAnimalType(animal_type)
                animal.setAnimalSex(sex)
                animal.setBreed(breed)
                animal.setBirthYear(int(dob_year))
                animals_dead.append(animal)
                # print animal
                continue

            # Sold Animals
            if row[46]:
                animal = AnimalSold()
                amount_sale = ec_util.to_float_or_none(row[46])
                animal.setSaleAmount(amount_sale)
                sale_id = ec_util.to_pos_int_or_none(row[45])
                sale_date = ec_hashmap.get(sales, sale_id)
                sale_year = None
                if sale_date:
                    sale_year = sale_date[0]
                    animal.setYearSold(sale_year)
                if animal.getYearSold() is None:
                    continue
                animal.setAnimalId(id)
                animal.setEarTag(ear_tag)
                animal.setAnimalType(animal_type)
                animal.setAnimalSex(sex)
                animal.setBreed(breed)
                animal.setBirthYear(dob_year)
                animals_sold.append(animal)
                # print animal
                continue;

            # Check for purchased cattle
            purchased = False
            if row[42] == "true": #AQ
                purchased = True
                # purchased from
                purchased_key = ec_util.to_pos_int_or_none(row[43]) #AR
                # print ec_hashmap.get(contacts,purchased_key)
                purchased_price = ec_util.to_float_or_none(row[44])  # AS
                purchased_date = ec_util.to_upper_or_none(row[41]) #AP
                if purchased_date:
                    purchased_year = purchased_date.split("-")[0]
                    purchased_month = purchased_date.split("-")[1]
                    purchased_day = purchased_date.split("-")[2]
                seller_id = ec_util.to_pos_int_or_none(row[43])
                seller = ec_hashmap.get(contacts, seller_id)
                animal = AnimalPurchased()
                animal.setPurchaseYear(purchased_year)
                animal.setPurchaseAmount(purchased_price)
                animal.setAnimalId(id)
                animal.setEarTag(ear_tag)
                animal.setAnimalType(animal_type)
                animal.setAnimalSex(sex)
                animal.setBreed(breed)
                animal.setSellerName(seller)
                animal.setBirthYear(dob_year)
                animals_purchased.append(animal)
                # print(animal)

            animal = AnimalRaised()
            animal.setAnimalId(id)
            animal.setEarTag(ear_tag)
            animal.setAnimalType(animal_type)
            animal.setAnimalSex(sex)
            animal.setBreed(breed)
            animal.setBirthYear(dob_year)
            animals_inventory.append(animal)

    cattle.write_dead(animals_dead, _year=REPORT_YEAR)
    cattle.write_sold(animals_sold, _year=REPORT_YEAR)
    cattle.write_purchased(animals_purchased, _year=REPORT_YEAR)
    cattle.write_inventory(animals_inventory, _year=REPORT_YEAR)


except psycopg2.DatabaseError, e:
    print 'Error %s' % e
    sys.exit(1)

else:
    sys.exit(0)

finally:
    if con:
        con.close()
    if csvfile:
        csvfile.close()