#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import csv

import ec_util
import ec_hashmap
import cattle


try:

    year_filter = 2014
    breeds = cattle.load_breeds()
    contacts = cattle.hashmap_contacts()
    sale_tickets = cattle.load_sale_tickets()
    deaths = []
    sales = []
    inventory = []
    purchases = []

    with open("../data/cattle/animals.csv", "rb") as csvfile:
        reader = csv.reader(csvfile, delimiter=',', quotechar='"')
        next(reader, None)

        for row in reader:

            death_year = None
            sale_year = None
            birth_year = None
            birth_date = None
            breed = None
            purchased_year = None

            status = ec_util.to_upper_or_none(row[25])
            #
            # REFERENCE - not part of our herd
            if status == "REFERENCE":
                continue

            animal_id = ec_util.to_pos_int_or_none(row[0])
            ear_tag = ec_util.to_upper_or_none(row[3])
            animal_type = ec_util.to_upper_or_none(row[17])
            animal_sex = ec_util.to_upper_or_none(row[18])
            #
            # if we have DOB
            birth_date = ec_util.to_upper_or_none(row[35])
            if birth_date:
                birth_year = int(birth_date.split("-")[0])
                birth_month = int(birth_date.split("-")[1])
                birth_day = int(birth_date.split("-")[2])
            #
            # breed
            breed_key = ec_util.to_pos_int_or_none(row[20])
            if breed_key:
                breed = ec_hashmap.get(breeds, breed_key)


            # animal status dead
            # get additional death information


            if status == "DEAD":
                death_date = ec_util.to_upper_or_none(row[50])
                if death_date:
                    death_year = int(death_date.split("-")[0])
                    death_month = int(death_date.split("-")[1])
                    death_day = int(death_date.split("-")[2])

            elif status == "SOLD":
                sale_ticket_key = ec_util.to_pos_int_or_none(row[45])
                if sale_ticket_key and ec_hashmap.get(sale_tickets, sale_ticket_key):
                    sale_year = ec_hashmap.get(sale_tickets, sale_ticket_key)[0]
                    sale_month = ec_hashmap.get(sale_tickets, sale_ticket_key)[1]
                    sale_day = ec_hashmap.get(sale_tickets, sale_ticket_key)[2]
                    sale_gross = ec_util.to_float_or_none(row[46])
                    if sale_gross:
                        sale_cost = ec_util.to_float_or_none(row[48])
                    sale_net = sale_gross - sale_cost

            #
            # If purchased, get information
            purchased = False
            if row[42] == "true":
                purchased = True
                # purchased from
                purchased_key = ec_util.to_pos_int_or_none(row[43])
                if purchased_key:
                    seller_name = ec_hashmap.get(contacts, purchased_key)
                purchased_price = ec_util.to_float_or_none(row[44])
                purchased_date = ec_util.to_upper_or_none(row[41])
                if purchased_date:
                    purchased_year = int(purchased_date.split("-")[0])
                    purchased_month = int(purchased_date.split("-")[1])
                    purchased_day = int(purchased_date.split("-")[2])
            # breed
            breed_key = ec_util.to_pos_int_or_none(row[20])

            if status == "DEAD" and death_year == year_filter:
                animal = cattle.AnimalDead()
                animal.setAnimalId(animal_id)
                animal.setEarTag(ear_tag)
                animal.setBirthYear(birth_year)
                animal.setAnimalSex(animal_sex)
                animal.setAnimalType(animal_type)
                animal.setBreed(breed)
                animal.setDeathYear(death_year)
                deaths.append(animal)
            elif status == "SOLD" and sale_year == year_filter:
                animal = cattle.AnimalSold()
                animal.setAnimalId(animal_id)
                animal.setEarTag(ear_tag)
                animal.setBirthYear(birth_year)
                animal.setAnimalSex(animal_sex)
                animal.setAnimalType(animal_type)
                animal.setBreed(breed)
                animal.setYearSold(sale_year)
                animal.setSaleAmount(sale_net)
                sales.append(animal)
            if purchased and purchased_year == year_filter:
                animal = cattle.AnimalPurchase()
                animal.setAnimalId(animal_id)
                animal.setEarTag(ear_tag)
                animal.setBirthYear(birth_year)
                animal.setAnimalSex(animal_sex)
                animal.setAnimalType(animal_type)
                animal.setBreed(breed)
                animal.setPurchaseAmount(sale_net)
                animal.setSellerName(seller_name)
                animal.setPurchaseYear(purchased_year)
                purchases.append(animal)

            if cattle.Animal.isBornFilter(birth_year, year_filter) and \
                    cattle.Animal.isSoldFilter(sale_year, year_filter) and \
                    cattle.Animal.isDeathFilter(death_year, year_filter) and \
                    cattle.Animal.isPurchasedFilter(purchased_year, year_filter):
                animal = cattle.AnimalRaised()
                animal.setAnimalId(animal_id)
                animal.setEarTag(ear_tag)
                animal.setBirthYear(birth_year)
                animal.setAnimalSex(animal_sex)
                animal.setAnimalType(animal_type)
                animal.setBreed(breed)
                inventory.append(animal)

        print "For Year {0} - Deaths {1}, Purchased {2}, Sold {3}, Inventory {4}". \
            format(year_filter,
                   deaths.__len__(),
                   purchases.__len__(),
                   sales.__len__(),
                   inventory.__len__())

        csvfile.close()
        cattle.write_sold(sales, year_filter)
        cattle.write_dead(deaths, year_filter)
        cattle.write_purchased(purchases, year_filter)
        cattle.write_inventory(inventory, year_filter)



except RuntimeError as e:
    print 'Error %s' % e
    sys.exit(1)

else:
    sys.exit(0)

finally:
    if csvfile:
        csvfile.close()