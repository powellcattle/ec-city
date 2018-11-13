#!/usr/bin/python
# -*- coding: utf-8 -*-

import logging

import cattle

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="load_animals.log",
                    filemode="w",
                    level=logging.DEBUG,
                    datefmt="%m/%d/%Y %I:%M:%S %p")

cattle.load_animals(r"data/cattle/animals.csv")
cattle.load_pastures(r"data/cattle/pastures.csv")
cattle.load_contacts(r"data/cattle/contacts.csv")
cattle.load_breedings(r"data/cattle/breedings.csv")
cattle.load_pregnancy_check(r"data/cattle/pregnancy_checks.csv")
cattle.load_embryos(r"data/cattle/embryos.csv")
cattle.load_movements(r"data/cattle/movements.csv")
cattle.load_breeds(r"data/cattle/breeds.csv")
cattle.load_breed_compositions(r"data/cattle/breed_compositions.csv")
#
# cattle.create_tru_test(r"data/cattle/tru_test_load.csv")
# cattle.create_allflex(r"data/cattle/allflex_load.csv")
# cattle.load_measurements(r"data/cattle/measurements.csv")
# cattle.load_epds(r"data/cattle/epds.csv")
# cattle.load_treatments(r"data/cattle/treatments.csv")
# cattle.sort_tags(r"data/cattle/animals.csv")
