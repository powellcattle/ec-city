#!/usr/bin/python
# -*- coding: utf-8 -*-

import cattle

cattle.load_calves(r"../data/cattle/animals.csv")
cattle.load_cows(r"../data/cattle/animals.csv")
cattle.load_bulls(r"../data/cattle/animals.csv")
cattle.load_contacts(r"../data/cattle/contacts.csv")
cattle.load_breedings(r"../data/cattle/breedings.csv")
cattle.load_pregnancy_check(r"../data/cattle/pregnancy_checks.csv")
cattle.load_embryos(r"../data/cattle/embryos.csv")
cattle.load_breed(r"../data/cattle/breeds.csv")
cattle.load_animals(r"../data/cattle/animals.csv")
cattle.create_tru_test(r"../data/cattle/tru_test_load.csv")
cattle.create_allflex(r"../data/cattle/allflex_load.csv")