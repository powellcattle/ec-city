__author__ = 'spowell'
import logging

import ec_addresses

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="load_addresses.log",
                    filemode="w",
                    level=logging.DEBUG,
                    datefmt="%m/%d/%Y %I:%M:%S %p")
# ec_addresses.load_unique_street_names()
# ec_addresses.load_unique_prefixes()
# ec_addresses.load_exceptions()
# ec_addresses.load_unique_units()
# ec_addresses.load_unique_street_types()
# ec_addresses.load_unique_street_type_aliases()
# ec_addresses.load_unique_prefix_aliases()
# ec_addresses.load_unique_street_name_aliases()
# ec_addresses.load_unique_full_street_names()
# ec_addresses.load_incode_addresses()
ec_addresses.load_parcel_addresses("D:/dev/projects/ec-city/data/WhartonCAD/Ownership.shp")
#
# Incode Data Load
ec_addresses.load_incode_addresses()
# HGAC Data Load
ec_addresses.load_e911_addresses("D:/dev/projects/ec-city/data/HGAC/oct1_2018/WhartonCo_Streets_Addresses_Oct1_2018/Wharton_HGAC_streets_addresses_Oct2018.gdb")
ec_addresses.load_starmap_streets("D:/dev/projects/ec-city/data/HGAC/oct1_2018/WhartonCo_Streets_Addresses_Oct1_2018/Wharton_HGAC_streets_addresses_Oct2018.gdb")

