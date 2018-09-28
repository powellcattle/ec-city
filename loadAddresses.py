__author__ = 'spowell'
import logging

import ec_addresses

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="load_addresses.log",
                    filemode="w",
                    level=logging.DEBUG,
                    datefmt="%m/%d/%Y %I:%M:%S %p")
ec_addresses.load_unique_street_names()
ec_addresses.load_unique_prefixes()
ec_addresses.load_exceptions()
ec_addresses.load_unique_units()
ec_addresses.load_unique_street_types()
ec_addresses.load_unique_street_type_aliases()
ec_addresses.load_unique_prefix_aliases()
ec_addresses.load_unique_street_name_aliases()
ec_addresses.load_unique_full_street_names()
# ec_addresses.load_incode_addresses()
# ec_addresses.load_parcel_addresses()
ec_addresses.load_e911_addresses("C:\\dev\\gis\\data\\Wharton_County_Streets_Address_Jan2016.gdb")
# ec_addresses.report_pregnant_by_month()
# ec_addresses.load_new_hgac_e911_addresses("C:\\dev\\temp\\El_Campo_addr_101615.gdb\\ElCampo_Addr_101615")
