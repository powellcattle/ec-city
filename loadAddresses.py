from __future__ import print_function, unicode_literals, absolute_import

import logging
import ec_addresses
import socket

__author__ = 'spowell'

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="load_addresses.log",
                    filemode="w",
                    level=logging.DEBUG,
                    datefmt="%m/%d/%Y %I:%M:%S %p")

if socket.gethostname() == 'gis':
    hgac_gdb = "E:/dev/projects/ec-city/data/HGAC/oct1_2018/WhartonCo_Streets_Addresses_Oct1_2018/Wharton_HGAC_streets_addresses_Oct2018.gdb"

else:
    hgac_gdb = "D:/dev/projects/ec-city/data/HGAC/oct1_2018/WhartonCo_Streets_Addresses_Oct1_2018/Wharton_HGAC_streets_addresses_Oct2018.gdb"
    incode_file_path = "data/incode/TMP_PC2HOST.TMP"

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

# Parcel Data Load
# ec_addresses.load_parcel_addresses("D:/dev/projects/ec-city/data/WhartonCAD/Ownership.shp")
# Incode Data Load
# address_list = ec_incode.read_address(incode_file_path)
# for address_tuples in address_list:
#
#     add_number = None
#     st_type = None
#     st_name = None
#     st_prefix = None
#     add_unit = None
#
#     for tuple in address_tuples:
#         print tuple
#         value,name = tuple
#         if name == "StreetNamePostType":
#             st_type = ec_util.to_upper_or_none(value)
#         elif name == "AddressNumber":
#             add_number = ec_util.to_pos_int_or_none(value)
#         elif name == "StreetNamePreDirectional":
#             st_prefix = ec_util.to_upper_or_none(value)
#         elif name == "StreetName":
#             st_name = ec_util.to_upper_or_none(value)
#         elif name == "OccupancyIdentifier":
#             if value == "#":
#                 continue
#             if add_unit:
#                 add_unit = add_unit + ec_util.to_upper_or_none(value)
#             else:
#                 add_unit = ec_util.to_upper_or_none(value)
#
#
#     address = ec_addresses.Address(add_number,st_prefix,st_name,st_type,None,add_unit,"EL CAMPO","77437")
#     if st_name:
#         print address



# HGAC Data Load
# ec_addresses.load_e911_addresses(hgac_gdb, True)
ec_addresses.load_starmap_streets(hgac_gdb, True)



