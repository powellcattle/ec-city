from __future__ import print_function, unicode_literals, absolute_import

import logging
import ec_addresses
import socket

import ec_incode
import ec_psql_util
import ec_util

__author__ = 'spowell'

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="load_addresses.log",
                    filemode="w",
                    level=logging.DEBUG,
                    datefmt="%m/%d/%Y %I:%M:%S %p")

if socket.gethostname() == 'gis':
    hgac_gdb = "E:/dev/projects/ec-city/data/HGAC/oct1_2018/WhartonCo_Streets_Addresses_Oct1_2018/Wharton_HGAC_streets_addresses_Oct2018.gdb"
    cad_shp = "D:/dev/projects/ec-city/data/WhartonCAD/Ownership.shp"

else:
    hgac_gdb = "D:/dev/projects/ec-city/data/HGAC/oct1_2018/WhartonCo_Streets_Addresses_Oct1_2018/Wharton_HGAC_streets_addresses_Oct2018.gdb"
    cad_shp = "data/WhartonCAD/Ownership.shp"
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
# ec_addresses.load_parcel_addresses(cad_shp)
# Incode Data Load
SQL_INSERT_ADDRESSES_INCODE = "INSERT INTO address.address_incode(add_number, st_prefix, st_name, st_suffix, st_type, add_unit, add_full, add_source, add_zip, add_city, fuzzy) " \
                              "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,soundex(%s)) ON CONFLICT ON " \
                              "CONSTRAINT address_incode_name_idx DO NOTHING"
address_list = ec_incode.read_address(incode_file_path)

psql_connection = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")
ec_addresses.setup_addresses_incode_table(psql_connection)
psql_connection = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")
for address in address_list:
    if address.is_valid():
        print(address.full_name())
        ec_addresses.insert_address(psql_connection, address, "INCODE", SQL_INSERT_ADDRESSES_INCODE)

if psql_connection:
    psql_connection.commit()
    psql_connection.close()

# add_number = None
# st_type = None
# st_name = None
# st_prefix = None
# add_unit = None
#
# for tuple in address_tuples:
#     value, name = tuple
#     if name == "StreetNamePostType":
#         st_type = ec_util.to_upper_or_none(value)
#     elif name == "AddressNumber":
#         add_number = ec_util.to_pos_int_or_none(value)
#     elif name == "StreetNamePreDirectional":
#         st_prefix = ec_util.to_upper_or_none(value)
#     elif name == "StreetName":
#         st_name = ec_util.to_upper_or_none(value)
#     elif name == "OccupancyIdentifier":
#         if value == "#":
#             continue
#         if add_unit:
#             add_unit = add_unit + ec_util.to_upper_or_none(value)
#         else:
#             add_unit = ec_util.to_upper_or_none(value)
#
#     address = ec_addresses.Address(add_number, st_prefix, st_name, st_type, None, add_unit, "EL CAMPO", "77437")
#     if st_name:
#         print
#         address

# HGAC Data Load
ec_addresses.load_e911_addresses(hgac_gdb, True)
ec_addresses.load_starmap_streets(hgac_gdb, False)
