import logging

import ec_addresses
import ec_psql_util

__author__ = 'spowell'

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="load_addresses.log",
                    filemode="w",
                    level=logging.DEBUG,
                    datefmt="%m/%d/%Y %I:%M:%S %p")

hgac_gdb = "data/HGAC/oct1_2018/WhartonCo_Streets_Addresses_Oct1_2018/Wharton_HGAC_streets_addresses_Oct2018.gdb"
cad_shp = "data/WhartonCAD/Ownership.shp"
incode_file_path = "data/incode/TMP_PC2HOST.TMP"


ec_addresses.setup_not_found_address_table(ec_psql_util.psql_connection())

# HGAC Data Load
ec_addresses.load_e911_addresses(hgac_gdb, True)
# ec_addresses.load_starmap_streets(hgac_gdb, True)
# ec_addresses.load_incode_addresses(incode_file_path, True)
ec_addresses.load_parcel_addresses(cad_shp, True)
ec_addresses.create_unique_tables()


