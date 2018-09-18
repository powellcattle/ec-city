#!/usr/bin/python
import logging

import arcpy

import ec_arcpy_util

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="load_addresses.log",
                    filemode="w",
                    level=logging.DEBUG,
                    datefmt="%m/%d/%Y %I:%M:%S %p")
# address = ec_addresses.get_street_name(None)
#print("{} {} {}".format(address.prefix,address.name,address.type))
workspace_meter = ec_arcpy_util.sde_workspace() + "\\\Water" + "\\\Meter"
workspace_meter_address = ec_arcpy_util.sde_workspace() + "\\\Address" + "\\\meter_address"
fields_meter = ["incode_account", "OID@", "SHAPE@"]
fields_meter_address = ["account", "house_number", "prefix", "street_name", "street_type", "secondary_name", "city",
                        "state", "zip", "OID@", "SHAPE@"]
where_clause = """"incode_account" = '01-9100'"""

rows = arcpy.da.SearchCursor(workspace_meter, fields_meter, where_clause)
for row in rows:
    with arcpy.da.InsertCursor(workspace_meter_address, fields_meter_address) as ic:
        ic.insertRow("01-9")
