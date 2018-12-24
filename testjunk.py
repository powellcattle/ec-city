import pyodbc

import arcpy
import logging

import ec_arcpy_util
import ec_sql_server_util

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="load_addresses.log",
                    filemode="w",
                    level=logging.DEBUG,
                    datefmt="%m/%d/%Y %I:%M:? %p")


portal_url = arcpy.GetActivePortalURL()
portal_desc = arcpy.GetPortalDescription(portal_url)