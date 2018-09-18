#!/usr/bin/python
import logging

import ec_arcpy_util
import ec_util

incode_path = r"C:\\dev\\incode"
incode_file = r"TMP_PC2HOST.TMP"
SQL_SELECT = r"SELECT sde.meter_read.mtime AS mtime FROM sde.meter_read"
SQL_UPDATE = r"UPDATE sde.meter_read SET mtime=%s WHERE id=%s"
logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="test_parcel_loading.log",
                    filemode="w",
                    level=logging.DEBUG,
                    datefmt="%m/%d/%Y %I:%M:%S %p")

workspace = ec_util.sde_workspace()
ec_arcpy_util.dbCompress(workspace)
