#!/usr/bin/python
import os
import logging

import ec_incode
import ec_util

incode_path = r"C:\\dev\\incode"
incode_file = r"TMP_PC2HOST.TMP"
SQL_SELECT = r"SELECT sde.meter_read.mtime AS mtime FROM sde.meter_read"
SQL_UPDATE = r"UPDATE sde.meter_read SET mtime=%s WHERE id=%s"
logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="schedule_incode.log",
                    filemode="w",
                    level=logging.DEBUG,
                    datefmt="%m/%d/%Y %I:%M:%S %p")


def getFilePath():
    return os.path.join(incode_path, incode_file)


logging.info("update meter reads")
ec_incode.load_incode_readings(getFilePath(), ec_util.sde_workspace())
