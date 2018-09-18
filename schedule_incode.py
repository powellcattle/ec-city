#!/usr/bin/python
import os
import logging

import psycopg2

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


def last_mtime():
    logging.info("get last mtime")
    try:
        db = ec_util.psql_connection()
        cur = db.cursor()
        cur.execute(SQL_SELECT)
        row = cur.fetchone()
        _last_mtime = int(row[0])
        db.commit()
        return _last_mtime

    except psycopg2.Error as e:
        logging.info('Error %s' % e)
        if db:
            db.rollback();

    finally:
        if db:
            db.close()


def update_mtime():
    logging.info("update persistent mtime")
    try:
        db = ec_util.psql_connection()
        cur = db.cursor()
        current_time = int(current_mtime())
        logging.debug(current_time)
        cur.execute(SQL_UPDATE, (current_time, 1))
        db.commit()

    except psycopg2.DatabaseError, e:
        logging.info('Error %s' % e)
        if db:
            db.rollback();

    finally:
        if db:
            db.close()


def current_mtime():
    return int(os.path.getmtime(getFilePath()))


if current_mtime() > last_mtime():
    logging.info("update meter reads")
    ec_incode.load_incode_readings(getFilePath(), ec_util.sde_workspace())
    update_mtime()
else:
    logging.info("no update required")
