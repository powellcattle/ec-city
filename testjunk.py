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
con = None

try:

    con = ec_sql_server_util.connect(driver=r"{SQL Server Native Client 11.0};",
                                     server="HOME-GIS\SQLEXPRESS;",
                                     database="ec;",
                                     trusted_connection="yes;",
                                     uid="HOME-GIS\\sde;pwd=sde;")

    sql = "SELECT DISTINCT(st_name), st_prefix, st_type FROM address.unique_street_names WHERE st_name = ? AND st_prefix = ?"
    cur = con.cursor()
    sql_tuple = ("CALHOUN","E")
    cur.execute(sql, sql_tuple)
    rows = cur.fetchall()


except pyodbc.Error as e:
    sqlstate = e.args[0]
    if '23000' == sqlstate:
        print(True)


except Exception as e:
    print(e)
finally:
    con.commit()
    con.close()


