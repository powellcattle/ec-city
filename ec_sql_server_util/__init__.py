import inspect
import logging
import pyodbc


def connect(driver, server, database, trusted_connection, uid):
    try:
        return pyodbc.connect(Driver=driver, Server=server, Database=database, Trusted_Connection=trusted_connection, uid=uid)

    # "Driver={SQL Server Native Client 11.0};"
    #                         "Server=HOME-GIS\SQLEXPRESS;"
    #                         "Database=ec;"
    #                         "Trusted_Connection=yes;"
    #                         "uid=HOME-GIS\\sde;pwd=sde")
    except Exception as e:
        logging.error("{} {}".format(inspect.stack()[0][3], e))
