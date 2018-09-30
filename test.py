import ec_arcpy_util
import ec_psql_util
import psycopg2
import arcpy
import logging
import sys

SQL_DROP_ADDRESSES_911 = "DROP TABLE IF EXISTS address.address_911"
SQL_CREATE_ADDRESSES_911 = "CREATE TABLE address.address_911(" \
                           "address_911_id SERIAL4 NOT NULL," \
                           "add_number INT NOT NULL, " \
                           "st_prefix CHARACTER(1) NULL," \
                           "st_name VARCHAR(100) NOT NULL," \
                           "st_type VARCHAR(10) NULL," \
                           "add_unit VARCHAR(10) NULL," \
                           "add_full VARCHAR(50) NOT NULL," \
                           "CONSTRAINT unique_address_911_pkey PRIMARY KEY (address_911_id))"
SQL_INSERT_ADDRESSES_911 = "INSERT INTO address.address_911(add_number, st_prefix, st_name, st_type, add_unit, add_full) VALUES (%s,%s,%s,%s,%s,%s)"

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="test.log",
                    filemode="w",
                    level=logging.DEBUG,
                    datefmt="%m/%d/%Y %I:%M:%S %p")

workspace = r"C:/Users/spowell/AppData/Roaming/Esri/Desktop10.6/ArcCatalog/localhost.sde"
con = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432", _read_only=False)
xid = con.xid(1, 'drop tables', 'connection 1')

try:
    workspace = ec_arcpy_util.sde_workspace(workspace)

    cur = con.cursor()
    cur.execute(SQL_DROP_ADDRESSES_911)
    cur.execute(SQL_CREATE_ADDRESSES_911)

    con.commit()
    cur = con.cursor()

    arcpy.env.workspace = workspace
    arcpy.AcceptConnections(workspace, False)
    arcpy.DisconnectUser(workspace, "ALL")

    rows = arcpy.SearchCursor("ec.sde.address911", fields="add_number; prefix; name; st_type; add_unit; add_full")
    for row in rows:
        print(row.getValue("add_full"))
        add_number = row.getValue("add_number")
        prefix = row.getValue("prefix")
        name = row.getValue("name")
        st_type = row.getValue("st_type")
        add_unit = row.getValue("add_unit")
        add_full = row.getValue("add_full")
        cur.execute(SQL_INSERT_ADDRESSES_911, (add_number, prefix, name, st_type, add_unit, add_full))

    # data_sets = arcpy.ListDatasets()
    # for ds in data_sets:
    #
    #     for fc in arcpy.ListFeatureClasses(feature_dataset=ds):
    #         print ec_arcpy_util.fc_feature_count(fc)
    #         print(fc)

    con.commit()

except psycopg2.Error as e:
    print(e)

except:
    logging.error(sys.exc_info()[1])
    con.rollback()

finally:
    con.close()
