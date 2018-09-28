import ec_arcpy_util
import arcpy
import logging
import sys

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="test.log",
                    filemode="w",
                    level=logging.DEBUG,
                    datefmt="%m/%d/%Y %I:%M:%S %p")

workspace = r"C:/Users/spowell/AppData/Roaming/Esri/Desktop10.6/ArcCatalog/localhost.sde"

try:
    workspace = ec_arcpy_util.sde_workspace(workspace)

    arcpy.env.workspace = workspace
    arcpy.AcceptConnections(workspace, False)
    arcpy.DisconnectUser(workspace, "ALL")

    data_sets = arcpy.ListDatasets()
    for ds in data_sets:
        if ds == "ec.sde.HGAC":
            print(True)




except:
    logging.error(sys.exc_info()[1])
