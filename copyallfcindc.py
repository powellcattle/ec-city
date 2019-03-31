import inspect
import logging

import arcpy
import yaml

import ec_arcpy_util

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="copyallfcindc.log",
                    filemode="w",
                    level=logging.ERROR,
                    datefmt="%m/%d/%Y %I:%M:%S %p")


class Settings:
    def __init__(self, _yml_file):
        with open(_yml_file, "r") as ymlfile:
            cfg = yaml.load(ymlfile)
            self.workspace = cfg["geodatabase"]["workspace"]
            self.data_set = str(cfg["geodatabase"]["data_set"]).lower()
            self.feature_classes = list(cfg["geodatabase"]["feature_classes"])


yml_file = "copyallfcindc.yml"
data_type = "FeatureClass"

try:
    settings = Settings(yml_file)
    #
    # arcgis stuff for multi-users
    arcpy.AcceptConnections(settings.workspace, False)
    arcpy.DisconnectUser(settings.workspace, "ALL")
    arcpy.env.workspace = settings.workspace

    data_set = ec_arcpy_util.find_dataset("".join(("*", settings.data_set)))

    if data_set is None:
        logging.error(f"Could not locate {settings.data_set}")
        exit(-1)

    for fc in settings.feature_classes:
        arcpy.CopyFeatures_management("".join((data_set, "//", fc)), "".join((data_set, "//", fc, "_1")))

    for fc in settings.feature_classes:
        arcpy.Delete_management("".join((data_set, "//", fc)))

    for fc in settings.feature_classes:
        arcpy.CopyFeatures_management("".join((data_set, "//", fc, "_1")), "".join((data_set, "//", fc)))

    for fc in settings.feature_classes:
        arcpy.Delete_management("".join((data_set, "//", fc, "_1")))

    arcpy.RegisterAsVersioned_management(data_set, "EDITS_TO_BASE")
    ec_arcpy_util.dbCompress(arcpy.env.workspace)
    arcpy.AcceptConnections(settings.workspace, True)
    logging.info("Successful completion")

except Exception as e:
    logging.error(f"{inspect.stack()[0][3]} {e}")
