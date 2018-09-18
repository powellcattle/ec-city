__author__ = 'spowell'
import logging
import os
import socket

import arcpy


def sde_workspace(_hostname=None):
    workspace = None

    if _hostname is None:
        _hostname = socket.gethostname()
    if _hostname == "gis-development":
        workspace = r"C:\\Users\\sde\\AppData\\Roaming\\ESRI\\" \
                    r"Desktop10.3\\ArcCatalog\\dev.cityofelcampo.org.sde"
    elif _hostname == "gis":
        workspace = r"C:\\Users\\spowell\\AppData\\Roaming\\ESRI\Desktop10.3\\" \
                    r"ArcCatalog\\powellcattle.com.sde"
    else:
        workspace = \
            r"C:\\Users\\spowell\\AppData\\Roaming\\ESRI\\" \
            r"Desktop10.3\\ArcCatalog\\black-charolais.com.sde"
    return (workspace)


def fieldExists(_feature_class, _field_name):
    """
    :rtype : boolean
    """
    field_list = arcpy.ListFields(_feature_class)
    for field in field_list:
        if field.name.lower() == _field_name.lower():
            return True
    return False


def dbCompress(inputWS):
    if all([arcpy.Exists(inputWS), arcpy.Compress_management(inputWS), arcpy.Exists(inputWS)]):
        logging.info("Workspace (%s) clear to continue." % inputWS)
        return (True)
    else:
        logging.info("!!!!!!!! ERROR WITH WORKSPACE %s !!!!!!!!" % inputWS)
        return (False)


def CopyFeatureClasses(_fromdb, _todb):
    # Set workspaces
    arcpy.env.workspace = _fromdb
    wk2 = _todb

    # for feature classes within datasets
    for feature_class in arcpy.ListFeatureClasses():
        logging.info("Reading: {0}".format(feature_class))
        name = arcpy.Describe(feature_class)
        new_data = name.name.split(".")[2]
        if arcpy.Exists(wk2 + os.sep + new_data) == False:

            arcpy.Copy_management(feature_class, wk2 + os.sep + new_data)
            print "Completed copy on {0}".format(new_data)

        else:
            print "Feature class {0} already exists in the end_db so skipping".format(new_data)
    # Clear memory
    del feature_class
