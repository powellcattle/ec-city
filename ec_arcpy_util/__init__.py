__author__ = 'spowell'
import logging
import socket

import arcpy


def sde_workspace_via_host(_hostname=None):
    workspace = None
    if _hostname is None:
        _hostname = socket.gethostname()
    if _hostname == "gis":
        workspace = r"C:\Users\spowell\AppData\Roaming\ESRI\Desktop10.6\ArcCatalog\local.sde"
    elif _hostname == "home-gis":
        # workspace = r"C:\\Users\\spowell\\AppData\\Roaming\\Esri\\Desktop10.6\\ArcCatalog\\localhost-sql-server.sde"
        workspace = r"C:\\Users\\spowell\\AppData\\Roaming\\Esri\\Desktop10.6\\ArcCatalog\\localhost.sde"
    elif _hostname == "powell-gis":
        # workspace = r"C:\\Users\\spowell\\AppData\\Roaming\\Esri\\Desktop10.6\\ArcCatalog\\localhost-sql-server.sde"
        workspace = r"C:\\Users\\spowell\\AppData\\Roaming\\Esri\\Desktop10.6\\ArcCatalog\\localhost.sde"
    return workspace


def fc_feature_count(_feature_class):
    assert isinstance(arcpy.GetCount_management(_feature_class).getOutput, object)
    return int(arcpy.GetCount_management(_feature_class).getOutput(0))


def fieldExists(_feature_class, _field_name):
    """
    :rtype : boolean
    """
    field_list = arcpy.ListFields(_feature_class)
    for field in field_list:
        if field.name.lower() == _field_name.lower():
            return True
    else:
        return False


def find_dataset(_dataset: str) -> str:
    """
    :rtype: str
    """
    datasets = arcpy.ListDatasets(_dataset, r"Feature")
    for dataset in datasets:
        return dataset
    else:
        return None


def find_feature_class(_feature_class, _data_set):
    """
    :rtype: object
    """
    feature_classes = arcpy.ListFeatureClasses(_feature_class, r"All", _data_set)
    for fc in feature_classes:
        return fc
    else:
        return None


def dbCompress(inputWS):
    if all([arcpy.Exists(inputWS), arcpy.Compress_management(inputWS), arcpy.Exists(inputWS)]):
        logging.info(f"Workspace {inputWS} clear to continue.")
        return True
    else:
        logging.error(f"!!!!!!!! ERROR WITH WORKSPACE {inputWS} !!!!!!!!")
        return False
