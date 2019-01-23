import pathlib
import re
import inspect
import logging
import os
import zipfile
from ftplib import FTP

import arcpy
import yaml

import ec_arcpy_util

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="parcel_updates.log",
                    filemode="w",
                    level=logging.ERROR,
                    datefmt="%m/%d/%Y %I:%M:%S %p")


class Settings:
    def __init__(self, _yml_file):
        with open(_yml_file, "r") as ymlfile:
            cfg = yaml.load(ymlfile)
            self.url = cfg["ftp"]["url"]
            self.username = cfg["ftp"]["username"]
            self.password = cfg["ftp"]["password"]
            self.isZipped = cfg["ftp"]["zipfile"]
            self.database = cfg["ftp"]["database"]
            self.download_path = cfg["ftp"]["download_path"]
            self.workspace = cfg["geodatabase"]["workspace"]
            self.data_set = str(cfg["geodatabase"]["data_set"]).lower()
            self.feature_class = str(cfg["geodatabase"]["feature_class"]).lower()


yml_file = "update_parcel_fc.yml"


def download_parcel_ftp(_settings: Settings):
    try:
        path = os.getcwd()
        os.rmdir(settings.download_path)
        os.makedirs(settings.download_path)

    except OSError as e:
        logging.error(f"{inspect.stack()[0][3]} {e}")

    ftp = FTP(settings.url)
    ftp.login(settings.username, settings.password)
    # Get All Files
    files = ftp.nlst()
    zip_file = None

    for file in files:
        if ".pdf" in file:
            continue
        if not ".zip" in file:
            continue
        zip_file = file
        ftp.retrbinary(f"RETR {zip_file}", open(settings.download_path + zip_file, 'wb').write)
    ftp.close()
    zip_ref = zipfile.ZipFile(settings.download_path + zip_file, 'r')
    zip_ref.extractall(settings.download_path)
    zip_ref.close()


def find_parcel_shape(_relative_path: str, _find_file: str, _filter: str ="*.shp") -> dict:
    p = pathlib.Path(_relative_path)
    for _ in p.rglob(_filter):
        if re.search(_find_file, _.name, re.IGNORECASE):
            return {"path": pathlib.PureWindowsPath(_.resolve()).as_posix(), "file": _.name.split(".")[0]}


parcel_update_fields = ["prop_id", "cad_url"]
previous = "_previous"
data_type = "FeatureClass"

try:
    settings = Settings(yml_file)
    # download_parcel_ftp(settings)
    shape_file_loc = find_parcel_shape(settings.download_path, settings.database)
    if not shape_file_loc:
        logging.error(f"Could not locate {settings.database}")
        exit(-1)
    #
    # arcgis stuff for multi-users
    arcpy.AcceptConnections(settings.workspace, False)
    arcpy.DisconnectUser(settings.workspace, "ALL")
    arcpy.env.workspace = settings.workspace

    # Manage FC names for copy
    #
    # if first time, create featuredataset HGAC
    ds = ec_arcpy_util.find_dataset("".join(("*", settings.data_set)))

    if ds is None:
        sr_2278 = arcpy.SpatialReference(2278)
        ds = arcpy.CreateFeatureDataset_management(settings.workspace, settings.data_set, sr_2278)

    #
    # do a little clean up
    # if *_previous fc exists, delete
    # if settings.feature_class already exists, append _previous to name
    fc = ec_arcpy_util.find_feature_class("".join(("*", settings.feature_class)), ds)
    fc_prev = ec_arcpy_util.find_feature_class("".join(("*", settings.feature_class, previous)), ds)
    if fc_prev:
        arcpy.Delete_management(fc_prev)
    if fc:
        fc_prev = "".join((settings.feature_class, previous))
        # arcpy.Rename_management(str(settings.feature_class), str(fc_prev), data_type)
        from_fc = "".join((settings.data_set,"/",settings.feature_class))
        to_fc = "".join((settings.data_set,"/",settings.feature_class,previous))
        arcpy.CopyFeatures_management(from_fc, to_fc)
        arcpy.Delete_management(fc)

    # get FTP supplied shapefile directory and file name
    shp_fc_file = shape_file_loc["path"]
    shp_name = shape_file_loc["file"]
    shp_fc = ec_arcpy_util.find_feature_class("".join(("*", shp_name)), ds)
    if shp_fc:
        arcpy.Delete_management(shp_fc)
    arcpy.FeatureClassToGeodatabase_conversion(shp_fc_file, ds)

    shp_fc = ec_arcpy_util.find_feature_class("".join(("*", shp_name)), ds)
    # arcpy.Rename_management(str(shp_name), str(settings.feature_class), data_type)
    from_fc = "".join((settings.data_set, "/", shp_name))
    to_fc = "".join((settings.data_set, "/", settings.feature_class))
    arcpy.CopyFeatures_management(from_fc, to_fc)
    arcpy.Delete_management(from_fc)

    # arcpy.Copy_management(cad_copy, county_parcel_owner)
    # # Ownership FC no longer needed, delete
    # arcpy.Delete_management(cad_copy)

    # exists = arcpy.Exists(r"city_parcel_owner")
    # if exists:
    #     arcpy.Delete_management(r"city_parcel_owner")
    # arcpy.Clip_analysis(r"county_parcel_owner", r"city_limits", r"city_parcel_owner")
    # with arcpy.da.SearchCursor(r"parcel_city_clipped", original_fields) as cursor:
    #     for row in cursor:
    #         if row[0] is None:
    #             continue
    #         if row[1] is None:
    #             continue
    #         prop_id = int(row[0])
    #         zone_id = int(row[1])
    #         ec_hashmap.set(parcels_hashmap, prop_id, zone_id)

    # Add zone_id to county_par
    # field_exists = ec_arcpy_util.fieldExists(county_parcel_owner, "cad_url")
    # if field_exists is False:
    #     arcpy.AddField_management(county_parcel_owner, "cad_url", "TEXT", "", "", "100", "CAD URL",
    #                               "NULLABLE",
    #                               "NON_REQUIRED")
    #
    #     with arcpy.da.UpdateCursor(county_parcel_owner, parcel_update_fields) as cursor:
    #         for row in cursor:
    #             prop_id = int(row[0])
    #             if prop_id != 0:
    #                 cad_url = "http://search.wharton.manatron.com/details.php?DB_account=R0" + str(
    #                     prop_id) + "&account=R0" + str(prop_id)
    #                 row[1] = cad_url
    #                 cursor.updateRow(row)
    #     arcpy.RegisterAsVersioned_management("Boundary", "EDITS_TO_BASE")
    #     ec_arcpy_util.dbCompress(arcpy.env.workspace)

except Exception as e:
    logging.error(e)
