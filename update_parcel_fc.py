import inspect
import logging
# import os
import pathlib
import re
import zipfile
from ftplib import FTP

import arcpy
import yaml
from pyproj import Proj, transform

import ec_arcpy_util
import openlocationcode

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="update_parcel.log",
                    filemode="w",
                    level=logging.ERROR,
                    datefmt="%m/%d/%Y %I:%M:%S %p")
#todo
#make specific module to standalone
#provide CLI

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
    # todo
    # integrate pathlib instead of os
    # better error checking with custom Exceptions
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


def find_parcel_shape(_relative_path: str, _find_file: str, _filter: str = "*.shp") -> dict:
    p = pathlib.Path(_relative_path)
    for _ in p.rglob(_filter):
        if re.search(_find_file, _.name, re.IGNORECASE):
            return {"path": pathlib.PureWindowsPath(_.resolve()).as_posix(), "file": _.name.split(".")[0]}


parcel_update_fields = ["prop_id", "cad_url"]
previous = "_previous"
data_type = "FeatureClass"
proj_2278 = Proj(init='EPSG:2278', preserve_units=True)
proj_4326 = Proj(init='EPSG:4326')

try:
    settings = Settings(yml_file)
    if not __debug__:
        # must start Python with - o to use
        download_parcel_ftp(settings)
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
        from_fc = "".join((settings.data_set, "/", settings.feature_class))
        to_fc = "".join((settings.data_set, "/", settings.feature_class, previous))
        # arcpy.Rename_management(from_fc, to_fc, data_type)
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
    # Can not use rename because of a severe bug
    to_fc = "".join((settings.data_set, "/", settings.feature_class))
    from_fc = "".join((settings.data_set, "/", shp_name))
    # arcpy.Rename_management(from_fc, to_fc, data_type)
    arcpy.CopyFeatures_management(from_fc, to_fc)
    arcpy.Delete_management(from_fc)

    arcpy.AddField_management(to_fc, "plus_code", "TEXT", "", "", "11", "PLUS CODE", "NULLABLE", "NON_REQUIRED")
    arcpy.AddField_management(to_fc, "cad_url", "TEXT", "", "", "100", "CAD URL", "NULLABLE", "NON_REQUIRED")
    fields = ["prop_id", "plus_code", "cad_url", "SHAPE@XY"]

    with arcpy.UpdateCursor(to_fc, fields) as cursor:
        for row in cursor:
            if row[0] is None:
                continue

            prop_id = int(row[0])
            cad_url = f"http://search.wharton.manatron.com/details.php?DB_account=R0{str(prop_id)}&account=R0{str(prop_id)}"
            row[2] = cad_url

            # check for valid x,y tuple
            if row[3] is None or row[3][0] is None or row[3][1] is None:
                continue

            x, y = row[3]
            # convert from State Plane to lat/lng
            x, y = transform(proj_2278, proj_4326, x, y)
            plus_code = openlocationcode.encode(y, x)
            row[1] = plus_code
            cursor.updateRow(row)

    arcpy.RegisterAsVersioned_management(ds, "EDITS_TO_BASE")
    ec_arcpy_util.dbCompress(arcpy.env.workspace)
    logging.info("Successful completion")

except Exception as e:
    logging.error(f"{inspect.stack()[0][3]} {e}")
