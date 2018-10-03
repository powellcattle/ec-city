__author__ = 'spowell'
import logging
import os

import arcpy

import ec_arcpy_util
import ec_util

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="new_parcels.log",
                    filemode="w",
                    level=logging.DEBUG,
                    datefmt="%m/%d/%Y %I:%M:%S %p")
county_parcel_owner = r"Boundary\county_parcel_owner"
county_parcel_owner_old = r"Boundary\county_parcel_owner_old"
cad_copy = r"Ownership"

# obtain the SDE workspace based upon the hostname value
arcpy.env.workspace = ec_arcpy_util.sde_workspace_via_host()
logging.debug(arcpy.env.workspace)

parcel_update_fields = ["prop_id", "cad_url"]
# "current_consumption", "size_domain", "reading_status_domain", "reading_date", "active", "meter_number", "occupant",
# "address", "full_incode_account", "compound_meter_domain", "miu"]

try:

    ec_arcpy_util.dbCompress(arcpy.env.workspace)
    # arcpy.RegisterAsVersioned_management("Boundary","EDITS_TO_BASE")
    # edit = arcpy.da.Editor(arcpy.env.workspace)
    # edit.startEditing(with_undo=True, multiuser_mode=True)
    # edit.startOperation()


    # Manage FC names for copy
    county_parcel_exists = arcpy.Exists(county_parcel_owner)
    if county_parcel_exists:
        logging.debug("county_parcel_owner exists")
        old_exists = arcpy.arcpy.Exists(county_parcel_owner_old)
        if old_exists:
            logging.debug("county_parcel_owner_old exists, deleting")
            arcpy.Delete_management(county_parcel_owner_old)
            # edit.stopEditing(True)
            # edit.startEditing()
        logging.debug("renaming county_parcel_owner to county_parcel_owner_old")
        arcpy.Copy_management(county_parcel_owner, county_parcel_owner_old)
        # edit.stopEditing(True)
        # edit.startEditing()
        arcpy.Delete_management(county_parcel_owner)
        # edit.stopEditing(True)
        # edit.startEditing()

    site = r"ftp.bisenterprises.com"
    user = r"WhartonCAD"
    password = r"WHARTONCAD!@34"
    directory = r"/"
    file_name = r"WhartonCad.zip"

    current_path = os.path.dirname("C:\\dev\\temp\\")
    os.chdir(current_path)
    # Commented out for testing only
    ec_util.get_ftp_file(site, user, password, directory, file_name)
    current_path = os.path.dirname("C:\\dev\\temp\\")
    ec_util.unzip_CAD(current_path, file_name)

    # load Parcel Ownership shapefile into SDE
    shapefile_exists = arcpy.Exists(cad_copy)
    if shapefile_exists:
        arcpy.Delete_management(cad_copy)
    arcpy.FeatureClassToGeodatabase_conversion(os.path.join(current_path, "Ownership.shp"), arcpy.env.workspace)
    arcpy.Copy_management(cad_copy, county_parcel_owner)
    # Ownership FC no longer needed, delete
    arcpy.Delete_management(cad_copy)

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
    field_exists = ec_arcpy_util.fieldExists(county_parcel_owner, "cad_url")
    if field_exists is False:
        arcpy.AddField_management(county_parcel_owner, "cad_url", "TEXT", "", "", "100", "CAD URL",
                                  "NULLABLE",
                                  "NON_REQUIRED")

        with arcpy.da.UpdateCursor(county_parcel_owner, parcel_update_fields) as cursor:
            for row in cursor:
                prop_id = int(row[0])
                if prop_id != 0:
                    cad_url = "http://search.wharton.manatron.com/details.php?DB_account=R0" + str(
                        prop_id) + "&account=R0" + str(prop_id)
                    row[1] = cad_url
                    cursor.updateRow(row)
        arcpy.RegisterAsVersioned_management("Boundary", "EDITS_TO_BASE")
        ec_arcpy_util.dbCompress(arcpy.env.workspace)

except:
    print arcpy.GetMessages()


# finally:
# if edit:
# edit.stopEditing(save_changes=True)
