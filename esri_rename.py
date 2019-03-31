import inspect

import arcpy

try:
    arcpy.env.workspace = r"C:\Users\spowell\AppData\Roaming\ESRI\Desktop10.6\ArcCatalog\localhost.sde"
    #
    # arcgis stuff for multi-users
    arcpy.AcceptConnections(arcpy.env.workspace, False)
    arcpy.DisconnectUser(arcpy.env.workspace, "ALL")
    arcpy.Delete_management("boundary/county_parcel_1")
    arcpy.Rename_management("boundary/county_parcel", "boundary/county_parcel_1", "FeatureClass")
    arcpy.Rename_management("boundary/county_parcel_1", "boundary/county_parcel", "FeatureClass")

except Exception as e:
    print(f"{inspect.stack()[0][3]} {e}")
