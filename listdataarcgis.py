__author__ = 'spowell'
import csv

import arcpy


MSG_ERROR_CREATING_POINT = "Error creating point %s on feature %s"
arcpy.env.workspace = "C:\\Users\\spowell\\AppData\\Roaming\\ESRI\\Desktop10.3\\ArcCatalog\\powellcattle.com.sde"
LAYER_NAME = "light_test"


def create_point(x, y, spatialReference):
    """
    Creates a 2D point with an associated
    spatial reference
    :rtype : arcpy.PointGeometry
    """
    try:
        point = arcpy.Point(x, y)
        return arcpy.PointGeometry(point, spatialReference)
    except:
        raise Exception, MSG_ERROR_CREATING_POINT

try:
    sr4326 = arcpy.SpatialReference(4326)
    sr2278 = arcpy.SpatialReference(2278)
    arcpy.Delete_management(arcpy.env.workspace + "\\" + LAYER_NAME)
    fc = arcpy.CreateFeatureclass_management(arcpy.env.workspace, LAYER_NAME, 'POINT', "", "", "", sr2278)
    arcpy.AddField_management(LAYER_NAME, "owner", "TEXT", "", "", 3, "Owner")
    arcpy.AddField_management(LAYER_NAME, "application", "TEXT", "", "", 40, "Application")
    arcpy.AddField_management(LAYER_NAME, "bulb", "TEXT", "", "", 25, "Bulb")
    arcpy.AddField_management(LAYER_NAME, "lumen", "LONG", "", "", "", "Lumen")
    arcpy.AddField_management(LAYER_NAME, "watts", "LONG", "", "", "", "Watts")

    rows = arcpy.InsertCursor(arcpy.env.workspace + "\\" + LAYER_NAME)

    with open('../data/lights2012.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
        next(reader, None)
        for record in reader:
            watts = record[0]
            lumen = record[1]
            bulb = record[3].upper()
            application = record[5].upper()
            contract = record[6].upper()
            row = rows.newRow()
            row.setValue("owner","AEP")
            row.setValue("application",application)
            row.setValue("bulb",bulb)
            row.setValue("lumen",lumen)
            row.setValue("watts",watts)
            ptGeometry = create_point(record[14], record[13], sr4326)
            row.setValue("shape",ptGeometry)
            rows.insertRow(row)
        del row
    del rows

except Exception,  e:
    print 'Error %s' % e

finally:
    if csvfile:
        csvfile.close()