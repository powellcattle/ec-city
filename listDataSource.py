# Author:  Paul Frank, City of Austin
# Date:    November 7, 2012
# Version: ArcGIS 10.0
# Purpose:  This script will iterate through each MXD in a folder and change the SDE connection and dataset name.
#           Then it will save the mxd that it has finished changing.  The script is intended to run from a script
#           tool that requires a parameter with the folder containing MXDs.

#setting variables
import arcpy, os
arcpy.env.overwriteOutput = True
print arcpy.GetMessages()

#setting variables for new data source and table that lists the old and new dataset names
newdatasource = r"C:\Users\spowell\AppData\Roaming\ESRI\Desktop10.3\ArcCatalog\dev.cityofelcampo.org.sde"
#Read input parameters from GP dialog
#folderPath = arcpy.GetParameterAsText(0)
folderPath = r"\\10.140.1.14\gis\Maps\Eva"

try:
    #Loop through each MXD file
    for filename in os.listdir(folderPath):
        print filename
        fullpath = os.path.join(folderPath, filename)
        if os.path.isfile(fullpath):
            if filename.lower().endswith(".mxd"):
                #Reference MXD
                mxd = arcpy.mapping.MapDocument(fullpath)
                arcpy.AddMessage("Changing " + mxd.filePath)
                print "changing " + mxd.filePath
                #Reference each data frame and report data
                DFList = arcpy.mapping.ListDataFrames(mxd)
                for df in DFList:
                    #Reference each layer in a data frame
                    lyrs = arcpy.mapping.ListLayers(mxd, "", df)
                    for lyr in lyrs:
                        if lyr.isFeatureLayer and 'sde' in lyr.dataSource:
                            dSource = str(lyr.dataSource)
                            fcName = dSource.split('.sde\\')
                            print "Layer Name: " + lyr.name + " Workspace Path: " + fcName[0] + " Feature Class: " + fcName[1]
                            print

                #arcpy.AddMessage("Finished replacing " + filename)
                #mxd.save()
                #print "saved copy of mxd"
    del mxd
    del row
    del rows
    del folderPath, fullpath
    print "done"
except Exception, e:
    import traceback
    map(arcpy.AddError, traceback.format_exc().split("\n"))
    arcpy.AddError(str(e))

