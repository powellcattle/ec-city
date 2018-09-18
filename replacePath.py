import os

import arcpy

arcpy.env.overwriteOutput = True
print arcpy.GetMessages()

oldPath = [r"C:\Users\charris\AppData\Roaming\ESRI\Desktop10.2\ArcCatalog\Connection to maps.cityofelcampo.org.sde",
           r"C:\Users\cpriesmeyer\AppData\Roaming\ESRI\Desktop10.2\ArcCatalog\maps.cityofelcampo.org"]
newPath = r"\\10.140.1.14\gis\sde\dev.cityofelcampo.org.sde"
mxdPath = r"\\10.140.1.14\gis\Maps"
saveMxdPath = r"\\10.140.1.14\gis\Maps\source_agnostic"

try:
    # Loop through each MXD file
    for filename in os.listdir(mxdPath):
        fullpath = os.path.join(mxdPath, filename)
        if os.path.isfile(fullpath):
            if filename.lower().endswith(".mxd"):
                # Reference MXD
                mxd = arcpy.mapping.MapDocument(fullpath)
                arcpy.AddMessage("Changing " + mxd.filePath)
                print "Changing " + mxd.filePath
                for changePath in oldPath:
                    mxd.findAndReplaceWorkspacePaths(changePath, newPath, False)
                mxd.saveACopy(os.path.join(saveMxdPath, filename))
                del (mxd)
        del (fullpath)
        del (filename)
    print "Done"

except Exception, e:
    import traceback

    map(arcpy.AddError, traceback.format_exc().split("\n"))
    arcpy.AddError(str(e))
