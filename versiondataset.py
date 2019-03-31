__author__ = "spowell"
import arcpy

try:
    data_set = arcpy.GetParameterAsText(0)
    is_compress_db = False
    if arcpy.GetArgumentCount() > 1:
        is_compress_db = arcpy.GetParameterAsText(1)
    work_space = arcpy.env["scriptWorkspace"]
    assert work_space

    data_set_desc = arcpy.Describe(data_set)
    if not data_set_desc.isVersioned:
        arcpy.AddMessage(f"Disallowing new connections.")
        arcpy.AcceptConnections(work_space, False)
        arcpy.AddMessage(f"Disconnecting all Users.")
        arcpy.DisconnectUser(work_space, "ALL")
        arcpy.AddMessage(f"Registering {data_set} as Versioned.")
        arcpy.RegisterAsVersioned_management(data_set, "EDITS_TO_BASE")
        if is_compress_db:
            arcpy.AddMessage(f"Compressing geodatabase, this could take some time.")
            arcpy.Compress_management(work_space)
        arcpy.AddMessage(f"Accepting User Connections.")
        arcpy.AcceptConnections(work_space, True)
    else:
        arcpy.AddMessage(f"Nothing to version, {data_set} is already registered as versioned.")

except Exception as e:
    arcpy.AddError(f"Problem in the script.\n {e}")
