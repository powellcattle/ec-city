import ec_arcpy_util
import arcpy
import logging

__author__ = 'spowell'

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s",
                    filename="copy_street_fields.log",
                    filemode="w",
                    level=logging.DEBUG,
                    datefmt="%m/%d/%Y %I:%M:%S %p")


def update_fields():
    try:
        workspace = ec_arcpy_util.sde_workspace_via_host()

        arcpy.env.workspace = workspace
        ds = ec_arcpy_util.find_dataset("*Streets")
        fc = ec_arcpy_util.find_feature_class("*dt_streets", "Streets")

        #
        # arcgis stuff for multi-users
        arcpy.AcceptConnections(workspace, False)
        arcpy.DisconnectUser(workspace, "ALL")
        if not arcpy.Describe(ds).isVersioned:
            arcpy.RegisterAsVersioned_management(ds, "EDITS_TO_BASE")

        fields = arcpy.ListFields(fc)
        is_schema_updated = False
        for field in fields:
            if "pwid" == field.name:
                is_schema_updated = True
                break

        if not is_schema_updated:
            arcpy.AddField_management("dt_streets", "pwid", "TEXT", field_length=32, field_alias="PubWorks ID", field_is_nullable="NULLABLE")
            arcpy.AddField_management("dt_streets", "pwname", "TEXT", field_length=64, field_alias="PubWorks Name", field_is_nullable="NULLABLE")
            arcpy.AddField_management("dt_streets", "st_fullname", "TEXT", field_length=50, field_alias="Street Full Name", field_is_nullable="NULLABLE")
            arcpy.AddField_management("dt_streets", "from_addr_l", "LONG", field_alias="From/Left #", field_is_nullable="NULLABLE")
            arcpy.AddField_management("dt_streets", "to_addr_l", "LONG", field_alias="To/Left #", field_is_nullable="NULLABLE")
            arcpy.AddField_management("dt_streets", "from_addr_r", "LONG", field_alias="From/Right #", field_is_nullable="NULLABLE")
            arcpy.AddField_management("dt_streets", "to_addr_r", "LONG", field_alias="To/Right #", field_is_nullable="NULLABLE")



















    except Exception as e:
        print(e)
        logging.error("update_fields():{}".format(e))

    finally:
        logging.info("done.")


if __name__ == '__main__':
    # execute only if run as the entry point into the program
    update_fields()
