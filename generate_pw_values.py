import random
from typing import Set

import arcpy


def generate(_unique: set, _range: int) -> object:
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrstuvwxyz&^%$#@!~"
    while True:
        value: str = "".join(random.choice(chars) for _ in range(_range))
        if value not in string_set:
            _unique.add(value)
            return value


# Create set
string_set: Set[str] = set()

fields = ["pwid", "pwname"]

# street load to PubWorks
fc_streets = "F:/gis_data/PubWorks/streets.shp"

with arcpy.da.SearchCursor(fc_streets, ["fid", "st_name"]) as cursor_read:
    for row in cursor_read:
        where_clause: str = "fid=" + str(row[0])
        pwid: str = "pw" + str(row[0])
        pwname: str = ''
        if '' == row[1] or ' ' == row[1]:
            pwname = str(row[0]) + "-UKNOWN"
        else:
            pwname = str(row[0]) + "-" + row[1]

        with arcpy.da.UpdateCursor(fc_streets, fields, where_clause) as cursor:
            for row in cursor:
                row[0] = pwid
                row[1] = pwname
                cursor.updateRow(row)

fc_meter = "F:/gis_data/PubWorks/meter.shp"

with arcpy.da.SearchCursor(fc_meter, ["fid", "full_incod"]) as cursor_read_meter:
    for row in cursor_read_meter:
        where_clause: str = "fid=" + str(row[0])
        pwid: str = "pw" + str(row[0])
        pwname: str = ''
        if '' == row[1] or ' ' == row[1]:
            pwname = str(row[0]) + "-UKNOWN"
        else:
            pwname = row[1]

        with arcpy.da.UpdateCursor(fc_meter, fields, where_clause) as cursor_meter:
            for row in cursor_meter:
                row[0] = pwid
                row[1] = pwname
                cursor_meter.updateRow(row)

fc_manhole = "f:/gis_data/PubWorks/sanitary_manholes.shp"
with arcpy.da.SearchCursor(fc_manhole, ["fid", "identifier"]) as cursor_read_manhole:
    for row in cursor_read_manhole:
        where_clause: str = "fid=" + str(row[0])
        pwid: str = "pw" + str(row[0])
        pwname: str = ''
        if '' == row[1] or ' ' == row[1]:
            pwname = str(row[0]) + "-UKNOWN"
        else:
            pwname = str(row[0]) + "-" + row[1]

        with arcpy.da.UpdateCursor(fc_manhole, fields, where_clause) as cursor_manhole:
            for row in cursor_manhole:
                row[0] = pwid
                row[1] = pwname
                cursor_manhole.updateRow(row)
