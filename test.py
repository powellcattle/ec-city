import random

import arcpy


def generate(_unique: set, _range: int) -> object:
    chars = "ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890abcdefghijklmnopqrstuvwxyz&^%$#@!~"
    while True:
        value: str = "".join(random.choice(chars) for _ in range(_range))
        if value not in unique:
            _unique.add(value)
            return value


unique = set()
# for _ in range(100000):
#     generate(unique)
#
# print(unique[50])
#
# for n in unique:
#     print(n)

# pwid: str = generate(unique, 32)
# print(pwid)
# pwid = generate(unique, 64)
# print(pwid)


fields = ["pwid", "pwname"]
fc_streets = "F:/gis_data/PubWorks/streets.shp"
with arcpy.da.UpdateCursor(fc_streets, fields) as cursor:
    for row in cursor:
        pwid: str = generate(unique, 32)
        row[0] = pwid
        pwname: str = generate(unique, 64)
        row[1] = pwname
        cursor.updateRow(row)

fc_meter = "F:/gis_data/PubWorks/meter.shp"
with arcpy.da.UpdateCursor(fc_meter, fields) as cursor:
    for row in cursor:
        pwid: str = generate(unique, 32)
        row[0] = pwid
        pwname: str = generate(unique, 64)
        row[1] = pwname
        cursor.updateRow(row)

sanitary_manholes = "F:/gis_data/PubWorks/sanitary_manholes.shp"
with arcpy.da.UpdateCursor(sanitary_manholes, fields) as cursor:
    for row in cursor:
        pwid: str = generate(unique, 32)
        row[0] = pwid
        pwname: str = generate(unique, 64)
        row[1] = pwname
        cursor.updateRow(row)
