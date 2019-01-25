import phonetics
import re

from nosql import mongo_setup
from nosql.address import MongoAddress

try:
    mongo_setup.global_init()
    pattern = re.compile(r"\s+")
    addresses = MongoAddress.objects()
    for address in addresses:
        address["st_name"] = "SHORT"
        if address["st_name"].isalpha():
            st_name = pattern.sub("",address["st_name"].lower())
            if st_name == "short":
                print("short")
            print(st_name)
            print(f"soundex {phonetics.soundex(st_name,4)}")
            print(f"nysiis {phonetics.nysiis(address['st_name'].lower())}")
            print(f"metaphone {phonetics.metaphone(address['st_name'].lower())}")



    #
    # value = "US 90A".lower()
    # # value = pattern.sub("",value.lower())
    # print(value)
    # dmeta = fuzzy.DMetaphone()
    # result = str(dmeta(value))
    # print(result[0])
    # sdx = fuzzy.Soundex(4)
    # result = sdx(value)
    # print(result)
    # result = fuzzy.nysiis(value)
    # print(result)

except Exception as e:
    print(e)
