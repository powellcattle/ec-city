from pyproj import Proj, transform

from nonsql import mongo_setup
from nonsql.address import MongoAddress

mongo_setup.global_init()


mongo_address = MongoAddress.objects(ad_name_full="107 E CALHOUN ST").first()
if mongo_address:
    print(mongo_address.st_name)
