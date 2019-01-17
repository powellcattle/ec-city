import mongoengine
from pyproj import Proj, transform

from nosql.location import Location
from nosql.name_alias import NameAlias

proj_2278 = Proj(init='EPSG:2278', preserve_units=True)
proj_4326 = Proj(init='EPSG:4326')


class MongoAddress(mongoengine.Document):
    ad_block = mongoengine.IntField(required=False, null=False)
    st_prefix = mongoengine.StringField(required=False, null=False)
    st_name = mongoengine.StringField(required=True, null=False)
    st_name_aliases = mongoengine.EmbeddedDocumentListField(NameAlias, null=False)
    st_type = mongoengine.StringField(required=False, null=False)
    st_city = mongoengine.StringField(required=False)
    st_zip = mongoengine.StringField(required=False)
    st_name_full = mongoengine.StringField(required=False)
    ad_name_full = mongoengine.StringField(required=False)
    locations = mongoengine.EmbeddedDocumentListField(Location)

    meta = {
        "db_alias": "core",
        'all_inheritance': True,
        "collection": "address",
    }

    def create(self, address_dict: dict, x1: float, y1: float):
        self.append_location(address_dict, x1, y1)

        if address_dict["st_prefix"]:
            self.st_prefix = address_dict["st_prefix"]
        if address_dict["add_number"]:
            self.ad_block = address_dict["add_number"]
        if address_dict["st_name"]:
            self.st_name = address_dict["st_name"]
        if address_dict["st_type"]:
            self.st_type = address_dict["st_type"]
        if address_dict["city"]:
            self.st_city = address_dict["city"]
        if address_dict["zip"]:
            self.st_zip = address_dict["zip"]
        if address_dict["st_full_name"]:
            self.st_name_full = address_dict["st_full_name"]
        if address_dict["add_address"]:
            self.ad_name_full = address_dict["add_address"]

    def append_location(self, address_dict: dict, x1: float, y1: float):
        location = Location()
        if address_dict["source"]:
            location.source = address_dict['source']
        x2, y2 = transform(proj_2278, proj_4326, x1, y1)
        location.coords = (x1, y1), (x2, y2)
        self.locations.append(location)
