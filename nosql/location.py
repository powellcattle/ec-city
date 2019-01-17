import mongoengine
from nosql.coordinate import Coordinate

class Location(mongoengine.EmbeddedDocument):
    source = mongoengine.StringField(required=True, null=False)
    coords = mongoengine.MultiPointField()
