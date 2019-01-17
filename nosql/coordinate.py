import mongoengine

class Coordinate(mongoengine.EmbeddedDocument):
    epsg = mongoengine.StringField(required=True, default='EPSG:2278')
    coordinate = mongoengine.PointField(required=True)
