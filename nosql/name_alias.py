import mongoengine

class NameAlias(mongoengine.EmbeddedDocument):
    name = mongoengine.StringField(required=True, null=False)
