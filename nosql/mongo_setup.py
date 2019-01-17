import mongoengine


def global_init():
    mongoengine.register_connection(alias="core", name="address",host="192.168.1.130")
