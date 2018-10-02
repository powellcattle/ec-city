__author__ = 'spowell'
import psycopg2
import logging
import socket
import ec_addresses


def psql_connection(_database=r"elc", _user=r"sde", _password=r"sde", _host=r"localhost", _port=r"5433",
                    _read_only=False):
    database = _database
    user = _user
    password = _password
    host = _host
    port = _port
    db = None
    if socket.gethostname() == "gis-development":
        database = r"ec"
    if socket.gethostname() == "gis":
        port = r"5432"
    if socket.gethostname() == "home-gis":
        port = r"5432"
        host = r"localhost"
        database = r"ec"
    try:
        db = psycopg2.connect(database=database,
                              user=user,
                              password=password,
                              host=host,
                              port=port)
        db.set_session(readonly=_read_only, autocommit=False)

    except psycopg2.Error as e:
        logging.error(e)
        return None

    return db



