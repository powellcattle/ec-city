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


def insert_address(_con, _address, _source):
    SQL_INSERT = "INSERT INTO address.address_911" \
                 "(add_number, st_prefix, st_name, st_type, add_unit, add_full, add_source, fuzzy) " \
                 "VALUES (%s,%s,%s,%s,%s,%s,%s,soundex(%s)) " \
                 "ON CONFLICT ON CONSTRAINT address_911_name_idx DO NOTHING "
    try:
        if _con is None:
            logging.debug("Connection is None")
            return

        cur = _con.cursor()
        print _address
        cur.execute(SQL_INSERT, [int(_address.add_number), _address.st_prefix, _address.st_name,
                                 _address.st_type, _address.add_unit, _address.full_name(), _source,
                                 _address.full_name()])

    except psycopg2.DatabaseError as e:
        logging.error(e)
