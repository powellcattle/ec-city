#!/usr/bin/python
# -*- coding: utf-8 -*-

__author__ = 'spowell'


import csv
import logging
import os
import sys

import arcpy
import psycopg2

import ec_arcpy_util
import ec_hashmap
import ec_psql_util
import ec_util

class Address:
    add_number = None
    st_prefix = None
    st_name = None
    st_type = None
    st_suffix = None
    add_unit = None
    add_zip = None
    add_city = None
    unit_des_single = ['APT', 'FRNT', 'LBBY', 'LOWR', 'OFC', 'PH', 'REAR', 'SIDE', 'UPPR']
    unit_des_second = ['APT', 'BLDG', 'DEPT', 'FL', 'HNGR', 'KEY', 'LOT', 'PIER', 'RM' 'SLIP', 'SPC', 'STOP', 'STE',
                       'TRLR',
                       'UNIT']

    def __init__(self, add_number, st_prefix, st_name, st_type, st_suffix, add_unit, add_city, add_zip):
        if add_number:
            self.add_number = int(add_number)
        if st_prefix:
            self.st_prefix = str(st_prefix).strip().upper()
        if st_name:
            self.st_name = str(st_name).strip().upper()
        if st_type:
            self.st_type = str(st_type).strip().upper()
        if st_suffix:
            self.st_suffix = str(st_suffix).strip().upper()
        if add_unit:
            self.add_unit = str(add_unit).strip().upper()
            for sgl in self.unit_des_single:
                if sgl in self.add_unit:
                    self.add_unit = sgl
                    return
            for dbl in self.unit_des_second:
                if dbl in self.add_unit:
                    if len(self.add_unit.split(" ")) < 2:
                        self.add_unit = 'BAD DESIGNATOR'
                    else:
                        self.add_unit = dbl + ' ' + self.add_unit.split(" ", 2)[1]
                    return
            self.add_unit = ' # ' + self.add_unit
        if add_city:
            self.add_city = str(add_city).strip().upper()
        if add_zip:
            self.add_zip = str(add_zip).strip()

    def is_valid(self):
        if not self.add_number:
            return False
        if not self.st_name:
            return False

        return True

    def __str__(self):
        to_str = None

        if self.add_number:
            to_str = str(self.add_number)

        if self.st_prefix:
            if not to_str:
                to_str = "{}".format(self.st_prefix)
            else:
                to_str = "{} {}".format(to_str, self.st_prefix)

        if self.st_name:
            if not to_str:
                to_str = "{}".format(self.st_name)
            else:
                to_str = "{} {}".format(to_str, self.st_name)

        if self.st_type:
            if not to_str:
                to_str = "{}".format(self.st_type)
            else:
                to_str = "{} {}".format(to_str, self.st_type)

        if self.add_unit:
            if not to_str:
                to_str = "{}".format(self.add_unit)
            else:
                to_str = "{} {}".format(to_str, self.add_unit)

        return (to_str)

    def full_name(self):
        _tmp_full = None
        if self.add_number is None:
            return None
        else:
            _tmp_full = str(self.add_number)

        if self.st_prefix is not None:
            _tmp_full = _tmp_full + ' ' + str(self.st_prefix)

        if self.st_name is not None:
            _tmp_full = _tmp_full + ' ' + str(self.st_name)

        if self.st_type is not None:
            _tmp_full = _tmp_full + ' ' + str(self.st_type)

        if self.add_unit is not None:
            _tmp_full = _tmp_full + ' ' + str(self.add_unit)

        return _tmp_full


def insert_address(_con, _address, _source, _sql_insert):

    try:
        if _con is None:
            logging.debug("Connection is None")
            return

        cur = _con.cursor()
        cur.execute(_sql_insert, [
            _address.add_number,
            _address.st_prefix,
            _address.st_name,
            _address.st_suffix,
            _address.st_type,
            _address.add_unit,
            _address.full_name(),
            _source,
            _address.add_zip,
            _address.add_city,
            _address.full_name()])

    except psycopg2.DatabaseError as e:
        logging.error(e)


def setup_e911_addresses_tables(_con):
    SQL_DROP_ADDRESSES_911 = "DROP TABLE IF EXISTS address.address_911"
    SQL_CREATE_ADDRESSES_911 = "CREATE TABLE address.address_911(" \
                               "address_911_id SERIAL4 NOT NULL, " \
                               "add_number INT NOT NULL, " \
                               "st_prefix CHARACTER(9) NULL, " \
                               "st_name VARCHAR(100) NOT NULL, " \
                               "st_suffix VARCHAR(5) NULL, " \
                               "st_type VARCHAR(10) NULL, " \
                               "add_unit VARCHAR(20) NULL, " \
                               "add_full VARCHAR(50) NOT NULL, " \
                               "add_source VARCHAR(20) NOT NULL, " \
                               "add_zip CHARACTER(5) NULL, " \
                               "add_city VARCHAR(25) NULL, " \
                               "fuzzy CHARACTER(4) NULL, " \
                               "CONSTRAINT unique_address_911_pkey PRIMARY KEY (address_911_id), " \
                               "CONSTRAINT address_911_name_idx UNIQUE (add_full, add_unit))"

    try:
        cur = _con.cursor()
        cur.execute(SQL_DROP_ADDRESSES_911)
        cur.execute(SQL_CREATE_ADDRESSES_911)

    except psycopg2.DatabaseError as e:
        if _con:
            _con.rollback()
        logging.error(e)

    except:
        logging.error(sys.exc_info()[1])

    finally:
        if _con:
            _con.commit()
            _con.close()


def setup_addresses_incode_table(_con):
    SQL_DROP_ADDRESSES_INCODE = "DROP TABLE IF EXISTS address.address_incode"
    SQL_CREATE_ADDRESSES_INCODE = "CREATE TABLE address.address_incode(" \
                               "address_incode_id SERIAL4 NOT NULL, " \
                               "add_number INT NOT NULL, " \
                               "st_prefix VARCHAR(5) NULL, " \
                               "st_name VARCHAR(100) NOT NULL, " \
                               "st_suffix VARCHAR(5) NULL, " \
                               "st_type VARCHAR(10) NULL, " \
                               "add_unit VARCHAR(20) NULL, " \
                               "add_full VARCHAR(50) NOT NULL, " \
                               "add_source VARCHAR(20) NOT NULL, " \
                               "add_zip CHARACTER(5) NULL, " \
                               "add_city VARCHAR(25) NULL, " \
                               "fuzzy CHARACTER(4) NULL, " \
                               "CONSTRAINT unique_address_incode_pkey PRIMARY KEY (address_incode_id), " \
                               "CONSTRAINT address_incode_name_idx UNIQUE (add_full, add_unit))"

    try:
        cur = _con.cursor()
        cur.execute(SQL_DROP_ADDRESSES_INCODE)
        cur.execute(SQL_CREATE_ADDRESSES_INCODE)

    except psycopg2.DatabaseError as e:
        if _con:
            _con.rollback()
        logging.error(e)

    except:
        logging.error(sys.exc_info()[1])

    finally:
        if _con:
            _con.commit()
            _con.close()

def insert_incode(_con, _address, _source):
    SQL_INSERT_ADDRESSES_911 = "INSERT INTO address.address_911(add_number, st_name, st_suffix, st_type, add_unit, add_full, add_source, add_zip, add_city, fuzzy) " \
                               "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,soundex(%s)) ON CONFLICT ON " \
                               "CONSTRAINT address_911_name_idx DO NOTHING"
    try:
        if _con is None:
            logging.debug("Connection is None")
            return

        cur = _con.cursor()
        cur.execute(SQL_INSERT_ADDRESSES_911, [_address.add_number, _address.st_name, _address.st_suffix, _address.st_type,
                                 _address.add_unit, _address.full_name(), _source, _address.add_zip, _address.add_city,
                                 _address.full_name()])

    except psycopg2.DatabaseError as e:
        logging.error(e)


def setup_addresses_E911_table(_con):
    SQL_DROP_ADDRESSES_911 = "DROP TABLE IF EXISTS address.address_911"
    SQL_CREATE_ADDRESSES_911 = "CREATE TABLE address.address_911(" \
                               "address_911_id SERIAL4 NOT NULL, " \
                               "add_number INT NOT NULL, " \
                               "st_prefix CHARACTER(1) NULL, " \
                               "st_name VARCHAR(100) NOT NULL, " \
                               "st_suffix VARCHAR(5) NULL, " \
                               "st_type VARCHAR(10) NULL, " \
                               "add_unit VARCHAR(20) NULL, " \
                               "add_full VARCHAR(50) NOT NULL, " \
                               "add_source VARCHAR(20) NOT NULL, " \
                               "add_zip CHARACTER(5) NULL, " \
                               "add_city VARCHAR(25) NULL, " \
                               "fuzzy CHARACTER(4) NULL, " \
                               "CONSTRAINT unique_address_911_pkey PRIMARY KEY (address_911_id), " \
                               "CONSTRAINT address_911_name_idx UNIQUE (add_full, add_unit))"

    try:
        cur = _con.cursor()
        cur.execute(SQL_DROP_ADDRESSES_911)
        cur.execute(SQL_CREATE_ADDRESSES_911)

    except psycopg2.DatabaseError as e:
        if _con:
            _con.rollback()
        logging.error(e)

    except:
        logging.error(sys.exc_info()[1])

    finally:
        if _con:
            _con.commit()
            _con.close()

def load_starmap_streets(_from_workspace, _cleanup):
    edit = None

    try:
        #
        # arcgis stuff for multi-users
        to_workspace = ec_arcpy_util.sde_workspace_via_host()
        arcpy.AcceptConnections(to_workspace, False)
        arcpy.DisconnectUser(to_workspace, "ALL")
        arcpy.env.workspace = to_workspace
        #
        # if first time, create featuredataset HGAC
        ds = ec_arcpy_util.find_dataset("*HGAC")
        sr_2278 = arcpy.SpatialReference(2278)
        if ds is None:
            ds = arcpy.CreateFeatureDataset_management(to_workspace, "HGAC", sr_2278)
        #
        # if feature class exists, delete it
        fc = ec_arcpy_util.find_feature_class("*starmap","HGAC")
        if fc:
            arcpy.Delete_management("starmap")
        #
        # Define Fields for starmap featureclass
        fc = arcpy.CreateFeatureclass_management(to_workspace + r"\HGAC", "starmap", "POLYLINE", "", "", "", sr_2278)
        arcpy.AddField_management(fc, "pwid", "TEXT", "", "", 20, "PubWorks Id", "NON_NULLABLE")
        arcpy.AddField_management(fc, "pwname", "TEXT", "", "", 50, "PubWorks Name", "NON_NULLABLE")
        arcpy.AddField_management(fc, "st_predir", "TEXT", "", "", 9, "Street Prefix", "NULLABLE")
        arcpy.AddField_management(fc, "st_name", "TEXT", "", "", 50, "Street Name", "NULLABLE")
        arcpy.AddField_management(fc, "st_fullname", "TEXT", "", "", 50, "Full Street Name", "NULLABLE")
        arcpy.AddField_management(fc, "st_type", "TEXT", "", "", 4, "Street Type", "NULLABLE")
        arcpy.AddField_management(fc, "from_addr_l", "LONG", "", "", "", "From Left Block #", "NON_NULLABLE")
        arcpy.AddField_management(fc, "to_addr_l", "LONG", "", "", "", "To Left Block #", "NON_NULLABLE")
        arcpy.AddField_management(fc, "from_addr_r", "LONG", "", "", "", "From Right Block #", "NON_NULLABLE")
        arcpy.AddField_management(fc, "to_addr_r", "LONG", "", "", "", "To Right Block #", "NON_NULLABLE")
        arcpy.AddField_management(fc, "source", "TEXT", "", "", 40, "Data Source", "NON_NULLABLE")
        arcpy.AddField_management(fc, "global_id", "GUID", "", "", 10, "Global ID", "NON_NULLABLE")
        fields_starmap = ["pwid", "pwname", "st_predir", "st_name", "st_fullname", "st_type", "from_addr_l", "to_addr_l", "from_addr_r","to_addr_r", "source", "global_id", "SHAPE@"]

        if not arcpy.Describe(ds).isVersioned:
            arcpy.RegisterAsVersioned_management(ds, "EDITS_TO_BASE")
        edit = arcpy.da.Editor(to_workspace)
        edit.startEditing(with_undo=False, multiuser_mode=True)
        edit.startOperation()

        insert_cursor = arcpy.da.InsertCursor("starmap", fields_starmap)
        from_fields = ["St_PreDir","StreetName", "Full_Name", "ST_POSTYP", "FromAddr_L", "ToAddr_L", "FromAddr_R", "ToAddr_R","SOURCE", "GLOBALID", "SHAPE@", "OBJECTID"]
        from_fc = _from_workspace + os.sep + "hgac_starmap"

        blocks = []
        counter = 0
        with arcpy.da.SearchCursor(from_fc, from_fields, sql_clause=(None,"ORDER BY STREETNAME, FromAddr_L, FromAddr_R")) as cursor:
            for row in cursor:
                str_predir = ec_util.to_upper_or_none(row[0])
                name = ec_util.to_upper_or_none(row[1])
                if not name:
                    continue

                full_name = ec_util.to_upper_or_none(row[2])
                st_type = ec_util.to_upper_or_none(row[3])
                from_addr_l = ec_util.to_pos_int_or_none(row[4])
                to_addr_l = ec_util.to_pos_int_or_none(row[5])
                from_addr_r = ec_util.to_pos_int_or_none(row[6])
                to_addr_r = ec_util.to_pos_int_or_none(row[7])
                if (len(str(counter)) + len(name)) > 19:
                      pwid = name[0:(19-len(str(counter)))] + "-" + str(counter)
                else:
                    pwid = name + "-"  + str(counter)
                counter = counter + 1

                if from_addr_l:
                    blocks.append(from_addr_l)
                if to_addr_l:
                    blocks.append(to_addr_l)
                if from_addr_r:
                    blocks.append(from_addr_r)
                if to_addr_r:
                    blocks.append(to_addr_r)
                if len(blocks) > 2:
                    pwname = str(min(blocks)) + "-" + str(max(blocks)) + " " + full_name
                else:
                    pwname = full_name
                blocks = []

                source = ec_util.to_upper_or_none(row[8])
                global_id = row[9]
                shape = row[10]
                insert_cursor.insertRow((pwid, pwname, str_predir, name, full_name, st_type, from_addr_l, to_addr_l, from_addr_r, to_addr_r, source, global_id, shape))

    except psycopg2.DatabaseError as e:
        logging.error(e)

    except:
        logging.error(arcpy.GetMessages())
        logging.error(sys.exc_info()[1])

    finally:
        if edit:
            edit.stopOperation()
            logging.info("Saving changes for HGAC Starmap import")
            edit.stopEditing(save_changes=True)


def load_unique_street_types():
    con = None
    SQL_INSERT = "INSERT INTO address.unique_street_types(street_type) VALUES (%s)"
    SQL_DROP = "DROP TABLE IF EXISTS address.unique_street_types CASCADE"
    SQL_CREATE = "CREATE TABLE address.unique_street_types(" \
                 "id SERIAL NOT NULL," \
                 "street_type VARCHAR(100) NOT NULL," \
                 "CONSTRAINT unique_street_types_pkey PRIMARY KEY (id))"

    reader = None

    try:
        con = ec_psql_util.psql_connection(_database="ec", _host="localhost", _user="sde", _password="sde", _port=5432,
                                           _read_only=False)

        cur = con.cursor()
        cur.execute(SQL_DROP)
        con.commit()
        cur.execute(SQL_CREATE)
        con.commit()

        reader = None

        with open('./data/address/unique_street_types.csv', 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
            next(reader, None)
            for row in reader:
                street_type = str(row[0])
                cur.execute(SQL_INSERT, [street_type])
                con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    except OSError as e:
        logging.error(e)

    except IOError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def load_unique_street_type_aliases():
    con = None
    SQL_INSERT = "INSERT INTO address.unique_street_type_aliases(prefix,alias) VALUES (%s,%s)"
    SQL_DROP = "DROP TABLE IF EXISTS address.unique_street_type_aliases CASCADE"
    SQL_CREATE = "CREATE TABLE address.unique_street_type_aliases(" \
                 "prefix VARCHAR(100) NOT NULL," \
                 "alias VARCHAR(100) NOT NULL," \
                 "CONSTRAINT unique_street_type_aliases_pkey PRIMARY KEY (prefix,alias))"

    reader = None

    try:
        con = ec_psql_util.psql_connection(_database="ec", _host="localhost", _user="sde", _password="sde", _port=5432,
                                           _read_only=False)

        cur = con.cursor()
        cur.execute(SQL_DROP)
        con.commit()
        cur.execute(SQL_CREATE)
        con.commit()

        reader = None

        with open('./data/address/unique_street_type_aliases.csv', 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
            next(reader, None)
            for row in reader:
                prefix = str(row[0])
                alias = str(row[1])
                cur.execute(SQL_INSERT, (prefix, alias))
                con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    except OSError as e:
        logging.error(e)

    except IOError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def load_unique_prefix_aliases():
    con = None
    SQL_INSERT = "INSERT INTO address.unique_prefix_aliases(prefix,alias) VALUES (%s,%s)"
    SQL_DROP = "DROP TABLE IF EXISTS address.unique_prefix_aliases CASCADE"
    SQL_CREATE = "CREATE TABLE address.unique_prefix_aliases(" \
                 "prefix VARCHAR(100) NOT NULL," \
                 "alias VARCHAR(100) NOT NULL," \
                 "CONSTRAINT unique_prefix_suffix_pkey PRIMARY KEY (prefix,alias))"

    reader = None

    try:
        con = ec_psql_util.psql_connection(_database="ec", _host="localhost", _user="sde", _password="sde", _port=5432,
                                           _read_only=False)

        cur = con.cursor()
        cur.execute(SQL_DROP)
        con.commit()
        cur.execute(SQL_CREATE)
        con.commit()

        reader = None

        with open('./data/address/unique_prefix_aliases.csv', 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
            next(reader, None)
            for row in reader:
                prefix = str(row[0])
                alias = str(row[1])
                cur.execute(SQL_INSERT, (prefix, alias))
                con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    except OSError as e:
        logging.error(e)

    except IOError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def load_unique_street_name_aliases():
    con = None
    SQL_INSERT = "INSERT INTO address.unique_street_name_aliases(name,alias) VALUES (%s,%s)"
    SQL_DROP = "DROP TABLE IF EXISTS address.unique_street_name_aliases CASCADE"
    SQL_CREATE = "CREATE TABLE address.unique_street_name_aliases(" \
                 "name VARCHAR(100) NOT NULL," \
                 "alias VARCHAR(100) NOT NULL," \
                 "CONSTRAINT unique_street_name_alias_pkey PRIMARY KEY (name,alias))"

    reader = None

    try:
        con = ec_psql_util.psql_connection(_database="ec", _host="localhost", _user="sde", _password="sde", _port=5432,
                                           _read_only=False)

        cur = con.cursor()
        cur.execute(SQL_DROP)
        con.commit()
        cur.execute(SQL_CREATE)
        con.commit()

        reader = None

        with open('./data/address/unique_street_name_alias.csv', 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
            next(reader, None)
            for row in reader:
                name = str(row[0])
                alias = str(row[1])
                cur.execute(SQL_INSERT, (name, alias))
                con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    except OSError as e:
        logging.error(e)

    except IOError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def load_unique_full_street_names():
    con = None
    SQL_INSERT = "INSERT INTO address.unique_full_street_names(name,prefix,type) VALUES (%s,%s,%s)"
    SQL_DROP = "DROP TABLE IF EXISTS address.unique_full_street_names CASCADE"
    SQL_CREATE = "CREATE TABLE address.unique_full_street_names(" \
                 "id SERIAL NOT NULL," \
                 "name VARCHAR(100) NOT NULL," \
                 "prefix CHARACTER(1) NULL," \
                 "type VARCHAR(10) NULL," \
                 "CONSTRAINT unique_full_street_names_pkey PRIMARY KEY (id))"

    reader = None

    try:
        con = ec_util.psql_connection()

        cur = con.cursor()
        cur.execute(SQL_DROP)
        con.commit()
        cur.execute(SQL_CREATE)
        con.commit()

        reader = None

        with open('./data/address/unique_full_street_names.csv', 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
            next(reader, None)
            for row in reader:
                prefix = None
                type = None
                name = str(row[0]).upper()
                prefix = str(row[1]).upper()
                type = str(row[2]).upper()
                cur.execute(SQL_INSERT, (name, prefix, type))
                con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    except OSError as e:
        logging.error(e)

    except IOError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def load_unique_units():
    con = None
    SQL_INSERT = "INSERT INTO address.unique_units(name) VALUES (%s)"
    SQL_DROP = "DROP TABLE IF EXISTS address.unique_units CASCADE"
    SQL_CREATE = "CREATE TABLE address.unique_units(" \
                 "id SERIAL NOT NULL," \
                 "name VARCHAR(5) NOT NULL," \
                 "CONSTRAINT unique_units_pkey PRIMARY KEY (id))"

    reader = None

    try:
        con = ec_psql_util.psql_connection(_database="ec", _host="localhost", _user="sde", _password="sde", _port=5432,
                                           _read_only=False)

        cur = con.cursor()
        cur.execute(SQL_DROP)
        con.commit()
        cur.execute(SQL_CREATE)
        con.commit()

        reader = None

        with open('./data/address/unique_units.csv', 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
            next(reader, None)
            for row in reader:
                unit = str(row[0])
                cur.execute(SQL_INSERT, [unit])
                con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    except OSError as e:
        logging.error(e)

    except IOError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def load_exceptions():
    con = None
    SQL_INSERT = "INSERT INTO address.street_exceptions(exception,name) VALUES (%s,%s)"
    SQL_DROP = "DROP TABLE IF EXISTS address.street_exceptions CASCADE"
    SQL_CREATE = "CREATE TABLE address.street_exceptions(" \
                 "id SERIAL NOT NULL," \
                 "exception VARCHAR(100) NOT NULL," \
                 "name VARCHAR(100) NOT NULL," \
                 "CONSTRAINT street_exceptions_pkey PRIMARY KEY (id))"

    reader = None

    try:
        con = ec_psql_util.psql_connection(_database="ec", _host="localhost", _user="sde", _password="sde", _port=5432,
                                           _read_only=False)

        cur = con.cursor()
        cur.execute(SQL_DROP)
        con.commit()
        cur.execute(SQL_CREATE)
        con.commit()

        with open('./data/address/street_name_exceptions.csv', 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
            # next(reader, None)
            for row in reader:
                name_exception = str(row[0])
                name = str(row[1])
                cur.execute(SQL_INSERT, [name_exception, name])
                con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    except OSError as e:
        logging.error(e)

    except IOError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def load_unique_prefixes():
    con = None
    SQL_INSERT = "INSERT INTO address.unique_prefixes(prefix) VALUES (%s)"
    SQL_DROP = "DROP TABLE IF EXISTS address.unique_prefixes CASCADE"
    SQL_CREATE = "CREATE TABLE address.unique_prefixes(" \
                 "id SERIAL NOT NULL," \
                 "prefix VARCHAR(100) NOT NULL," \
                 "CONSTRAINT unique_prefixes_pkey PRIMARY KEY (id))"

    reader = None

    try:
        con = ec_psql_util.psql_connection(_database="ec", _host="localhost", _user="sde", _password="sde", _port=5432,
                                           _read_only=False)

        cur = con.cursor()
        cur.execute(SQL_DROP)
        con.commit()
        cur.execute(SQL_CREATE)
        con.commit()

        reader = None

        with open('./data/address/unique_prefixes.csv', 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
            next(reader, None)
            for row in reader:
                prefix = str(row[0])
                cur.execute(SQL_INSERT, [prefix])
                con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    except OSError as e:
        logging.error(e)

    except IOError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def get_all_street_type_aliases():
    con = None
    aliases = []
    try:
        con = ec_psql_util.psql_connection()
        cur = con.cursor()
        cur.execute("SELECT alias FROM address.unique_street_type_aliases")
        rows = cur.fetchall()
        for row in rows:
            aliases.append(row[0])
        con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()
    return (aliases)


def get_all_street_types():
    con = None
    street_types = []
    try:
        con = ec_psql_util.psql_connection()
        cur = con.cursor()
        cur.execute("SELECT street_type FROM address.unique_street_types")
        rows = cur.fetchall()
        for row in rows:
            street_types.append(row[0])
        con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()
    return (street_types)


def get_all_street_prefix_alias():
    con = None
    alias = None
    prefixes = ec_hashmap.new()
    try:
        con = ec_psql_util.psql_connection()
        cur = con.cursor()
        cur.execute("SELECT alias, prefix FROM address.unique_prefix_aliases")
        rows = cur.fetchall()
        for row in rows:
            key = row[0]
            value = row[1]
            ec_hashmap.set(prefixes, key, value)
        con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()
    return (prefixes)


def get_street_name_by_exception(_prefix, _exception_street_name):
    con = None
    street_name = None
    if _prefix:
        exception_name = _prefix + " " + _exception_street_name
    else:
        exception_name = _exception_street_name
    try:
        con = ec_util.psql_connection()
        cur = con.cursor()
        sql = None
        street_name = None
        sql = "SELECT name FROM address.street_exceptions AS a WHERE a.exception = %(_exception_street_name)s"
        cur.execute(sql, {"_exception_street_name": exception_name})
        street_name = cur.fetchone()
        con.commit()
    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()
    return (street_name)


def get_street_name_by_alias(_alias):
    con = None
    street_name = None
    try:
        con = ec_util.psql_connection()
        cur = con.cursor()
        sql = None
        street_name = None
        sql = "SELECT name FROM address.unique_street_name_aliases AS a WHERE a.alias = %(_alias)s"
        cur.execute(sql, {"_alias": _alias})
        street_name = cur.fetchone()
        con.commit()
    except psycopg2.DatabaseError as e:
        logging.error(e)

    finally:
        if con:
            con.close()
    return (street_name)


def get_street_name(_prefix, _name, _type=None):
    con = None
    address = None
    try:
        con = ec_util.psql_connection()
        cur = con.cursor()
        sql = "SELECT prefix, name, type FROM address.unique_full_street_names AS a WHERE "
        street_name = []
        if _prefix:
            if _type:
                sql = sql + "a.prefix = %(_prefix)s AND a.name = %(_name)s AND a.type = %(_type)s"
                cur.execute(sql, {"_prefix": _prefix, "_name": _name, "_type": _type})
            else:
                sql = sql + "a.prefix = %(_prefix)s AND a.name = %(_name)s"
                cur.execute(sql, {"_prefix": _prefix, "_name": _name})
        else:
            if _type:
                sql = sql + "a.name = %(_name)s AND a.type = %(_type)s"
                cur.execute(sql, {"_name": _name, "_type": _type})
            else:
                sql = sql + "a.name = %(_name)s"
                cur.execute(sql, {"_name": _name})
        row = cur.fetchone()
        if row:
            prefix = row[0]
            name = row[1]
            type = row[2]
            if prefix:
                prefix = prefix.strip().upper()
            if name:
                name = name.strip().upper()
            if type:
                type = type.strip().upper()
            address = Address(None, prefix, name, type)

        con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    except:
        logging.error(sys.exc_info()[0])

    finally:
        if con:
            con.close()
    return (address)


def load_unique_street_names():
    con = None
    SQL_INSERT_NAMES = "INSERT INTO address.unique_street_names(street_name,words,fuzzy) " \
                       "VALUES (%s," \
                       "array_length(regexp_split_to_array(%s, E'\\\s+'), 1), " \
                       "soundex(%s))"
    SQL_DROP_NAMES = "DROP TABLE IF EXISTS address.unique_street_names CASCADE"
    SQL_CREATE_NAMES = "CREATE TABLE address.unique_street_names(" \
                       "id SERIAL NOT NULL," \
                       "street_name VARCHAR(100) NOT NULL," \
                       "words INTEGER NOT NULL," \
                       "fuzzy CHARACTER(4) NOT NULL," \
                       "CONSTRAINT unique_street_names_pkey PRIMARY KEY (id)," \
                       "CONSTRAINT unique_street_names_unq UNIQUE (street_name))"

    reader = None

    try:
        con = ec_psql_util.psql_connection(_database="ec", _host="localhost", _user="sde", _password="sde", _port=5432,
                                           _read_only=False)

        cur = con.cursor()
        cur.execute(SQL_DROP_NAMES)
        con.commit()
        cur.execute(SQL_CREATE_NAMES)
        con.commit()

        reader = None

        with open('./data/address/unique_street_names.csv', 'rb') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
            next(reader, None)
            for row in reader:
                street_name = str(row[1])
                try:
                    cur.execute(SQL_INSERT_NAMES, [street_name, street_name, street_name])
                except psycopg2.IntegrityError:
                    con.rollback()
                else:
                    con.commit()

    except psycopg2.DatabaseError as e:
        logging.error(e)

    except OSError as e:
        logging.error(e)

    except IOError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def parse_house_number(_string):
    house_number = None
    if _string.isdigit():
        house_number = int(_string)
    return (house_number)


def split_parser(_prefixes, _freeform):
    logging.debug("READ: %s" % _freeform)
    # load necessary arrays
    street_types = get_all_street_types();
    street_type_aliases = get_all_street_type_aliases();

    # remove periods and commas in address
    for char in ".":
        freeform = _freeform.replace(char, "")
    for char in ",":
        freeform = _freeform.replace(char, "")

    split_freeform = freeform.split()
    number_splits = split_freeform.__len__()

    # Not a valid address with one split
    if number_splits == 1:
        logging.debug("%s is not a valid address." % _freeform)
        return None

    house_number = parse_house_number(split_freeform[0])
    # Must have a house_number
    if not house_number:
        logging.debug("No house number found for %s." % freeform)
        return None

    # Check last word: is it unit, street type, etc
    idx_end = split_freeform.__len__() - 1

    street_type = None
    for type in street_types:
        if split_freeform[idx_end] == type:
            idx_end = idx_end - 1
            street_type = type
            break

    # Check for Prefix value at first split
    prefix = ec_hashmap.get(_prefixes, split_freeform[1])
    if (split_freeform[1] == "EAST" or split_freeform[1] == "WEST" or split_freeform[1] == "SOUTH"):
        prefix = None

    address = None
    unit = None
    idx_start = 1
    if prefix:
        idx_start = 2

    num_words = idx_end - idx_start + 1

    # combine words to form composite street name
    if (number_splits - idx_start) >= 1:
        street_name = ""

        while (num_words > 0):
            street_name = ""
            for idx in range(idx_start, idx_start + num_words):
                street_name = street_name + " " + split_freeform[idx]

            street_name = street_name.strip()
            num_words = num_words - 1

            address = find_address(
                idx,
                split_freeform,
                house_number,
                prefix,
                street_name,
                street_type,
                street_types,
                street_type_aliases)

            if address:
                break

    if address:
        logging.debug("\tFOUND: %s\n" % address)
    else:
        logging.debug("\tNOT PARSED\n")

    return (address)


def find_address(_idx,
                 _split_freeform,
                 _house_number,
                 _prefix,
                 _street_name,
                 _type,
                 _street_types,
                 _street_type_aliases):
    number_splits = _split_freeform.__len__()
    address = get_street_name(_prefix, _street_name, _type)
    unit = None

    if not address:
        # check for alias
        street_name = get_street_name_by_alias(_street_name)
        address = get_street_name(_prefix, street_name, _type)
        # if not address:
        # check for street exceptions
        # street_name = get_street_name_by_exception(_prefix,_street_name)
        # address = get_street_name(_prefix, street_name)
    else:
        # check for secondary unit values
        if (number_splits - _idx) > 1:
            unit = get_unit(_street_types, _street_type_aliases,
                            _split_freeform[number_splits - 1].upper())
    if address:
        address.add_number = int(_house_number)
        address.add_unit = unit

    return (address)


def get_unit(_street_types, _street_type_aliases, _unit):
    unit = _unit
    for char in "#":
        unit = unit.replace(char, "")
    # need to make sure its not a street type
    for street_type in _street_types:
        if unit == street_type:
            return None
    for alias in _street_type_aliases:
        if unit == alias:
            return None

    if unit:
        unit = "#{}".format(unit.strip())
    return (unit)


def load_incode_addresses():
    con = None
    SQL_INSERT_INCODE = "INSERT INTO address.incode_address(" \
                        "freeform_address," \
                        "occupant," \
                        "account_status," \
                        "account," \
                        "addr_number," \
                        "addr_prefix," \
                        "addr_street_name," \
                        "addr_street_type," \
                        "addr_occupant_id," \
                        "addr_city," \
                        "addr_state," \
                        "addr_zip_code)" \
                        "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
    SQL_DROP_INCODE = "DROP TABLE IF EXISTS address.incode_address CASCADE"
    SQL_CREATE_INCODE = "CREATE TABLE address.incode_address(" \
                        "id SERIAL NOT NULL," \
                        "freeform_address VARCHAR(100) NOT NULL," \
                        "occupant VARCHAR(100) NULL," \
                        "account_status CHARACTER(1) NULL," \
                        "account CHARACTER(10) NOT NULL," \
                        "addr_number INTEGER NOT NULL," \
                        "addr_prefix VARCHAR(5) NULL," \
                        "addr_street_name VARCHAR(100) NOT NULL," \
                        "addr_street_type VARCHAR(6) NULL," \
                        "addr_occupant_id VARCHAR(10) NULL," \
                        "addr_city VARCHAR(25) NOT NULL," \
                        "addr_state CHARACTER(2) NOT NULL," \
                        "addr_zip_code CHARACTER(5) NOT NULL," \
                        "CONSTRAINT incode_address_pkey PRIMARY KEY (id))"
    edit = None
    out_file = None

    try:
        con = ec_util.psql_connection()

        cur = con.cursor()
        cur.execute(SQL_DROP_INCODE)
        con.commit()
        cur.execute(SQL_CREATE_INCODE)
        con.commit()

        prefixes = get_all_street_prefix_alias()
        reader = None
        workspace = ec_arcpy_util.sde_workspace_via_host()
        arcpy.env.workspace = workspace
        listDatasets = arcpy.ListDatasets("", "Feature")
        # for dataset in listDatasets:
        #     print(workspace + os.sep + os.sep + dataset)

        sr_2278 = arcpy.SpatialReference(2278)
        data_set_path_meter_address = workspace + os.sep + os.sep + "elc.sde.Address"
        workspace_meter = workspace + os.sep + os.sep + "Water" + os.sep + os.sep + "Meter"
        workspace_meter_address = workspace + os.sep + os.sep + "Address" + os.sep + os.sep + "meter_address"
        fields_meter = ["incode_account", "OID@", "SHAPE@"]
        fields_meter_address = ["account", "house_number", "prefix", "street_name", "suffix", "street_type","secondary_name","city", "state", "zip", "OID@", "SHAPE@"]

        arcpy.AcceptConnections(workspace, False)
        arcpy.DisconnectUser(workspace, "ALL")
        # arcpy.RegisterAsVersioned_management("Address", "EDITS_TO_BASE")

        if arcpy.Exists(workspace_meter_address):
            arcpy.Delete_management(workspace_meter_address)
        arcpy.CreateFeatureclass_management(workspace + os.sep + os.sep + "Address", "meter_address", "POINT", "", "", "", sr_2278)
        arcpy.AddField_management(workspace_meter_address, "account", "TEXT", "", "", 7, "Account", "NON_NULLABLE")
        arcpy.AddField_management(workspace_meter_address, "house_number", "SHORT", "", "", "", "House Number","NON_NULLABLE")
        arcpy.AddField_management(workspace_meter_address, "prefix", "TEXT", "", "", 6, "Account", "NULLABLE")
        arcpy.AddField_management(workspace_meter_address, "street_name", "TEXT", "", "", 50, "Street Name","NON_NULLABLE")
        arcpy.AddField_management(workspace_meter_address, "suffix", "TEXT", "", "", 10, "Suffix", "NULLABLE")
        arcpy.AddField_management(workspace_meter_address, "street_type", "TEXT", "", "", 4, "Street Type", "NULLABLE")
        arcpy.AddField_management(workspace_meter_address, "secondary_name", "TEXT", "", "", 10, "Secondary Name","NULLABLE")
        arcpy.AddField_management(workspace_meter_address, "city", "TEXT", "", "", 25, "City", "NON_NULLABLE")
        arcpy.AddField_management(workspace_meter_address, "state", "TEXT", "", "", 2, "State", "NON_NULLABLE")
        arcpy.AddField_management(workspace_meter_address, "zip", "TEXT", "", "", 5, "ZIP", "NON_NULLABLE")
        arcpy.RegisterAsVersioned_management("Address", "EDITS_TO_BASE")

        edit = arcpy.da.Editor(workspace)
        edit.startEditing(with_undo=False, multiuser_mode=True)
        edit.startOperation()
        out_file = open("../data/water_accounts_update.csv", "wb")
        writer = csv.writer(out_file, delimiter="\t")

        with open('../data/water_accounts.csv', 'r+b') as csvfile:
            reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
            # next(reader, None)

            for row in reader:
                full_account = str(row[0])
                freeform = str(row[1]).upper()
                occupant = str(row[2]).upper()
                account_status = str(row[3]).upper()
                address = split_parser(prefixes, freeform)
                # find meter account
                row_out = []
                row_out.append(full_account)
                row_out.append(freeform)
                row_out.append(occupant)
                row_out.append(account_status)

                if address:
                    account = full_account.split("-", 3)
                    account = "{}-{}".format(account[0], account[1])

                    where_clause = """"incode_account" = '{}'""".format(account)
                    rows = arcpy.da.SearchCursor(workspace_meter, fields_meter, where_clause)
                    for row in rows:
                        with arcpy.da.InsertCursor(workspace_meter_address, fields_meter_address) as ic:
                            ic.insertRow(
                                (account, address.add_number, address.st_prefix, address.st_name, None, address.st_type,
                                 address.add_unit, "EL CAMPO", "TX", "77437", row[1], row[2]))
                            # print(account)
                            break

                    row_out.append(address.__str__())
                    row_out.append(address.add_number)
                    row_out.append(address.st_prefix)
                    row_out.append(address.st_name)
                    row_out.append(address.st_suffix)
                    row_out.append(address.st_type)
                    row_out.append(address.add_unit)
                else:
                    row_out.append("NO ADDRESS MATCH")

                writer.writerow(row_out)

    except arcpy.ExecuteError as e:
        logging.error(arcpy.GetMessage(0))
        logging.error(e)

    except psycopg2.DatabaseError as e:
        logging.error(e)

    except:
        logging.error(sys.exc_info()[0])

    finally:
        if con:
            con.close()
        if out_file:
            out_file.close()
        if edit:
            edit.stopOperation()
            logging.info("Saving Changes")
            edit.stopEditing(save_changes=True)


def load_parcel_addresses(_from_shapefile):
    con = None
    edit = None
    out_file = None
    # prefixes = get_all_street_prefix_alias()

    try:
        to_workspace = ec_arcpy_util.sde_workspace_via_host()
        arcpy.env.workspace = to_workspace
        arcpy.AcceptConnections(to_workspace, False)
        arcpy.DisconnectUser(to_workspace, "ALL")
        sr_2278 = arcpy.SpatialReference(2278)

        from_workspace = _from_shapefile
        workspace_parcel_county = to_workspace + os.sep + os.sep + "Boundary" + os.sep + os.sep + "county_parcel_owner"

        if not arcpy.Exists("WhartonCAD"):
            arcpy.CreateFeatureDataset_management(to_workspace, "WhartonCAD", sr_2278)

        if arcpy.Exists("parcel_address"):
            arcpy.Delete_management("parcel_address")
        #
        # Define Fields for parcel_address
        arcpy.CreateFeatureclass_management(to_workspace, "WhartonCAD/parcel_address", "POINT", "", "", "", sr_2278)
        arcpy.AddField_management("parcel_address", "prop_id", "TEXT", "", "", "10", "Property ID", "NON_NULLABLE")
        arcpy.AddField_management("parcel_address", "add_number", "LONG", "", "", "", "House Number", "NON_NULLABLE")
        arcpy.AddField_management("parcel_address", "st_prefix", "TEXT", "", "", 6, "Prefix", "NULLABLE")
        arcpy.AddField_management("parcel_address", "st_name", "TEXT", "", "", 50, "Street Name",
                                  "NON_NULLABLE")
        arcpy.AddField_management("parcel_address", "st_type", "TEXT", "", "", 4, "Street Type", "NULLABLE")
        arcpy.AddField_management("parcel_address", "add_unit", "TEXT", "", "", 20, "Unit",
                                  "NULLABLE")
        arcpy.AddField_management("parcel_address", "st_fullname", "TEXT", "", "", 50, "Full Street Name", "NULLABLE")
        arcpy.AddField_management("parcel_address", "source", "TEXT", "", "", 40, "Data Source", "NON_NULLABLE")

        if not arcpy.Describe("WhartonCAD").isVersioned:
            arcpy.RegisterAsVersioned_management("WhartonCAD", "EDITS_TO_BASE")

        to_fields = ["prop_id", "add_number", "st_prefix", "st_name", "st_type", "add_unit", "st_fullname", "source",
                     "SHAPE@"]
        from_fields = ["prop_id", "situs_num", "situs_stre", "situs_st_1", "situs_city", "SHAPE@"]
        street_types = get_all_street_types()
        street_type_aliases = get_all_street_type_aliases()

        edit = arcpy.da.Editor(to_workspace)
        edit.startEditing(with_undo=False, multiuser_mode=True)
        edit.startOperation()

        insert_cursor = arcpy.da.InsertCursor("WhartonCAD/parcel_address", to_fields)

        # where clause to restrict to EL CAMPO
        # where_clause = "\"prop_id\" > 0 AND \"situs_num\" IS NOT NULL AND \"situs_num\" != '0' AND \"situs_city\" = 'EL CAMPO'"
        where_clause = """ "situs_num" IS NOT NULL AND "situs_num" <> '0' AND situs_city = 'EL CAMPO' """
        # where_clause = "\"prop_id\" > 0 AND \"situs_num\" IS NOT NULL AND \"situs_num\" != '0'"
        # where_clause = "\"prop_id\" > 0 AND \"situs_num\" IS NOT NULL AND \"situs_num\" != '0' AND \"situs_city\" = 'EL CAMPO' AND \"situs_st_1\" = 'MICHAEL'"
        with arcpy.da.SearchCursor(from_workspace, from_fields, where_clause) as cursor:
            for row in cursor:
                house_number = row[0]
                prefix = None
                if str(row[1]).strip().__len__() > 0:
                    prefix = str(row[1])
                street = str(row[2]).strip().upper()
                # suffix = str(row[3]).strip().upper()
                # type = None
                # city = str(row[4]).strip().upper()
                # prop_id = row[5]
                # oid = row[6]
                # if not row[7]:
                #     continue
                # else:
                #     if not row[7].labelPoint:
                #         continue
                #     point = row[7].labelPoint
                #
                # split_freeform = street.split()
                #
                # len = split_freeform.__len__()
                # for street_type in street_types:
                #     if street_type == split_freeform[split_freeform.__len__() - 1].upper():
                #         len = split_freeform.__len__() - 1
                #         type = street_type
                #         break
                #
                # for street_type in street_type_aliases:
                #     if street_type == split_freeform[split_freeform.__len__() - 1].upper():
                #         len = split_freeform.__len__() - 1
                #         type = street_type
                #         break
                #
                # free_form = str(house_number)
                # if prefix:
                #     free_form = free_form + " " + prefix
                #
                # free_form = free_form + " " + street
                #
                # # for idx in range(0,len):
                # #     if free_form:
                # #         free_form = free_form + " " + split_freeform[idx]
                # #     else:
                # #         free_form = str(house_number) + " " + split_freeform[idx]
                #
                # idx_start = 0
                #
                # address = split_parser(prefixes, free_form)
                # if address:
                #     insert_cursor.insertRow((prop_id, address.house_number, address.prefix, address.name, None,
                #                              address.type, address.unit, "EL CAMPO", "TX", "77437", oid, point))



    except arcpy.ExecuteError as e:
        logging.error(arcpy.GetMessage(0))
        logging.error(e)

    except psycopg2.DatabaseError as e:
        logging.error(e)

    except:
        logging.error(sys.exc_info()[1])

    finally:
        if con:
            con.close()
        if out_file:
            out_file.close()
        if edit:
            edit.stopOperation()
            logging.info("Saving Changes")
            edit.stopEditing(save_changes=True)


def load_e911_addresses(_from_workspace, _cleanup=True):
    edit = None
    SQL_INSERT_ADDRESSES_911 = "INSERT INTO address.address_911(add_number, st_prefix, st_name, st_suffix, st_type, add_unit, add_full, add_source, add_zip, add_city, fuzzy) " \
                               "VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,soundex(%s)) ON CONFLICT ON " \
                               "CONSTRAINT address_911_name_idx DO NOTHING"


    try:
        #
        # drop/create table for schema address
        psql_connection = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")
        if _cleanup:
            setup_e911_addresses_tables(psql_connection)
            psql_connection = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")

        #
        # arcgis stuff for multi-users
        to_workspace = ec_arcpy_util.sde_workspace_via_host()
        arcpy.AcceptConnections(to_workspace, False)
        arcpy.DisconnectUser(to_workspace, "ALL")
        arcpy.env.workspace = to_workspace

        #
        # if first time, create featuredataset HGAC
        ds = ec_arcpy_util.find_dataset("*HGAC")
        sr_2278 = arcpy.SpatialReference(2278)
        if ds is None:
            ds = arcpy.CreateFeatureDataset_management(to_workspace, "HGAC", sr_2278)

        #
        # if feature class exists, delete it
        fc = ec_arcpy_util.find_feature_class("*address911","HGAC")
        if fc:
            arcpy.Delete_management("address911")

        #
        # Define Fields for starmap featureclass
        fc = arcpy.CreateFeatureclass_management(to_workspace + r"\HGAC", "address911", "POINT", "", "", "", sr_2278)
        arcpy.AddField_management(fc, "add_number", "LONG", "", "", "", "House Number", "NON_NULLABLE")
        arcpy.AddField_management(fc, "st_prefix", "TEXT", "", "", 6, "Prefix", "NULLABLE")
        arcpy.AddField_management(fc, "st_name", "TEXT", "", "", 50, "Street Name", "NON_NULLABLE")
        arcpy.AddField_management(fc, "st_suffix", "TEXT", "", "", 10, "Street Suffix", "NULLABLE")
        arcpy.AddField_management(fc, "st_type", "TEXT", "", "", 4, "Street Type", "NULLABLE")
        arcpy.AddField_management(fc, "add_unit", "TEXT", "", "", 20, "Unit", "NULLABLE")
        arcpy.AddField_management(fc, "st_fullname", "TEXT", "", "", 50, "Full Street Name", "NON_NULLABLE")
        arcpy.AddField_management(fc, "add_zip", "TEXT", "", "", 5, "ZIP", "NULLABLE")
        arcpy.AddField_management(fc, "add_city", "TEXT", "", "", 40, "City", "NULLABLE")
        arcpy.AddField_management(fc, "source", "TEXT", "", "", 40, "Data Source", "NON_NULLABLE")
        arcpy.AddField_management(fc, "global_id", "GUID", "", "", 10, "Global ID", "NON_NULLABLE")

        if not arcpy.Describe(ds).isVersioned:
            arcpy.RegisterAsVersioned_management(ds, "EDITS_TO_BASE")
        edit = arcpy.da.Editor(to_workspace)
        edit.startEditing(with_undo=False, multiuser_mode=True)
        edit.startOperation()

        #
        # feature class field list
        fields_e911_address = ["add_number", "st_prefix", "st_name", "st_suffix", "st_type", "add_unit", "st_fullname", "add_zip", "add_city", "source", "global_id", "SHAPE@"]
        insert_cursor = arcpy.da.InsertCursor("address911", fields_e911_address)

        from_fields = ["ADD_NUMBER", "ST_PREDIR", "STREETNAME", "ST_POSTYP", "UNIT", "POST_COMM", "POST_CODE", "SOURCE", "GLOBALID", "SHAPE@"]
        from_fc = _from_workspace + os.sep + "hgac911_address"

        # Iterator over HGAC supplied feature class
        with arcpy.da.SearchCursor(from_fc, from_fields) as cursor:
            for row in cursor:
                house_number = ec_util.to_pos_int_or_none(row[0])
                #
                # if no house number it can not be a valid address
                if house_number is None:
                    continue
                prefix = ec_util.to_upper_or_none(row[1])
                st_name = ec_util.to_upper_or_none(row[2])
                #
                # if no street name it can not be a valid address
                if st_name is None:
                    continue
                st_type = ec_util.to_upper_or_none(row[3])
                unit = ec_util.to_upper_or_none(row[4])
                add_city = ec_util.to_upper_or_none(row[5])
                add_zip = ec_util.to_upper_or_none(row[6])
                source = ec_util.to_upper_or_none(row[7])
                global_id = row[8]
                point = row[9]

                address = Address(add_number=house_number, st_prefix=prefix, st_name=st_name, st_type=st_type, st_suffix=None, add_unit=unit, add_city=add_city, add_zip=add_zip )
                insert_cursor.insertRow((address.add_number,
                                         address.st_prefix,
                                         address.st_name,
                                         address.st_suffix,
                                         address.st_type,
                                         address.add_unit,
                                         address.full_name(),
                                         address.add_zip,
                                         address.add_city,
                                         source,
                                         global_id,
                                         point))

                # Insert into Address schema
                insert_address(psql_connection, address, source, SQL_INSERT_ADDRESSES_911)

            #
            # Create attribute index
            table_name = "ec.sde.address911"
            arcpy.AddIndex_management(table_name, ["st_fullname", "add_unit"], "address911_fc_unq_idx", "unique")

    except psycopg2.DatabaseError as e:
        if psql_connection:
            psql_connection.rollback()
        logging.error(e)

    except:
        logging.error(sys.exc_info()[1])

    finally:
        if psql_connection:
            psql_connection.commit()
            psql_connection.close()
        if edit:
            edit.stopOperation()
            logging.info("Saving changes for HGAC address import")
            edit.stopEditing(save_changes=True)


def load_new_hgac_e911_addresses(_gdb_database):
    con = None
    fc_new_hgac_e911 = _gdb_database

    try:
        workspace = ec_arcpy_util.sde_workspace_via_host()
        arcpy.env.workspace = workspace
        arcpy.AcceptConnections(workspace, False)
        arcpy.DisconnectUser(workspace, "ALL")

        workspace_e911_address = workspace + os.sep + os.sep + "Address" + os.sep + os.sep + "e911_address"
        #
        fields_e911_address = ["house_number", "prefix", "street_name", "suffix", "street_type",
                               "secondary_name",
                               "city", "state", "zip", "SHAPE@"]
        fields_new_hgac_address = ["ADD_NUMBER", "PREFIX", "NAME", "ST_TYPE", "SUFFIX", "ADD_UNIT", "SHAPE@"]
        #
        edit = arcpy.da.Editor(workspace)
        edit.startEditing(with_undo=False, multiuser_mode=True)
        edit.startOperation()
        #
        insert_cursor = arcpy.da.InsertCursor(workspace_e911_address, fields_e911_address)

        select_cursor = arcpy.da.SearchCursor(fc_new_hgac_e911, fields_new_hgac_address)
        counter = 0

        for row in select_cursor:
            counter = counter + 1
            select_query = None
            house_number = ec_util.to_pos_int_or_none(row[0])
            if house_number is None:
                continue
            select_query = "house_number = " + str(house_number)

            prefix = ec_util.to_upper_or_none(row[1])
            if prefix is not None:
                select_query = select_query + " AND prefix = '" + prefix + "'"

            name = ec_util.to_upper_or_none(row[2])
            if name is None:
                continue
            select_query = select_query + " AND street_name = '" + name + "'"

            type = ec_util.to_upper_or_none(row[3])
            if type is not None:
                select_query = select_query + " AND street_type = '" + type + "'"

            suffix = ec_util.to_upper_or_none(row[4])
            if suffix is not None:
                select_query = select_query + " AND suffix = '" + suffix + "'"

            add_unit = ec_util.to_upper_or_none(row[5])
            if add_unit is not None:
                select_query = select_query + " AND secondary_name = '" + add_unit + "'"
                add_unit = "#" + add_unit

            point = row[6]
            if point is None:
                continue

            # print str(counter) + " " + select_query

            # If we find the exact address, then
            # found_cursor = arcpy.da.SearchCursor(workspace_e911_address, fields_e911_address, select_query)
            # for found_row in found_cursor:
            insert_needed = True
            with arcpy.da.UpdateCursor(workspace_e911_address, fields_e911_address, select_query) as update_cursor:
                for update_row in update_cursor:
                    point = update_row[9]
                    update_row[9] = point
                    insert_needed = False
                    update_cursor.updateRow(update_row)
                    # print "update needed"
                    break
            if insert_needed:
                # print "insert needed"
                insert_row = []
                insert_row.append(house_number)
                insert_row.append(prefix)
                insert_row.append(name)
                insert_row.append(suffix)
                insert_row.append(type)
                insert_row.append(add_unit)
                insert_row.append("EL CAMPO")
                insert_row.append("TX")
                insert_row.append("77437")
                insert_row.append(point)
                insert_cursor.insertRow(insert_row)

    except psycopg2.DatabaseError as e:
        if con:
            con.rollback()
        logging.error(e)

    except:
        logging.error(sys.exc_info()[1])

    finally:
        # if con:
        #     con.commit()
        #     con.close()
        if edit:
            edit.stopOperation()
            logging.info("Saving changes for HGAC import")
            edit.stopEditing(save_changes=True)
