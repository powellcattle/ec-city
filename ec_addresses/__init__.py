#!/usr/bin/python
# -*- coding: utf-8 -*-
import pyodbc
import re
from collections import defaultdict
import inspect

import usaddress
from scourgify.exceptions import UnParseableAddressError

import ec_addresses
import ec_incode

__author__ = 'spowell'

import scourgify
import scourgify.address_constants
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

ADDRESS_LABELS = [
    'add_number',
    'st_prefix',  # 6
    'st_name',  # 50
    'st_type',  # 4
    'add_unit',  # 20
    'st_full_name',  # 50
    'add_address',  # 60
    'city',  # 40
    'zip',  # 5
    'source'  # 40
]


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
        if self.add_number is None or self.st_name is None:
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


def full_street_name(_address_dict):
    logging.debug("full_street_name: {}".format(_address_dict))

    full_street_name = None
    if _address_dict["st_prefix"]:
        full_street_name = _address_dict["st_prefix"]

    if _address_dict["st_name"]:
        if full_street_name is None:
            full_street_name = _address_dict["st_name"]
        else:
            full_street_name = " ".join([full_street_name, _address_dict["st_name"]])

    if _address_dict["st_type"]:
        full_street_name = " ".join([full_street_name, _address_dict["st_type"]])

    return full_street_name


def full_address(_address_dict):
    logging.debug("full_address: {}".format(_address_dict))

    _full_address = full_street_name(_address_dict)
    _full_address = " ".join([str(_address_dict["add_number"]).strip(), _full_address])

    if _address_dict["add_unit"]:
        _full_address = " # ".join([_full_address, _address_dict["add_unit"]])

    return _full_address


def find_st_type(_con, _add_number, _st_prefix, _st_name):
    SQL_QUERY_ST_TYPE_1 = "SELECT DISTINCT(st_type) FROM address.address_911 WHERE add_number = ? AND st_name = ?"
    SQL_QUERY_ST_TYPE_2 = "SELECT DISTINCT(st_type) FROM address.address_911 WHERE add_number = ? AND st_prefix = ? AND st_name = ?"
    try:
        if _con is None:
            logging.debug("Connection is None")
            return

        cur = _con.cursor()
        # cur.execute(_sql_query,{"st_prefix": _st_prefix, "st_name": _st_name})
        if _st_prefix is None:
            cur.execute(SQL_QUERY_ST_TYPE_1, (_add_number, _st_name))
        else:
            cur.execute(SQL_QUERY_ST_TYPE_2, (_add_number, _st_prefix, _st_name))
        row = cur.fetchone()
        if row:
            return row[0]
        else:
            return None

    except psycopg2.Error as e:
        logging.error(e)


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

    except psycopg2.Error as e:
        logging.error(e)


def sql_insert_address(_con, _address_dict, _sql_insert):
    logging.debug("sql_insert_address: {}, {}".format(_address_dict, _sql_insert))
    try:
        if _con is None:
            logging.debug("Connection is None")
            return

        cur = _con.cursor()
        cur.execute(_sql_insert,
            _address_dict["add_number"],
            _address_dict["st_prefix"],
            _address_dict["st_name"],
            _address_dict["st_type"],
            _address_dict["add_unit"],
            _address_dict["st_full_name"],
            _address_dict["add_address"],
            _address_dict["city"],
            _address_dict["zip"],
            _address_dict["source"],
            _address_dict["st_full_name"])

    except pyodbc.Error as e:
        sqlstate = e.args[0]
        if '23000' == sqlstate:
            logging.info("Duplicate address - sql_insert_address (database):{}".format(e))

    except Exception as e:
        logging.error("Fatal Error - sql_insert_address (database):{}".format(e))


def setup_CAD_addresses_table(_con):
    SQL_DROP_ADDRESSES = "DROP TABLE IF EXISTS address.address_CAD CASCADE"
    SQL_CREATE_ADDRESSES = "CREATE TABLE address.address_CAD(" \
                           "addressCAD_id SERIAL4 NOT NULL, " \
                           "add_number INT NOT NULL, " \
                           "st_prefix CHARACTER(6) NULL, " \
                           "st_name VARCHAR(50) NOT NULL, " \
                           "st_type VARCHAR(4) NULL, " \
                           "add_unit VARCHAR(60) NULL, " \
                           "st_full_name VARCHAR(50) NOT NULL, " \
                           "add_address VARCHAR(60) NOT NULL, " \
                           "zip CHARACTER(5) NULL, " \
                           "city VARCHAR(25) NULL, " \
                           "source VARCHAR(20) NOT NULL, " \
                           "fuzzy CHARACTER(4) NULL, " \
                           "CONSTRAINT unique_addressCAD_pkey PRIMARY KEY (addressCAD_id), " \
                           "CONSTRAINT addressCAD_name_idx UNIQUE (add_address))"

    try:
        cur = _con.cursor()
        cur.execute(SQL_DROP_ADDRESSES)
        cur.execute(SQL_CREATE_ADDRESSES)

    except psycopg2.Error as e:
        if _con:
            _con.rollback()
            logging.error(e)

    finally:
        if _con:
            _con.commit()
            _con.close()


def sql_insert_not_found_address(_con, _address):
    SQL_INSERT_ADDRESSES = "INSERT INTO address.not_found_addresses(add_number,st_prefix,st_name,st_type,st_full_name,add_address,city,zip,source) " \
                           "VALUES (?,?,?,?,?,?,?,?,?) ON CONFLICT ON " \
                           "CONSTRAINT not_found_addresses_idx DO NOTHING"
    if _con is None:
        logging.debug("Connection is None")
        return

    cur = _con.cursor()
    cur.execute(SQL_INSERT_ADDRESSES, [
        _address["add_number"],
        _address["st_prefix"],
        _address["st_name"],
        _address["st_type"],
        _address["st_full_name"],
        _address["add_address"],
        _address["city"],
        _address["zip"],
        _address["source"]])


def setup_not_found_address_table(_con):
    SQL_DROP_ADDRESSES = "DROP TABLE IF EXISTS address.not_found_addresses"
    SQL_CREATE_ADDRESSES = "CREATE TABLE address.not_found_addresses(" \
                           "id INT IDENTITY NOT NULL, " \
                           "add_number INT NOT NULL, " \
                           "st_prefix CHARACTER(9) NULL, " \
                           "st_name VARCHAR(50) NOT NULL, " \
                           "st_type VARCHAR(10) NULL, " \
                           "st_full_name VARCHAR(50) NULL, " \
                           "add_address VARCHAR(60) NULL," \
                           "city CHARACTER(40) NULL, " \
                           "zip VARCHAR(5) NULL, " \
                           "source VARCHAR(40) NULL, " \
                           "CONSTRAINT not_found_addresses_pkey PRIMARY KEY (id)," \
                           "CONSTRAINT not_found_addresses_idx UNIQUE (add_address))"

    try:
        cur = _con.cursor()
        cur.execute(SQL_DROP_ADDRESSES)
        cur.execute(SQL_CREATE_ADDRESSES)

    except Exception as e:
        if _con:
            _con.rollback()
            logging.error("{} {}".format(inspect.stack()[0][3], e))

    finally:
        if _con:
            _con.commit()
            _con.close()


def setup_e911_addresses_tables(_con):
    SQL_DROP_ADDRESSES = "DROP TABLE IF EXISTS address.address_911"
    SQL_CREATE_ADDRESSES = "CREATE TABLE address.address_911(" \
                           "id INT IDENTITY NOT NULL," \
                           "add_number INT NOT NULL," \
                           "st_prefix CHARACTER(6) NULL," \
                           "st_name VARCHAR(50) NOT NULL," \
                           "st_type VARCHAR(4) NULL," \
                           "add_unit VARCHAR(20) NULL," \
                           "st_full_name VARCHAR(50) NOT NULL," \
                           "add_address VARCHAR (60) NOT NULL," \
                           "city VARCHAR(40) NOT NULL," \
                           "zip CHARACTER(5) NULL," \
                           "source VARCHAR(40) NULL," \
                           "fuzzy CHARACTER(4) NULL," \
                           "CONSTRAINT address_911_pkey PRIMARY KEY (id), CONSTRAINT address_911_constraint UNIQUE (add_address))"

    try:
        cur = _con.cursor()
        cur.execute(SQL_DROP_ADDRESSES)
        cur.execute(SQL_CREATE_ADDRESSES)

# except psycopg2.Error as e:
#     if _con:
#         _con.rollback()
#         logging.error(e)
#
    finally:
        if _con:
            _con.commit()


def setup_addresses_incode_table(_con):
    SQL_DROP_ADDRESSES = "DROP TABLE IF EXISTS address.address_incode"
    SQL_CREATE_ADDRESSES = "CREATE TABLE address.address_incode(" \
                           "address_incode_id INT IDENTITY NOT NULL, " \
                           "add_number INT NOT NULL, " \
                           "st_prefix VARCHAR(5) NULL, " \
                           "st_name VARCHAR(100) NOT NULL, " \
                           "st_suffix VARCHAR(5) NULL, " \
                           "st_type VARCHAR(10) NULL, " \
                           "add_unit VARCHAR(20) NULL," \
                           "add_full VARCHAR(50) NOT NULL, " \
                           "add_source VARCHAR(20) NOT NULL, " \
                           "add_zip CHARACTER(5) NULL, " \
                           "add_city VARCHAR(25) NULL, " \
                           "fuzzy CHARACTER(4) NULL, " \
                           "CONSTRAINT unique_address_incode_pkey PRIMARY KEY (address_incode_id), " \
                           "CONSTRAINT address_incode_name_idx UNIQUE (add_full, add_unit))"

    try:
        cur = _con.cursor()
        cur.execute(SQL_DROP_ADDRESSES)
        cur.execute(SQL_CREATE_ADDRESSES)

    # except psycopg2.Error as e:
    #     if _con:
    #         _con.rollback()
    #         logging.error(e)

    finally:
        if _con:
            _con.commit()



def insert_incode(_con, _address, _source):
    SQL_INSERT_ADDRESSES_911 = "INSERT INTO address.address_911(add_number, st_name, st_suffix, st_type, add_unit, add_full, add_source, add_zip, add_city, fuzzy) " \
                               "VALUES (?,?,?,?,?,?,?,?,?,soundex(?)) ON CONFLICT ON " \
                               "CONSTRAINT address_911_name_idx DO NOTHING"
    try:
        if _con is None:
            logging.debug("Connection is None")
            return

        cur = _con.cursor()
        cur.execute(SQL_INSERT_ADDRESSES_911, [_address.add_number, _address.st_name, _address.st_suffix, _address.st_type,
                                               _address.add_unit, _address.full_name(), _source, _address.add_zip, _address.add_city,
                                               _address.full_name()])

    except psycopg2.Error as e:
        logging.error(e)


def setup_addresses_E911_table(_con):
    SQL_DROP_ADDRESSES = "DROP TABLE IF EXISTS address.address_911"
    SQL_CREATE_ADDRESSES = "CREATE TABLE address.address_911(" \
                           "address_911_id SERIAL4 NOT NULL, " \
                           "add_number INT NOT NULL, " \
                           "st_prefix CHARACTER(6) NULL, " \
                           "st_name VARCHAR(50) NOT NULL, " \
                           "st_type VARCHAR(4) NULL, " \
                           "add_unit VARCHAR(20) NULL, " \
                           "st_full_name VARCHAR(50) NOT NULL, " \
                           "add_address VARCHAR(60) NOT NULL, " \
                           "city  VARCHAR(20) NOT NULL, " \
                           "zip CHARACTER(5) NULL, " \
                           "source VARCHAR(25) NULL, " \
                           "fuzzy CHARACTER(4) NULL, " \
                           "CONSTRAINT unique_address_911_pkey PRIMARY KEY (address_911_id), " \
                           "CONSTRAINT address_911_name_idx UNIQUE (add_address))"

    try:
        cur = _con.cursor()
        cur.execute(SQL_DROP_ADDRESSES)
        cur.execute(SQL_CREATE_ADDRESSES)

    except psycopg2.Error as e:
        if _con:
            _con.rollback()
            logging.error(e)

    finally:
        if _con:
            _con.commit()
            _con.close()


def load_starmap_streets(_from_workspace):
    edit = None

    try:
        #
        # arcgis stuff for multi-users
        to_workspace = ec_arcpy_util.sde_workspace_via_host()
        desc = arcpy.Describe(to_workspace)
        cp = desc.connectionProperties
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
        fc = ec_arcpy_util.find_feature_class("*STARMAP", "HGAC")
        if fc:
            arcpy.Delete_management("STARMAP")
        #
        # Define Fields for starmap featureclass
        fc = arcpy.CreateFeatureclass_management(to_workspace + r"\HGAC", "STARMAP", "POLYLINE", "", "", "", sr_2278)
        arcpy.AddField_management(fc, "pwid", "TEXT", "", "", 32, "PubWorks Id", "NON_NULLABLE")
        arcpy.AddField_management(fc, "pwname", "TEXT", "", "", 64, "PubWorks Name", "NON_NULLABLE")
        arcpy.AddField_management(fc, "st_predir", "TEXT", "", "", 9, "Street Prefix", "NULLABLE")
        arcpy.AddField_management(fc, "st_name", "TEXT", "", "", 50, "Street Name", "NULLABLE")
        arcpy.AddField_management(fc, "st_type", "TEXT", "", "", 4, "Street Type", "NULLABLE")
        arcpy.AddField_management(fc, "st_fullname", "TEXT", "", "", 50, "Full Street Name", "NULLABLE")
        arcpy.AddField_management(fc, "from_addr_l", "LONG", "", "", "", "From Left Block #", "NON_NULLABLE")
        arcpy.AddField_management(fc, "to_addr_l", "LONG", "", "", "", "To Left Block #", "NON_NULLABLE")
        arcpy.AddField_management(fc, "from_addr_r", "LONG", "", "", "", "From Right Block #", "NON_NULLABLE")
        arcpy.AddField_management(fc, "to_addr_r", "LONG", "", "", "", "To Right Block #", "NON_NULLABLE")
        arcpy.AddField_management(fc, "source", "TEXT", "", "", 40, "Data Source", "NON_NULLABLE")
        arcpy.AddField_management(fc, "global_id", "GUID", "", "", 10, "Global ID", "NON_NULLABLE")
        arcpy.AddField_management(fc, "city", "TEXT", "", "", 40, "City", "NULLABLE")
        fields_starmap = ["pwid", "pwname", "st_predir", "st_name", "st_fullname", "st_type", "from_addr_l", "to_addr_l", "from_addr_r", "to_addr_r", "source", "global_id", "SHAPE@", "city"]

        if not arcpy.Describe(ds).isVersioned:
            arcpy.RegisterAsVersioned_management(ds, "EDITS_TO_BASE")
        edit = arcpy.da.Editor(to_workspace)
        edit.startEditing(with_undo=False, multiuser_mode=True)
        edit.startOperation()

        insert_cursor = arcpy.da.InsertCursor("starmap", fields_starmap)
        from_fields = ["St_PreDir", "StreetName", "Full_Name", "ST_POSTYP", "FromAddr_L", "ToAddr_L", "FromAddr_R", "ToAddr_R", "SOURCE", "GLOBALID", "SHAPE@", "OBJECTID", "PostComm_L"]
        from_fc = _from_workspace + os.sep + "hgac_starmap"

        blocks_list = list()
        counter = 0

        with arcpy.da.SearchCursor(from_fc, from_fields, sql_clause=(None, 'ORDER BY StreetName, FromAddr_L, FromAddr_R')) as cursor:
            for row in cursor:
                str_predir = ec_util.to_upper_or_none(row[0])
                name = ec_util.to_upper_or_none(row[1])
                if not name:
                    continue

                full_name = ec_util.to_upper_or_none(row[2])
                st_type = ec_util.to_upper_or_none(row[3])
                #
                # street block ranges
                from_addr_l = ec_util.to_pos_int_or_none(row[4])
                if from_addr_l:
                    blocks_list.append(from_addr_l)
                to_addr_l = ec_util.to_pos_int_or_none(row[5])
                if to_addr_l:
                    blocks_list.append(to_addr_l)
                from_addr_r = ec_util.to_pos_int_or_none(row[6])
                if from_addr_r:
                    blocks_list.append(from_addr_r)
                to_addr_r = ec_util.to_pos_int_or_none(row[7])
                if to_addr_r:
                    blocks_list.append(to_addr_r)

                if blocks_list.__len__() == 0:
                    continue

                min_block = min(blocks_list)
                max_block = max(blocks_list)
                mean_block = round(int((min_block + max_block) / 2), -1)
                blocks_list = list()

                pwid = "-".join([full_name, str(mean_block)])
                if pwid.__len__() > 32:
                    pwid = name

                pwname = " ".join([full_name, "-".join([str(min_block), str(max_block)])])
                if pwname.__len__() > 64:
                    pwname = full_name

                source = ec_util.to_upper_or_none(row[8])
                global_id = row[9]
                shape = row[10]
                city = ec_util.to_upper_or_none(row[12])

                insert_cursor.insertRow((pwid, pwname, str_predir, name, full_name, st_type, from_addr_l, to_addr_l, from_addr_r, to_addr_r, source, global_id, shape, city))

    except Exception as e:
        logging.error("{} {}".format(inspect.stack()[0][3], e))

    finally:
        if edit:
            edit.stopOperation()
            logging.info("Saving changes for HGAC Starmap import")
            edit.stopEditing(save_changes=True)


def load_unique_street_types():
    con = None
    SQL_INSERT = "INSERT INTO address.unique_street_types(street_type) VALUES (?)"
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

    except psycopg2.Error as e:
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
    SQL_INSERT = "INSERT INTO address.unique_street_type_aliases(prefix,alias) VALUES (?,?)"
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

    except psycopg2.Error as e:
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
    SQL_INSERT = "INSERT INTO address.unique_prefix_aliases(prefix,alias) VALUES (?,?)"
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

    except psycopg2.Error as e:
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
    SQL_INSERT = "INSERT INTO address.unique_street_name_aliases(name,alias) VALUES (?,?)"
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

    except psycopg2.Error as e:
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
    SQL_INSERT = "INSERT INTO address.unique_full_street_names(name,prefix,type) VALUES (?,?,?)"
    SQL_DROP = "DROP TABLE IF EXISTS address.unique_full_street_names CASCADE"
    SQL_CREATE = "CREATE TABLE address.unique_full_street_names(" \
                 "id SERIAL NOT NULL," \
                 "name VARCHAR(100) NOT NULL," \
                 "prefix CHARACTER(1) NULL," \
                 "type VARCHAR(10) NULL," \
                 "CONSTRAINT unique_full_street_names_pkey PRIMARY KEY (id))"

    reader = None

    try:
        con = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")

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

    except psycopg2.Error as e:
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
    SQL_INSERT = "INSERT INTO address.unique_units(name) VALUES (?)"
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

    except psycopg2.Error as e:
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
    SQL_INSERT = "INSERT INTO address.street_exceptions(exception,name) VALUES (?,?)"
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

    except psycopg2.Error as e:
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
    SQL_INSERT = "INSERT INTO address.unique_prefixes(prefix) VALUES (?)"
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

    except psycopg2.Error as e:
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
        con = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")
        cur = con.cursor()
        cur.execute("SELECT alias FROM address.unique_street_type_aliases")
        rows = cur.fetchall()
        for row in rows:
            aliases.append(row[0])
        con.commit()

    except psycopg2.Error as e:
        logging.error(e)

    finally:
        if con:
            con.close()
    return (aliases)


def get_all_street_types():
    con = None
    street_types = []
    try:
        con = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")
        cur = con.cursor()
        cur.execute("SELECT street_type FROM address.unique_street_types")
        rows = cur.fetchall()
        for row in rows:
            street_types.append(row[0])
        con.commit()

    except psycopg2.Error as e:
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
        con = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")
        cur = con.cursor()
        cur.execute("SELECT alias, prefix FROM address.unique_prefix_aliases")
        rows = cur.fetchall()
        for row in rows:
            key = row[0]
            value = row[1]
            ec_hashmap.set(prefixes, key, value)
        con.commit()

    except psycopg2.Error as e:
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
        con = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")
        cur = con.cursor()
        sql = None
        street_name = None
        sql = "SELECT name FROM address.street_exceptions AS a WHERE a.exception = %(_exception_street_name)s"
        cur.execute(sql, {"_exception_street_name": exception_name})
        street_name = cur.fetchone()
        con.commit()
    except psycopg2.Error as e:
        logging.error(e)

    finally:
        if con:
            con.close()
    return (street_name)


def sql_get_street_name_by_alias(_alias):
    logging.debug("sql_get_street_name_by_alias {}".format(_alias))
    con = None
    street_name = None

    try:
        con = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")
        cur = con.cursor()
        street_name = None
        sql = "SELECT name FROM address.unique_street_name_aliases AS a WHERE a.alias = %(_alias)s"
        cur.execute(sql, {"_alias": _alias})
        street_name = cur.fetchone()
        con.commit()

    except Exception as e:
        logging.error("sql_get_street_name_by_alias: {}".format(e))

    except psycopg2.Error as e:
        logging.error("sql_get_street_name_by_alias: {}".format(e))

    finally:
        if con:
            con.close()
    return street_name


def sql_get_unique_street_name(_st_predir=None, _streetname=None, _st_postype=None):
    logging.debug("sql_get_unique_street_name: {}, {}".format(_st_predir, _streetname))
    address_dict = defaultdict(lambda: None)
    sql_list = list()

    try:
        con = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")
        cur = con.cursor()
        sql_dict = dict()
        sql = "SELECT DISTINCT(st_name), st_prefix, st_type FROM address.view_unique_street_names WHERE st_name = ?"
        sql_list.append(_streetname)
        sql_dict["_streetname"] = _streetname

        if _st_predir:
            sql = sql + " AND st_prefix = ?"
            sql_list.append(_st_predir)
            sql_dict["_st_predir"] = _st_predir
        # else:
        #     sql = sql + " AND a.st_predir IS NULL"

        if _st_postype:
            sql = sql + " AND st_type = ?"
            sql_list(_st_postype)
            sql_dict["_st_postype"] = _st_postype
        # else:
        #     sql = sql + " AND a.st_postype IS NULL"

        # cur.execute(sql, sql_dict)
        cur.execute(sql, sql_list)
        rows = cur.fetchall()

        if rows.__len__() == 0:
            sql_dict = dict()
            sql = "SELECT DISTINCT(st_name), st_prefix, st_type FROM address.view_unique_street_names WHERE st_name = %(_streetname)s"
            sql_dict["_streetname"] = _streetname

            if _st_predir:
                sql = sql + " AND st_prefix = %(_st_predir)s"
                sql_dict["_st_predir"] = _st_predir
            else:
                sql = sql + " AND st_prefix IS NULL"

            cur.execute(sql, sql_dict)
            rows = cur.fetchall()
            if rows.__len__() == 0:
                logging.error("Ambiguous address provided and could not determine a matching address for {}".format(sql))
                return None
            elif rows.__len__() > 1:
                logging.error("Multiple street types for {}".format(_streetname))
                return None

        row = rows[0]
        address_dict = defaultdict(lambda: None)
        st_predir = row[1]
        street_name = row[0]
        st_postype = row[2]
        if st_predir:
            st_predir = st_predir.strip().upper()
            address_dict['st_prefix'] = st_predir.strip().upper()
        if street_name:
            street_name = street_name.strip().upper()
            address_dict['st_name'] = street_name.strip().upper()
        if st_postype:
            st_postype = st_postype.strip().upper()
            address_dict['st_type'] = st_postype.strip().upper()

    except Exception as e:
        logging.error("sql_get_unique_street_name database: {}".format(e))

    except psycopg2.Error as e:
        logging.error("sql_get_unique_street_name database: {}".format(e))

    finally:
        if con:
            con.commit()
            con.close() - +1

    return address_dict


def load_unique_street_names():
    con = None
    SQL_INSERT_NAMES = "INSERT INTO address.unique_street_names(street_name,words,fuzzy) " \
                       "VALUES (?," \
                       "array_length(regexp_split_to_array(?, E'\\\s+'), 1), " \
                       "soundex(?))"
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

    except psycopg2.Error as e:
        logging.error(e)

    except OSError as e:
        logging.error(e)

    except IOError as e:
        logging.error(e)

    finally:
        if con:
            con.close()


def parse_house_number(_string):
    """
    Return a house number
    :rtype: int
    :param _string: the value representing the house number
    :return: is a digit representing the house number
    """
    house_number = None
    if _string.isdigit():
        house_number = int(_string)
    return (house_number)


def problem_address_tokenizer(_name):
    address_dict = defaultdict(lambda: None)

    pattern = re.compile("\.")
    _name = pattern.sub("", _name)

    pattern = re.compile("\s")
    result_split = pattern.split(_name)

    for idx, st_name in enumerate(result_split[1:]):
        for key, value in scourgify.address_constants.PROBLEM_ST_NAMES.items():
            if "FRANKE" == key and st_name == key:
                address_dict["add_number"] = int(result_split[0])
                address_dict["st_name"] = value
                address_dict["st_type"] = "ST"
                if result_split[result_split.__len__() - 1] != address_dict["st_name"]:
                    add_unit = get_unit(result_split[result_split.__len__() - 1])
                    if add_unit:
                        address_dict["add_unit"] = add_unit
                return address_dict

            if st_name == key and "SANDY" == key:
                #
                # looking for other SANDY streets by combining words
                if result_split[1:].__len__() > 1:
                    sandy_streets = " ".join([result_split[idx + 1], result_split[idx + 2]])
                    if sandy_streets == "SANDY CORNER":
                        address_dict["add_number"] = int(result_split[0])
                        address_dict["st_name"] = "SANDY CORNER"
                        address_dict["st_type"] = "RD"

                        if result_split[1:].__len__() > 2:
                            add_unit = get_unit(result_split[idx + 3])
                            if add_unit:
                                address_dict["add_unit"] = add_unit

                        return address_dict

            if st_name == key and "HEIGHTS" == key:
                address_dict["add_number"] = int(result_split[0])
                address_dict["st_name"] = result_split[1]
                address_dict["st_type"] = "AVE"
                if result_split[result_split.__len__() - 1] != address_dict["st_name"]:
                    add_unit = get_unit(result_split[result_split.__len__() - 1])
                    if add_unit:
                        address_dict["add_unit"] = add_unit
                break
            if st_name == key and "AVE" == key:
                st_name = " ".join([result_split[1], result_split[2]])
                for key, value in scourgify.address_constants.AVE_ST_NAMES.items():
                    if key == st_name:
                        address_dict["add_number"] = int(result_split[0])
                        address_dict["st_name"] = value
                        address_dict["st_type"] = "ST"
                        break

            if st_name == key and ("EAST" == key or "SOUTH" == key):
                address_dict["add_number"] = int(result_split[0])
                address_dict["st_name"] = result_split[1]
                address_dict["st_type"] = "ST"
                if result_split[result_split.__len__() - 1] != address_dict["st_name"]:
                    add_unit = get_unit(result_split[result_split.__len__() - 1])
                    if add_unit:
                        address_dict["add_unit"] = add_unit
                break
            elif st_name == key and "WEST" == key:
                #
                # looking for other WEST ... streets by combining words
                for idx1 in [0, 1, 2]:
                    west_street = " ".join([result_split[idx + idx1], result_split[idx + idx1 + 1]])

                    if west_street == "WEST LOOP":
                        address_dict["add_number"] = int(result_split[0])
                        address_dict["st_name"] = "WEST LOOP"

                        if result_split[1:].__len__() > 2:
                            add_unit = get_unit(result_split[idx + 3])
                            if add_unit:
                                address_dict["add_unit"] = add_unit

                        return address_dict

                    elif west_street == "WEST CREEK":
                        address_dict["add_number"] = int(result_split[0])
                        address_dict["st_name"] = "WEST CREEK"
                        address_dict["st_type"] = "DR"

                        return address_dict

                    elif west_street == "W WEST":
                        address_dict["add_number"] = int(result_split[0])
                        address_dict["st_type"] = "W"
                        address_dict["st_name"] = "WEST"
                        address_dict["st_type"] = "ST"

                        return address_dict

                    elif west_street == "E WEST":
                        address_dict["add_number"] = int(result_split[0])
                        address_dict["st_type"] = "E"
                        address_dict["st_name"] = "WEST"
                        address_dict["st_type"] = "ST"

                        return address_dict

    return address_dict


def address_parcer(_prefixes, _full_address_text):
    logging.debug("address_parcer: {}".format(_full_address_text))

    house_number = None
    street_name = None
    prefix = None
    type = None
    add_unit = None

    # load necessary arrays
    # street_types = get_all_street_types();
    # street_type_aliases = get_all_street_type_aliases();

    result = dict()

    address_dict = problem_address_tokenizer(_full_address_text)

    if not address_dict:
        try:
            result = scourgify.normalize_address_record(_full_address_text)
        except UnParseableAddressError as e:
            logging.debug("\tNOT PARSED: {}\n".format(_full_address_text))
            return None

        # todo lookup valid unit designators
        if result["address_line_2"]:
            add_unit = result["address_line_2"]

        tuple = usaddress.tag(result["address_line_1"])

        for key, value in tuple[0].items():
            if "AddressNumber" == key:
                house_number = value
                continue

            if "StreetName" == key:
                street_name = value
                continue

            if "StreetNamePreDirectional" == key:
                # EAST street name exception
                if "E" == value and 2 == tuple[0].__len__():
                    house_number = tuple[0]["AddressNumber"]
                    street_name = "EAST"
                    type = "ST"
                    break

                prefix = value
                continue

            if "StreetNamePostType" == key:
                type = value
                continue

            # CALHOUN A exception
            if "StreetNamePreType" == key and "CALHOUN" == value:
                house_number = tuple[0]["AddressNumber"]
                street_name = tuple[0]["StreetNamePreType"]
                if 4 == tuple[0].__len__():
                    add_unit = tuple[0]["StreetName"]
                break

            # AVE street name exception
            if "StreetNamePreType" == key and "AVE" == value:
                house_number = tuple[0]["AddressNumber"]
                street_name = " ".join([tuple[0]["StreetNamePreType"], tuple[0]["StreetName"]])
                break

            # US 59 HWY
            if "StreetNamePreType" == key and "US" == value:
                house_number = tuple[0]["AddressNumber"]
                if "59TH" == tuple[0]["StreetName"]:
                    street_name = " ".join([tuple[0]["StreetNamePreType"], "59"])
                type = "HWY"
                break

        address_dict = sql_get_unique_street_name(prefix, street_name, type)
    else:
        house_number = address_dict["add_number"]
        prefix = address_dict["st_prefix"]
        street_name = address_dict["st_name"]
        type = address_dict["st_type"]
        add_unit = address_dict["add_unit"]
        address_dict = sql_get_unique_street_name(address_dict["st_prefix"], address_dict["st_name"], address_dict["st_type"])

    if address_dict is None:
        street_name = sql_get_street_name_by_alias(street_name)
        address_dict = sql_get_unique_street_name(prefix, street_name, type)

    if address_dict:
        address_dict["add_number"] = house_number
        address_dict["add_unit"] = add_unit
        logging.debug("\tFOUND: {}\n".format(address_dict))
    else:
        logging.debug("\tNOT FOUND: {}\n".format(address_dict))
        address_dict = None

    return address_dict


def sql_address_validator(_idx,
                          _address_words,
                          _house_number,
                          _prefix,
                          _street_name,
                          _type,
                          _street_types,
                          _street_type_aliases):
    logging.debug("sql_address_validator: {}, {}, {}, {}, {}".format(_address_words, _house_number, _prefix, _street_name, _type))

    #
    # SQL lookup for address
    # based on the information provided

    address_dict = sql_get_unique_street_name(_prefix, _street_name, _type)
    unit = None

    if address_dict is None or address_dict.__len__() == 0:
        # could not find based on provided address
        # try using street name alias
        logging.info("Trying to find alias for {}".format(_street_name))
        street_name = sql_get_street_name_by_alias(_street_name)
        address_dict = sql_get_unique_street_name(_prefix, street_name, _type)
        if address_dict is None:
            return None

        # if not address:
        # check for street exceptions
        # street_name = get_street_name_by_exception(_prefix,_street_name)
        # address = get_street_name(_prefix, street_name)
    # else:
    # check for secondary unit values
    # if (address_word_count - _idx) > 1:
    #     unit = get_unit(_street_types, _street_type_aliases,
    #                     _address_words[address_word_count - 1].upper())
    if address_dict is not None and address_dict.__len__() > 0:
        address_dict["add_number"] = int(_house_number)
        address_dict["add_unit"] = unit

    return address_dict


def get_unit(_unit):
    unit = _unit
    # need to make sure its not a street type
    for street_type in scourgify.address_constants.STREET_TYPE_ABBREVIATIONS:
        if unit == street_type:
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
                        "VALUES (?,?,?,?,?,?,?,?,?,?,?)"
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
        con = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")

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
        fields_meter_address = ["account", "house_number", "prefix", "street_name", "suffix", "street_type", "secondary_name", "city", "state", "zip", "OID@", "SHAPE@"]

        arcpy.AcceptConnections(workspace, False)
        arcpy.DisconnectUser(workspace, "ALL")
        # arcpy.RegisterAsVersioned_management("Address", "EDITS_TO_BASE")

        if arcpy.Exists(workspace_meter_address):
            arcpy.Delete_management(workspace_meter_address)
        arcpy.CreateFeatureclass_management(workspace + os.sep + os.sep + "Address", "meter_address", "POINT", "", "", "", sr_2278)
        arcpy.AddField_management(workspace_meter_address, "account", "TEXT", "", "", 7, "Account", "NON_NULLABLE")
        arcpy.AddField_management(workspace_meter_address, "house_number", "SHORT", "", "", "", "House Number", "NON_NULLABLE")
        arcpy.AddField_management(workspace_meter_address, "prefix", "TEXT", "", "", 6, "Account", "NULLABLE")
        arcpy.AddField_management(workspace_meter_address, "street_name", "TEXT", "", "", 50, "Street Name", "NON_NULLABLE")
        arcpy.AddField_management(workspace_meter_address, "suffix", "TEXT", "", "", 10, "Suffix", "NULLABLE")
        arcpy.AddField_management(workspace_meter_address, "street_type", "TEXT", "", "", 4, "Street Type", "NULLABLE")
        arcpy.AddField_management(workspace_meter_address, "secondary_name", "TEXT", "", "", 10, "Secondary Name", "NULLABLE")
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
                address = address_parcer(prefixes, freeform)
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

    except psycopg2.Error as e:
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


def load_parcel_addresses(_from_shapefile, _cleanup):
    edit = None
    SQL_INSERT_ADDRESSES = "INSERT INTO address.address_CAD(add_number, st_prefix, st_name, st_type, add_unit, st_full_name, add_address, city, zip, source, fuzzy) " \
                           "VALUES (?,?,?,?,?,?,?,?,?,?,soundex(?)) ON CONFLICT ON " \
                           "CONSTRAINT addressCAD_name_idx DO NOTHING"

    try:
        #
        # drop/create table for schema address
        psql_connection = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")
        if _cleanup:
            setup_CAD_addresses_table(psql_connection)
            psql_connection = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")

        #
        # arcgis stuff for multi-users
        to_workspace = ec_arcpy_util.sde_workspace_via_host()
        arcpy.AcceptConnections(to_workspace, False)
        arcpy.DisconnectUser(to_workspace, "ALL")
        arcpy.env.workspace = to_workspace
        #
        # if first time, create featuredataset WhartonCAD
        ds = ec_arcpy_util.find_dataset("*WhartonCAD")
        sr_2278 = arcpy.SpatialReference(2278)
        if ds is None:
            ds = arcpy.CreateFeatureDataset_management(to_workspace, "WhartonCAD", sr_2278)
        #
        # if feature class exists, delete it
        fc = ec_arcpy_util.find_feature_class("*addressCAD", "WhartonCAD")
        if fc:
            arcpy.Delete_management("addressCAD")

        # Define Fields for CAD address
        arcpy.CreateFeatureclass_management(to_workspace + r"\WhartonCAD", "addressCAD", "POINT", "", "", "", sr_2278)
        arcpy.AddField_management(fc, "prop_id", "LONG", "", "", "", "CAD Prop ID", "NON_NULLABLE")
        arcpy.AddField_management(fc, "add_number", "LONG", "", "", "", "House Number", "NULLABLE")
        arcpy.AddField_management(fc, "st_prefix", "TEXT", "", "", 6, "Prefix", "NULLABLE")
        arcpy.AddField_management(fc, "st_name", "TEXT", "", "", 50, "Street Name", "NULLABLE")
        arcpy.AddField_management(fc, "st_type", "TEXT", "", "", 4, "Street Type", "NULLABLE")
        arcpy.AddField_management(fc, "add_unit", "TEXT", "", "", 20, "Unit", "NULLABLE")
        arcpy.AddField_management(fc, "st_fullname", "TEXT", "", "", 50, "Full Street Name", "NULLABLE")
        arcpy.AddField_management(fc, "add_address", "TEXT", "", "", 60, "Full Address", "NON_NULLABLE")
        arcpy.AddField_management(fc, "zip", "TEXT", "", "", 5, "ZIP", "NULLABLE")
        arcpy.AddField_management(fc, "city", "TEXT", "", "", 40, "City", "NULLABLE")
        arcpy.AddField_management(fc, "source", "TEXT", "", "", 40, "Data Source", "NON_NULLABLE")

        if not arcpy.Describe(ds).isVersioned:
            arcpy.RegisterAsVersioned_management(ds, "EDITS_TO_BASE")
        edit = arcpy.da.Editor(to_workspace)
        edit.startEditing(with_undo=False, multiuser_mode=True)
        edit.startOperation()

        to_fields = ["prop_id", "add_number", "st_prefix", "st_name", "st_type", "add_unit", "st_fullname", "add_address", "zip", "city", "source", "SHAPE@"]
        from_fields = ["prop_id", "situs_num", "situs_stre", "situs_st_1", "situs_st_2", "situs_city", "zip", "SHAPE@"]
        # street_types = get_all_street_types()
        # street_type_aliases = get_all_street_type_aliases()

        insert_cursor = arcpy.da.InsertCursor("addressCAD", to_fields)

        # where clause to restrict to EL CAMPO
        where_clause = "\"situs_num\" IS NOT NULL AND \"situs_city\" = 'EL CAMPO'"
        # where_clause = "\"prop_id\" > 0 AND \"situs_num\" IS NOT NULL AND \"situs_num\" != '0' AND \"situs_city\" = 'EL CAMPO'"
        # where_clause = """ "situs_num" IS NOT NULL AND "situs_num" <> '0' AND situs_city = 'EL CAMPO' """
        # where_clause = "\"prop_id\" > 0 AND \"situs_num\" IS NOT NULL AND \"situs_num\" != '0'"
        # where_clause = "\"prop_id\" > 0 AND \"situs_num\" IS NOT NULL AND \"situs_num\" != '0' AND \"situs_city\" = 'EL CAMPO' AND \"situs_st_1\" = 'MICHAEL'"

        prefixes = get_all_street_prefix_alias()

        with arcpy.da.SearchCursor(_from_shapefile, from_fields, where_clause) as cursor:
            for row in cursor:
                #
                # create defaultdict to hold address values
                # we want null values with invalid keys
                address_dict = defaultdict(lambda: None)
                address_dict["source"] = "WHARTONCAD"

                prop_id = ec_util.to_pos_int_or_none(row[0])
                if not prop_id:
                    continue

                add_number = ec_util.to_pos_int_or_none(row[1])
                if None is not add_number and 0 != add_number:
                    address_dict["add_number"] = add_number
                else:
                    # bad address number
                    continue

                if ec_util.to_upper_or_none(row[2]):
                    address_dict["st_prefix"] = ec_util.to_upper_or_none(row[2])

                if ec_util.to_upper_or_none(row[3]):
                    address_dict["st_name"] = ec_util.to_upper_or_none(row[3])
                    #
                    # street name contains illegal value
                    if "&" in address_dict["st_name"]:
                        continue

                if ec_util.to_upper_or_none(row[5]):
                    address_dict["city"] = ec_util.to_upper_or_none(row[5])

                if ec_util.to_pos_int_or_none(row[6]):
                    address_dict["zip"] = str(ec_util.to_pos_int_or_none(row[6]))

                if row[7]:
                    if row[7].centroid:
                        address_dict["point"] = row[7].centroid
                    else:
                        # bad centroid
                        continue
                else:
                    # no geometry
                    continue

                address_parced_dict = address_parcer(prefixes, full_address(address_dict))
                if address_parced_dict is None:
                    #
                    # No match found - log un-matchable address
                    address_dict["st_full_name"] = full_street_name(address_dict)
                    address_dict["add_address"] = full_address(address_dict)
                    sql_insert_not_found_address(psql_connection, address_dict)
                    continue

                if address_parced_dict.__len__() > 0 and address_parced_dict["add_number"] and address_parced_dict["st_name"]:
                    address_parced_dict["source"] = address_dict["source"]
                    address_parced_dict["st_full_name"] = full_street_name(address_parced_dict)
                    address_parced_dict["add_address"] = full_address(address_parced_dict)
                    address_parced_dict["city"] = address_dict["city"]
                    address_parced_dict["zip"] = address_dict["zip"]
                    address_parced_dict["point"] = address_dict["point"]

                    sql_insert_address(psql_connection, address_parced_dict, SQL_INSERT_ADDRESSES)

                    insert_cursor.insertRow([prop_id,
                                             address_parced_dict["add_number"],
                                             address_parced_dict["st_prefix"],
                                             address_parced_dict["st_name"],
                                             address_parced_dict["st_type"],
                                             address_parced_dict["add_unit"],
                                             address_parced_dict["st_full_name"],
                                             address_parced_dict["add_address"],
                                             address_parced_dict["zip"],
                                             address_parced_dict["city"],
                                             address_parced_dict["source"],
                                             address_parced_dict["point"]])


    except arcpy.ExecuteError as e:
        logging.error("FATAL ERROR in load_parcel_addresses: {}".format(e))

    except (psycopg2.Error, psycopg2.DatabaseError, psycopg2.OperationalError)  as e:
        logging.error("FATAL ERROR in load_parcel_addresses(database): {}".format(e))

    except Exception as e:
        logging.error("FATAL ERROR in load_parcel_addresses: {}".format(e))

    finally:
        if psql_connection:
            psql_connection.commit()
            psql_connection.close()

        if edit:
            edit.stopOperation()
            logging.info("Saving changes for WHARTON CAD address import")
            edit.stopEditing(save_changes=True)


def create_unique_tables():
    SQL_DROP_UNIQUE_STREETS = "DROP TABLE IF EXISTS address.unique_street_names CASCADE"
    SQL_CREATE_UNIQUE_STREETS = "CREATE TABLE address.unique_street_names(" \
                                "id SERIAL4 NOT NULL, " \
                                "st_prefix CHARACTER(6) NULL, " \
                                "st_name VARCHAR(50) NOT NULL, " \
                                "st_type VARCHAR(4) NULL, " \
                                "st_full_name VARCHAR(50) NOT NULL, " \
                                "zip CHARACTER(5) NULL, " \
                                "city VARCHAR(25) NULL, " \
                                "fuzzy CHARACTER(4) NULL, " \
                                "CONSTRAINT unique_street_names_pk PRIMARY KEY (id), " \
                                "CONSTRAINT unique_street_names_idx UNIQUE (st_full_name))"

    SQL_INSERT_UNIQUE_STREETS = "INSERT INTO address.unique_street_names(st_prefix, st_name, st_type, st_full_name, city, fuzzy) " \
                                "VALUES (?,?,?,?,?,soundex(?)) ON CONFLICT ON CONSTRAINT unique_street_names_idx DO NOTHING"

    SQL_QUERY_UNIQUE_STREETS_STARMAP = "SELECT DISTINCT(st_fullname), st_predir, st_name, st_type, city FROM sde.starmap"

    SQL_QUERY_UNIQUE_STREETS_911 = "SELECT DISTINCT(st_full_name), st_prefix, st_name, st_type, city FROM address.address_911 WHERE city = 'EL CAMPO'"

    SQL_QUERY_UNIQUE_STREETS_CAD = "SELECT DISTINCT(st_full_name), st_prefix, st_name, st_type, city FROM address.address_cad WHERE city = 'EL CAMPO'"

    try:
        #
        # drop/create table for schema address
        psql_connection = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")
        cur = psql_connection.cursor()
        cur.execute(SQL_DROP_UNIQUE_STREETS)
        cur.execute(SQL_CREATE_UNIQUE_STREETS)

        cur.execute(SQL_QUERY_UNIQUE_STREETS_STARMAP)
        rows = cur.fetchall()

        for row in rows:
            cur.execute(SQL_INSERT_UNIQUE_STREETS, [
                row[1],  # str_pre
                row[2],  # st_name
                row[3],  # st_type
                row[0],  # full name
                row[4],  # city
                row[2]  # fuzzy
            ])

        cur.execute(SQL_QUERY_UNIQUE_STREETS_911)
        rows = cur.fetchall()
        for row in rows:
            cur.execute(SQL_INSERT_UNIQUE_STREETS, [
                row[1],  # str_pre
                row[2],  # st_name
                row[3],  # st_type
                row[0],  # full name
                row[4],  # city
                row[2]  # fuzzy
            ])

        cur.execute(SQL_QUERY_UNIQUE_STREETS_CAD)
        rows = cur.fetchall()
        for row in rows:
            cur.execute(SQL_INSERT_UNIQUE_STREETS, [
                row[1],  # str_pre
                row[2],  # st_name
                row[3],  # st_type
                row[0],  # full name
                row[4],  # city
                row[2]  # fuzzy
            ])



    except psycopg2.Error as e:
        logging.error("create_unique_tables: {}".format(e))

    except Exception as e:
        logging.error("create_unique_tables: {}".format(e))

    finally:
        logging.error("completed create_unique_tables")
        if psql_connection:
            psql_connection.commit()
            psql_connection.close()


def load_incode_addresses(_con, _incode_file_path, _cleanup=True):
    r"INSERT INTO address.address_911(add_number, st_prefix, st_name, st_type, add_unit, st_full_name, add_address, city, zip, source, fuzzy) VALUES (?,?,?,?,?,?,?,?,?,?,soundex(?))"
    SQL_INSERT_ADDRESSES_INCODE = r"INSERT INTO address.address_incode(add_number, st_prefix, st_name, st_suffix, st_type, add_unit, add_full, add_source, add_zip, add_city, fuzzy) VALUES (?,?,?,?,?,?,?,?,?,?,soundex(?))"
    sql_server_connection = None

    try:
        address_dict_list = ec_incode.read_incode_address(_incode_file_path)

        sql_server_connection = _con
        if _cleanup:
            ec_addresses.setup_addresses_incode_table(sql_server_connection)
        for address_dict in address_dict_list:
            if address_dict is not None and address_dict.__len__() > 0:
                address_dict["full_addr"] = full_street_name(address_dict)
                ec_addresses.sql_insert_address(sql_server_connection, address_dict, SQL_INSERT_ADDRESSES_INCODE)


    except pyodbc.Error as e:
        if sql_server_connection:
            sql_server_connection.rollback()
            sql_server_connection.close()
        logging.error("{} {}".format(inspect.stack()[0][3], e))

    except Exception as e:
        logging.error("{} {}".format(inspect.stack()[0][3], e))

    finally:
        if sql_server_connection:
            logging.info("Saving changes for INCODE address import")
            sql_server_connection.commit()
            sql_server_connection.close()


def load_e911_addresses(_con, _from_workspace, _cleanup=True):
    edit = None
    SQL_INSERT_ADDRESSES_911 = r"INSERT INTO address.address_911(add_number, st_prefix, st_name, st_type, add_unit, st_full_name, add_address, city, zip, source, fuzzy) VALUES (?,?,?,?,?,?,?,?,?,?,soundex(?))"

    try:
        #
        # drop/create table for schema address
        sql_server_con = _con
        if _cleanup:
            setup_e911_addresses_tables(sql_server_con)
            # sql_server_con = ec_psql_util.psql_connection("ec", "sde", "sde", "localhost", "5432")

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
        fc = ec_arcpy_util.find_feature_class("*address911", "HGAC")
        if fc:
            arcpy.Delete_management("address911")

        #
        # Define Fields for HGAC E911 address features
        fc = arcpy.CreateFeatureclass_management(ds, "address911", "POINT", "", "", "", sr_2278)
        arcpy.AddField_management(fc, "add_number", "LONG", "", "", "", "House Number", "NON_NULLABLE")
        arcpy.AddField_management(fc, "st_prefix", "TEXT", "", "", 6, "Prefix", "NULLABLE")
        arcpy.AddField_management(fc, "st_name", "TEXT", "", "", 50, "Street Name", "NON_NULLABLE")
        arcpy.AddField_management(fc, "st_type", "TEXT", "", "", 4, "Street Type", "NULLABLE")
        arcpy.AddField_management(fc, "add_unit", "TEXT", "", "", 20, "Unit", "NULLABLE")
        arcpy.AddField_management(fc, "st_full_name", "TEXT", "", "", 50, "Full Street Name", "NON_NULLABLE")
        arcpy.AddField_management(fc, "add_address", "TEXT", "", "", 60, "Full Address", "NON_NULLABLE")
        arcpy.AddField_management(fc, "zip", "TEXT", "", "", 5, "ZIP", "NULLABLE")
        arcpy.AddField_management(fc, "city", "TEXT", "", "", 40, "City", "NULLABLE")
        arcpy.AddField_management(fc, "source", "TEXT", "", "", 40, "Data Source", "NON_NULLABLE")
        arcpy.AddField_management(fc, "global_id", "GUID", "", "", 10, "Global ID", "NON_NULLABLE")

        if not arcpy.Describe(ds).isVersioned:
            arcpy.RegisterAsVersioned_management(ds, "EDITS_TO_BASE")
        edit = arcpy.da.Editor(to_workspace)
        edit.startEditing(with_undo=False, multiuser_mode=True)
        edit.startOperation()

        #
        # feature class field list
        to_fc_fields = ["add_number", "st_prefix", "st_name", "st_type", "add_unit", "st_full_name", "add_address", "zip", "city", "source", "global_id", "SHAPE@"]
        insert_cursor = arcpy.da.InsertCursor("address911", to_fc_fields)

        from_fields = ["ADD_NUMBER", "ST_PREDIR", "STREETNAME", "ST_POSTYP", "UNIT", "POST_COMM", "POST_CODE", "SOURCE", "GLOBALID", "SHAPE@"]
        from_fc = _from_workspace + os.sep + "hgac911_address"

        # Iterator over HGAC supplied feature class
        with arcpy.da.SearchCursor(from_fc, from_fields) as cursor:
            for row in cursor:
                address_dict = defaultdict(lambda: None)

                address_dict["add_number"] = ec_util.to_pos_int_or_none(row[0])
                #
                # if no house number it can not be a valid address
                if None is address_dict["add_number"]:
                    continue

                address_dict["st_prefix"] = ec_util.to_upper_or_none(row[1])
                address_dict["st_name"] = ec_util.to_upper_or_none(row[2])
                #
                # if no street name it can not be a valid address
                if None is address_dict["st_name"]:
                    continue

                address_dict["st_type"] = ec_util.to_upper_or_none(row[3])
                address_dict["add_unit"] = ec_util.to_upper_or_none(row[4])
                address_dict["city"] = ec_util.to_upper_or_none(row[5])
                address_dict["zip"] = ec_util.to_upper_or_none(row[6])
                address_dict["source"] = ec_util.to_upper_or_none(row[7])
                address_dict["st_full_name"] = full_street_name(address_dict)
                address_dict["add_address"] = full_address(address_dict)
                global_id = row[8]
                point = row[9]

                insert_cursor.insertRow((address_dict["add_number"],
                                         address_dict["st_prefix"],
                                         address_dict["st_name"],
                                         address_dict["st_type"],
                                         address_dict["add_unit"],
                                         address_dict["st_full_name"],
                                         address_dict["add_address"],
                                         address_dict["zip"],
                                         address_dict["city"],
                                         address_dict["source"],
                                         global_id,
                                         point))

                # Insert into Address schema
                sql_insert_address(sql_server_con, address_dict, SQL_INSERT_ADDRESSES_911)

    except Exception as e:
        logging.error("{} {}".format(inspect.stack()[0][3], e))

    finally:
        if sql_server_con:
            sql_server_con.commit()
            sql_server_con.close()
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

    except psycopg2.Error as e:
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
