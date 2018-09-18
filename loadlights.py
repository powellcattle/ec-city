#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys
import csv

import psycopg2


con = None
SQL_INSERT_LIGHT = "INSERT INTO utility.light(geom,watts,lumens,owner,bulb,application,contract)" \
                   "VALUES (ST_Transform(ST_SetSRID(ST_MakePoint(%s,%s),4326),2278),%s,%s,%s,%s,%s,%s)"
SQL_DROP_LIGHT = "DROP TABLE utility.light"
SQL_CREATE_LIGHT = "CREATE TABLE utility.light(" \
                   "light_id serial NOT NULL," \
                   "application character varying(40)," \
                   "bulb character varying(25)," \
                   "contract character varying(10)," \
                   "geom geometry(Geometry,2278)," \
                   "lumens integer NOT NULL," \
                   "owner character varying(35) NOT NULL," \
                   "watts integer," \
                   "CONSTRAINT light_pkey PRIMARY KEY (light_id))" \
                   "WITH (OIDS=FALSE)"
SQL_ALTER_LIGHT = "ALTER TABLE utility.light OWNER TO sde"
SQL_GRANT_LIGHT = "GRANT ALL ON TABLE utility.light TO sde"

try:
    con = psycopg2.connect(database='elc',
                           user='postgres',
                           password='postgres',
                           host='powellcattle.com')

    cur = con.cursor()
    cur.execute(SQL_DROP_LIGHT)
    con.commit()
    cur.execute(SQL_CREATE_LIGHT)
    con.commit()
    cur.execute(SQL_ALTER_LIGHT)
    con.commit()
    cur.execute(SQL_GRANT_LIGHT)
    con.commit()

    with open('../data/lights2012.csv', 'rb') as csvfile:
        reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
        next(reader, None)
        for row in reader:
            watts = row[0]
            lumens = row[1]
            owner = 'AEP'
            bulb = row[3].upper()
            application = row[5].upper()
            contract = row[6].upper()
            y = row[13]
            x = row[14]

            cur = con.cursor()
            cur.execute(SQL_INSERT_LIGHT, (x, y, watts, lumens, owner, bulb, application, contract))

    con.commit()

except psycopg2.DatabaseError, e:
    print 'Error %s' % e
    sys.exit(1)

else:
    sys.exit(0)

finally:
    if con:
        con.close()
    if csvfile:
        csvfile.close()