#!/usr/bin/python
# -*- coding: utf-8 -*-
import sys

import psycopg2

con = None
SQL_FIND_ACCOUNTS = "SELECT distinct(incode_account) FROM sde.meter t1 WHERE (SELECT COUNT(*) FROM sde.meter t2 WHERE t2.incode_account = t1.incode_account) > 1 ORDER BY incode_account"
SQL_REC_TO_UPDATE = "SELECT objectid FROM sde.meter m WHERE m.incode_account = %s"
SQL_UPDATE = "UPDATE sde.meter SET meter_count = %s WHERE objectid = %s"

try:
    con = psycopg2.connect(database='elc',
                           user='postgres',
                           password='postgres',
                           host='localhost')

    cur = con.cursor()
    cur_update = con.cursor()
    accounts = []

    cur.execute(SQL_FIND_ACCOUNTS)
    for rec in cur:
        accounts.append(rec[0])
    con.commit()

    for account in accounts:
        cur.execute(SQL_REC_TO_UPDATE, ([account]))
        count = 0
        for rec in cur:
            objectid = int(rec[0])
            count = count + 1
            cur_update.execute(SQL_UPDATE, [count, objectid])
            print(str(objectid) + " " + str(count))
            con.commit()

except psycopg2.DatabaseError, e:
    print 'Error %s' % e
    sys.exit(1)

else:
    sys.exit(0)

finally:
    if con:
        con.close()
