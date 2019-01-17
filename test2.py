
import psycopg2

import ec_psql_util




con = ec_psql_util.psql_connection()
sql = r"SELECT distinct(st_name), st_predir, st_type, st_fullname, city FROM sde.starmap"
cur = con.cursor()
cur.execute(sql)
rows = cur.fetchall()
con.commit()

sql_insert = r"INSERT INTO address.unique_street_names(st_name, st_prefix, st_type, st_full_name, city) " \
             r"VALUES (%s,%s,%s,%s,%s) ON CONFLICT ON CONSTRAINT unique_street_name_unique DO NOTHING"

con_insert = ec_psql_util.psql_connection()
cur_insert = con_insert.cursor()

for row in rows:
    cur_insert.execute(sql_insert,[
        str(row[0]),
        str(row[1]),
        str(row[2]),
        str(row[3]),
        str(row[4])
    ])


