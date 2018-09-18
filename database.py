import postgresql
db = postgresql.open('pq://postgres:postgres@localhost:5432/elc')
get_type = db.prepare("SELECT st.type FROM street.street_type AS st")
for x in get_type():
	print(x[0])
