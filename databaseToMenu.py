import postgresql

of = open('meters_.txt', 'w')
of.write('"meters", Dictionary\n')
of.write('"meters", point, "", 1, seconds, 10, Code\n')
of.write('   "miu", menu, normal, normal, Label1\n')
start_account = input("Enter Starting Account: ")
meterCount = input("Enter number Meters: ")
counter = 0
db = postgresql.open('pq://postgres:postgres@localhost:5432/elc')
get_meters = db.prepare("SELECT miu, account FROM utility.meter_import_view WHERE account>$1 ORDER BY account")
with db.xact():
	for row in get_meters(start_account):
		of.write('	   "' + row[0] + '"\n')
		counter = counter + 1
		if counter >= int(meterCount):
			break
		
of.close()




