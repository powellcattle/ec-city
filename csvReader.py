import csv

of = open('meters.txt', 'w')
of.write('"meters", Dictionary\n')
of.write('"Meters", point, "", 1, seconds, 10, Code\n')
of.write('   "MIU", menu, normal, normal, Label1\n')
start_account = input("Enter Starting Account: ")
meterCount = input("Enter number Meters: ")
counter = 0

with open('../data/miu_numbers.csv', newline='') as csvfile:
    miu_reader = csv.reader(csvfile, delimiter='\t', quotechar='"')
    
    while counter < int(meterCount):
        rec = miu_reader.__next__()
        miu = int(rec[7])
        account = rec[2]
        
        if account == start_account or counter > 0:
            of.write('       "' + str(miu) + '"\n')
            counter = counter + 1
            
of.close()
