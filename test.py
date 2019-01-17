import fuzzy
import pymongo
from pymongo import MongoClient

ENCODERS = {
    'soundex': fuzzy.Soundex(4),
    'nysiis': fuzzy.nysiis,
    'dmetaphone': fuzzy.DMetaphone(),
}

# parser = argparse.ArgumentParser(description='Search for a name in the database')
# parser.add_argument('algorithm', choices=('soundex', 'nysiis', 'dmetaphone'))
# parser.add_argument('name')
# args = parser.parse_args()

client = MongoClient("192.168.1.130", 27017)
db = client["address"]
addresses = db.address


# encoded_name = ENCODERS[args.algorithm](args.name)
# query = {args.algorithm:encoded_name}
# encoded_name = ENCODERS[args.algorithm](args.name)
query = {"st_name":"fuzzy.nysiis(CALHOUN)"}
# addresses = db.
try:
    # cols = addresses.find_one()
    # print(cols)
    # pass

    for address in addresses.find(query):
        print(address['st_name'])

except Exception as e:
    print(f"{e}")
