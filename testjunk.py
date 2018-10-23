from collections import defaultdict

from ec_addresses import sql_get_unique_street_name

name = "LIBERTY"
prefix = "S"
type = None

# prefixes = get_all_street_prefix_alias()

address_dict = sql_get_unique_street_name(prefix, name, type)
print(address_dict)