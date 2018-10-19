import scourgify

add_string = "107 East calhoun, EL CAMPO TX  77437"

result = scourgify.normalize_address_record(add_string)

result = scourgify.get_geocoder_normalized_addr(result)

print(True)