from scourgify import get_geocoder_normalized_addr

address = {
    'address_line_1': '1234 Main',
    'address_line_2': None,
    'city': 'Boring',
    'state': 'OR',
    'postal_code': '97000'
}
addr_str_return_value = "1234 Main Boring OR 97000"
get_geocoder_normalized_addr(address)
# mock_geocoder.google.assert_called_with(addr_str_return_value)