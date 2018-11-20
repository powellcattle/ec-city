import scourgify
import geocoder
bing_api_key = "AulXAjib6WbCjGk5ZH56GwRvzWUziDz9gNcIElCSEVUEY0JWyhQj8XTWdBQAUUGw"
google_api_key = "AIzaSyCoZiK2xLl4iAzAEUnWSZEahqNWCuAr-HM"
add_string = "107 E. 1st ST"

result = scourgify.normalize_address_record(add_string)

# g = geocoder.bing(add_string,key=bing_api_key)
# print (g.geojason)
