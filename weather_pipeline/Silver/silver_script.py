import os
import json


bronze_path = os.path.join(os.path.dirname(__file__), "..", "bronze")
# print(bronze_path)

# bronze_contents = glob.glob(bronze_path) # * means all if need specific format then *.csv
# latest_file = max(bronze_contents, key=os.path.getctime)
# print(latest_file)

files = os.listdir(bronze_path)

latest = ""
for file in files:
    if (file > latest):
        latest = file

# print(latest)

latest_path =  os.path.join(os.path.dirname(__file__), "..", "bronze", latest)
# print(latest_path)


try:
    with open(latest_path, 'r') as file:
        data = json.load(file)
        dict_results = {
            "city_id": data['id'],
            "city_name": data['name'],
            "country": data['sys']['country'],
            "coord_lon": data['coord']['lon'],
            "coord_lat": data['coord']['lat'],
            "weather_id": data['weather'][0]['id'],
            "weather_main": data['weather'][0]['main'],
            "weather_description": data['weather'][0]['description'],
            "weather_icon": data['weather'][0]['icon'],
            "temp": data['main']['temp'],
            "feels_like": data['main']['feels_like'],
            "temp_min": data['main']['temp_min'],
            "temp_max": data['main']['temp_max'],
            "pressure": data['main']['pressure'],
            "humidity": data['main']['humidity'],
            "sea_level": data['main']['sea_level'],
            "grnd_level": data['main']['grnd_level'],
            "visibility": data['visibility'],
            "wind_speed": data['wind']['speed'],
            "wind_deg": data['wind']['deg'],
            "clouds_all": data['clouds']['all'],
            "sunrise": data['sys']['sunrise'],
            "sunset": data['sys']['sunset'],
            "data_time": data['dt'],
            "timezone": data['timezone'],
            "base": data['base'],
            "cod": data['cod'],
        }
except FileNotFoundError:
    print("File not found")
except json.JSONDecodeError as e:
    print("JSON error:", e)

print(dict_results)