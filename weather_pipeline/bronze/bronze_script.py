import requests
from dotenv import load_dotenv
import os
from datetime import datetime
import json

load_dotenv()
API_KEY = os.getenv('API_KEY')

lat = "32.088349"
lon = "34.887428"

URL = f"https://api.openweathermap.org/data/2.5/weather?lat={lat}&lon={lon}&appid={API_KEY}"

response = requests.get(URL)
raw_json = response.json()
# print("\n\n")


second = datetime.now().second
minute = datetime.now().minute
hour = datetime.now().hour
day = datetime.now().day
month = datetime.now().month
year = datetime.now().year

# print(f"{second} / {minute} / {hour} / {day} / {month} / {year}")

new_file_path = f"{os.path.dirname(__file__)}\\weather_raw_{day}{month}{year}_{hour}{minute}.txt"
# print(new_file_path)

with open(new_file_path, 'w', encoding='utf-8') as file:
    json.dump(raw_json, file, indent=4)

print(new_file_path)