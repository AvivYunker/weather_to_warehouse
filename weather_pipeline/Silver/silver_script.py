import os
import json
import pandas as pd
from datetime import datetime
import numpy as np


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

# print(dict_results)

# Rename key "city_id" to be "City ID"

# convert the python dictionary to a dataframe
df = pd.DataFrame([dict_results])  # wrap the dict in a list

# change the name of each of the keys of the dataframe:
df = df.rename(columns={
    "city_id": "City ID",
    "city_name": "City Name",
    "country": "Country",
    "coord_lon": "Longitude",
    "coord_lat": "Latitude",
    "weather_id": "Weather ID",
    "weather_main": "Weather Main",
    "weather_description": "Weather Description",
    "weather_icon": "Weather Icon",
    "temp": "Temperature",
    "feels_like": "Feels Like",
    "temp_min": "Minimum Temperature",
    "temp_max": "Maximum Temperature",
    "pressure": "Pressure",
    "humidity": "Humidity",
    "sea_level": "Sea Level",
    "grnd_level": "Ground Level",
    "visibility": "Visibility",
    "wind_speed": "Wind Speed",
    "wind_deg": "Wind Degrees",
    "clouds_all": "Cloudiness Percentage",
    "sunrise": "Sunrise Time",
    "sunset": "Sunset Time",
    "data_time": "Time of Data",
    "timezone": "Timezone",
    "base": "Base",
    "cod": "HTTP Status Code"
})

# Convert the UNIX times to Human Readable times:
df["Sunrise Time"] = pd.to_datetime(df["Sunrise Time"], unit="s")
df["Sunset Time"] = pd.to_datetime(df["Sunset Time"], unit="s")
df["Time of Data"] = pd.to_datetime(df["Time of Data"], unit="s")

# Convert the tempereature to Celcius:
def to_celsius (temperature):
    if (temperature > 100):
        temperature -= 273.15
    return temperature

temperature_columns = ["Temperature", "Feels Like", "Minimum Temperature", "Maximum Temperature"]

for temp in temperature_columns:
    df[temp] = df[temp].apply(to_celsius)


# Convert the numeric values to :
float_cols = ["Temperature", "Feels Like", "Minimum Temperature", "Maximum Temperature", "Wind Speed", "Longitude", "Latitude"]
int_cols = ["Pressure", "Humidity", "Sea Level", "Ground Level", "Visibility", "Wind Degrees", "Cloudiness Percentage"]

df[float_cols] = df[float_cols].apply(pd.to_numeric, errors="coerce")
df[int_cols] = df[int_cols].apply(pd.to_numeric, errors="coerce").astype("Int64")


# dropping columns that are unnecessary for alanysis:
df = df.drop('Base', axis=1)
df = df.drop('HTTP Status Code', axis=1)
df = df.drop('Timezone', axis=1)
df = df.drop('Weather Icon', axis=1)

# Filling NaN values with "Unknown" strings
string_columns = ["City Name", "Country", "Weather Main", "Weather Description"]
numeric_columns = ["Temperature", "Feels Like", "Minimum Temperature", "Maximum Temperature", "Pressure", "Humidity", "Sea Level", "Ground Level", "Visibility", "Wind Speed", "Wind Degrees", "Cloudiness Percentage", "Longitude", "Latitude"]

df[string_columns] = df[string_columns].fillna("Unknown")
df[numeric_columns] = df[numeric_columns].fillna(0)

# add metadata to the DataFrame:
df["Pipeline Version"] = "v1.0"
df["Data Source"] = "OpenWeather API"
df["Created By"] = "Aviv Yunker"
df["Load Datetime"] = pd.Timestamp.now()

# save the DataFrame to a new CSV file

second = datetime.now().second
minute = datetime.now().minute
hour = datetime.now().hour
day = datetime.now().day
month = datetime.now().month
year = datetime.now().year

df.to_csv(f"SilverCSV_{day}{month}{year}_{hour}{minute}.csv", sep="\t", encoding='utf-8')

# Adding a new "Temperature Range" column
df["Temperature Range"] = df["Maximum Temperature"] - df["Minimum Temperature"]
print(df["Temperature Range"])

# Adding a new "Day Length" column:
df["Day Length"] = df["Sunset Time"] - df["Sunrise Time"]
print(df["Day Length"])


# Adding a new Daytime / Nightime radio column:
df["Is Daytime"] = (df["Time of Data"] >= df["Sunrise Time"]) & (df["Time of Data"] < df["Sunset Time"])


# Adding a new "Weather Category" column
# df["Weather Category"] = np.where(
#     df["Weather Main"] == "Clear", "Clear",
#         np.where(df["Weather Main"] == "Clouds", "Cloudy",
#             np.where(df["Weather Main"] == "Rain", "Rain",
#                 np.where(df["Weather Main"] == "Drizzle", "Rain",
#                     np.where(df["Weather Main"] == "Thunderstorm", "Storm",
#                         np.where(df["Weather Main"] == "Snow", "Snow",
#                             np.where(df["Weather Main"] == "Mist", "Low Visibility",
#                                 np.where(df["Weather Main"] == "Fog", "Low Visibility",
#                                     np.where(df["Weather Main"] == "Haze", "Low Visibility", "Other")
#                                 )
#                             )
#                         )
#                     )
#                 )
#             )
#         )
# )

# Adding a new "Weather Category" column
weather_map = {
    "Clear": "Clear",
    "Clouds": "Cloudy",
    "Rain": "Rain",
    "Drizzle": "Rain",
    "Thunderstorm": "Storm",
    "Snow": "Snow",
    "Mist": "Low Visibility",
    "Fog": "Low Visibility",
    "Haze": "Low Visibility",
}
df["Weather Category"] = df["Weather Main"].map(weather_map).fillna("Other")

#⭐ 6️⃣ Visibility in km

# Adding a new "Celcius to Fahrenheit" column:
def to_fahrenheit(celcius_temp):
    return (celcius_temp * 9 / 5) + 32

df["Temperature in Fahrenheit"] = df["Temperature"].apply(to_fahrenheit)

# Adding a new "visiblity in km" column:
df["Visibility in KM"] = df["Visibility"] / 1000