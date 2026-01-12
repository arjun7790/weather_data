import pymysql
import pandas as pd
import requests

# =====================================
# DATABASE CONNECTION
# =====================================
connector = pymysql.connect(
    user="root",
    host="localhost",
    password="root",
    port=3306,
    database="weather_events"
)

cursor = connector.cursor()
print(connector, "connected successfully✌")

# =====================================
# CITY → LATITUDE & LONGITUDE
# =====================================
CITY_COORDS = {
    "Bangalore": (12.9716, 77.5946),
    "Hyderabad": (17.3850, 78.4867),
    "Chennai": (13.0827, 80.2707)
}

# =====================================
# WEATHER RISK LOGIC
# =====================================
def get_weather_risk(precipitation):
    if precipitation is None:
        return "Unknown"
    if precipitation >= 5:
        return "High Risk"
    elif precipitation > 0:
        return "Medium Risk"
    else:
        return "Low Risk"

# =====================================
# EXTRACT: READ CSV (FORCE DATE FORMAT)
# =====================================
events_df = pd.read_csv("data/events.csv")

# Convert event_date safely to YYYY-MM-DD
events_df["event_date"] = pd.to_datetime(
    events_df["event_date"], errors="coerce"
).dt.strftime("%Y-%m-%d")

# =====================================
# ETL PIPELINE
# =====================================
for _, row in events_df.iterrows():

    event_name = row["event_name"]
    venue = row["venue"]
    city = row["city"]
    event_date = row["event_date"]   # NOW SAFE FOR MYSQL

    lat, lon = CITY_COORDS.get(city, (None, None))

    # DEFAULT WEATHER VALUES
    temperature = None
    precipitation = None
    weather_risk = "Unknown"

    # =====================================
    # WEATHER API CALL (SAFE)
    # =====================================
    if lat and lon:
        api_url = (
            f"https://api.open-meteo.com/v1/forecast?"
            f"latitude={lat}&longitude={lon}"
            f"&daily=temperature_2m_max,precipitation_sum"
            f"&start_date={event_date}&end_date={event_date}"
            f"&timezone=auto"
        )

        try:
            response = requests.get(api_url, timeout=10).json()

            if "daily" in response and response["daily"]["temperature_2m_max"]:
                temperature = response["daily"]["temperature_2m_max"][0]
                precipitation = response["daily"]["precipitation_sum"][0]
                weather_risk = get_weather_risk(precipitation)

        except Exception as e:
            print("Weather API issue:", e)

    # =====================================
    # LOAD: EVENTS
    # =====================================
    cursor.execute("""
        INSERT IGNORE INTO events (event_name, venue, city, event_date)
        VALUES (%s, %s, %s, %s)
    """, (event_name, venue, city, event_date))
    connector.commit()

    cursor.execute("""
        SELECT event_id FROM events
        WHERE event_name=%s AND venue=%s AND event_date=%s
    """, (event_name, venue, event_date))
    event_id = cursor.fetchone()[0]

    # =====================================
    # LOAD: WEATHER_FORECASTS
    # =====================================
    cursor.execute("""
        INSERT IGNORE INTO weather_forecasts
        (city, forecast_date, temperature, precipitation, weather_risk)
        VALUES (%s, %s, %s, %s, %s)
    """, (city, event_date, temperature, precipitation, weather_risk))
    connector.commit()

    cursor.execute("""
        SELECT weather_id FROM weather_forecasts
        WHERE city=%s AND forecast_date=%s
    """, (city, event_date))
    weather_id = cursor.fetchone()[0]

    # =====================================
    # LOAD: ENRICHED_EVENTS
    # =====================================
    cursor.execute("""
        INSERT IGNORE INTO enriched_events (event_id, weather_id)
        VALUES (%s, %s)
    """, (event_id, weather_id))
    connector.commit()

print("✅ ETL completed successfully")

cursor.close()
connector.close()
