import pymysql
import pandas as pd
import matplotlib.pyplot as plt

# ================================
# DATABASE CONNECTION
# ================================
conn = pymysql.connect(
    user="root",
    host="localhost",
    
    password="root",
    database="weather_events",
    port=3306
)

print("Connected to database")

# ================================
# QUERY 1: High Risk Events per Venue
# ================================
query1 = """
SELECT e.venue, COUNT(*) AS high_risk_events
FROM enriched_events ee
JOIN events e ON ee.event_id = e.event_id
JOIN weather_forecasts w ON ee.weather_id = w.weather_id
WHERE w.weather_risk = 'High Risk'
GROUP BY e.venue;
"""

df_high_risk = pd.read_sql(query1, conn)

# Plot 1: Bar chart
plt.figure()
plt.bar(df_high_risk["venue"], df_high_risk["high_risk_events"])
plt.title("High Risk Weather Events per Venue")
plt.xlabel("Venue")
plt.ylabel("Number of High Risk Events")
plt.xticks(rotation=45)
plt.tight_layout()
plt.show()

# ================================
# QUERY 2: Average Temperature by City
# ================================
query2 = """
SELECT city, AVG(temperature) AS avg_temp
FROM weather_forecasts
WHERE temperature IS NOT NULL
GROUP BY city;
"""

df_avg_temp = pd.read_sql(query2, conn)

# Plot 2: Line chart
plt.figure()
plt.plot(df_avg_temp["city"], df_avg_temp["avg_temp"], marker='o')
plt.title("Average Forecast Temperature by City")
plt.xlabel("City")
plt.ylabel("Average Temperature (°C)")
plt.tight_layout()
plt.show()

# ================================
# QUERY 3: Weather Risk Distribution
# ================================
query3 = """
SELECT weather_risk, COUNT(*) AS count
FROM weather_forecasts
GROUP BY weather_risk;
"""

df_risk_dist = pd.read_sql(query3, conn)

# Plot 3: Pie chart
plt.figure()
plt.pie(df_risk_dist["count"], labels=df_risk_dist["weather_risk"], autopct='%1.1f%%')
plt.title("Weather Risk Distribution")
plt.tight_layout()
plt.show()

conn.close()
