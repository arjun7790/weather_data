# ================================
# Database: Real-Time Weather & Events Tracker
# ================================

CREATE DATABASE IF NOT EXISTS weather_events;
USE weather_events;

# ================================
# TABLE 1: events
# ================================
CREATE TABLE IF NOT EXISTS events (
    event_id INT AUTO_INCREMENT PRIMARY KEY,
    event_name VARCHAR(100) NOT NULL,
    venue VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    event_date DATE NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_event (event_name, venue, event_date)
);

# ================================
# TABLE 2: weather_forecasts
# ================================
CREATE TABLE IF NOT EXISTS weather_forecasts (
    weather_id INT AUTO_INCREMENT PRIMARY KEY,
    city VARCHAR(50) NOT NULL,
    forecast_date DATE NOT NULL,
    temperature DECIMAL(5,2),
    precipitation DECIMAL(5,2),
    weather_risk VARCHAR(20) NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    UNIQUE KEY uq_weather (city, forecast_date)
);

# ================================
# TABLE 3: enriched_events
# ================================
CREATE TABLE IF NOT EXISTS enriched_events (
    enriched_id INT AUTO_INCREMENT PRIMARY KEY,
    event_id INT NOT NULL,
    weather_id INT NOT NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    CONSTRAINT fk_event
        FOREIGN KEY (event_id)
        REFERENCES events(event_id)
        ON DELETE CASCADE,
    CONSTRAINT fk_weather
        FOREIGN KEY (weather_id)
        REFERENCES weather_forecasts(weather_id)
        ON DELETE CASCADE,
    UNIQUE KEY uq_event_weather (event_id, weather_id)
);
