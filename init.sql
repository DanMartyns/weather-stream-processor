CREATE TABLE weather_data (
    id SERIAL PRIMARY KEY,
    timestamp TIMESTAMP NOT NULL,
    lat DOUBLE PRECISION NOT NULL,  -- Latitude
    lon DOUBLE PRECISION NOT NULL,  -- Longitude
    hourly_precipitation DOUBLE PRECISION NOT NULL  -- Hourly precipitation in mm
);
