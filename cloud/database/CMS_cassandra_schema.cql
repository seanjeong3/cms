-- Create Keyspace

CREATE KEYSPACE IF NOT EXISTS cms_system
WITH REPLICATION = { 'class' : 'NetworkTopologyStrategy', 'DC1' : 1 };

-- Sensor Data

CREATE TABLE IF NOT EXISTS cms_system.sensor_data_raw_regular
(sensor_id text, year text, event_time timestamp, data list<double>, PRIMARY KEY((sensor_id, year), event_time));

CREATE TABLE IF NOT EXISTS cms_system.sensor_data_raw_abnormal
(sensor_id text, year text, event_time timestamp, data list<double>, PRIMARY KEY((sensor_id, year), event_time));

CREATE TABLE IF NOT EXISTS cms_system.sensor_data_preprocessed
(sensor_id text, year text, event_time timestamp, min double, max double, PRIMARY KEY((sensor_id, year), event_time));