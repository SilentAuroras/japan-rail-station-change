-- Create catalog for project
CREATE CATALOG japan_rail;

-- Create schema for project
CREATE SCHEMA japan_rail.rail_station_changes;

-- Create a volume to upload larger datasets files
CREATE VOLUME japan_rail.rail_station_changes.public_datasets;