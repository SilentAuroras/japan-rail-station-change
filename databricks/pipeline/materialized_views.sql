-- 2016 stations list materialized views
CREATE OR REFRESH MATERIALIZED VIEW stations_2016
AS SELECT *
FROM japan_rail.rail_station_changes.stations_list
WHERE year = 2016;

-- 2025 stations list materialized views
CREATE OR REFRESH MATERIALIZED VIEW stations_2025
AS SELECT *
FROM japan_rail.rail_station_changes.stations_list
WHERE year = 2025;

-- 2016 grids array materialized views
CREATE OR REFRESH MATERIALIZED VIEW grids_2016
AS SELECT *
FROM japan_rail.rail_station_changes.grid_details
WHERE year = 2016;

-- 2025 grids array materialized views
CREATE OR REFRESH MATERIALIZED VIEW grids_2025
AS SELECT *
FROM japan_rail.rail_station_changes.grid_details
WHERE year = 2025;