import folium
import geopandas as gpd
from geopandas import GeoSeries
import h3
import numpy as np
import pandas as pd
from shapely.geometry import Point

class GridInfo:
    """
    Create a class for grid info. Stores attributes and methods for each H3 grid.
    """

    # Constructor for grid essentials
    def __init__(self, gid, grid_centroid, geometry):
        self.id = gid
        self.h3_id = grid_centroid
        self.centroid = h3.cell_to_latlng(grid_centroid)
        self.centroid_to_station = 0
        self.geography = geometry
        self.population = 0
        self.station_count = 0
        self.stations = []

    # Check if the passed coordinates are within the geometry bounds
    def check_bounds(self, coordinates):

        # Pull lat/long
        bounding_latitude = coordinates[0]
        bounding_longitude = coordinates[1]

        # Check bounds
        point = Point(bounding_longitude, bounding_latitude)
        return self.geography.contains(point)

    # Return simple id
    def get_id(self):
        return self.id

    # Return the h3 cell id
    def get_h3_id(self):
        return self.h3_id

    # return centroid lat/long
    def get_centroid(self):
        return self.centroid

    # Set distance from centroid to nearest station
    def set_centroid_to_station(self, centroid_distance):
        self.centroid_to_station = centroid_distance

    # Return centroid distance to nearest station
    def get_centroid_to_station(self):
        return self.centroid_to_station / 1000

    # Update population within that grid
    def set_population(self, grid_population):

        # Cast to integer from rasterstats
        self.population = int(grid_population)

    # Return population for that grid
    def get_population(self):
        return self.population

    # Add to station count within that grid
    def add_station(self, station_name):

        # Add to station list
        self.stations.append(station_name)

        # Increment counter
        self.station_count += 1

    # Return station count
    def get_station_count(self):

        # Return length of station array
        return self.station_count

    # Return station list array
    def get_station_list(self):
        return self.stations

    # Return stations per capita 10k
    def get_per_capita(self):

        # Prevent divide by zero
        if self.population == 0:
            return None

        # Return station count per 10,000
        else:
            return (self.station_count / self.population) * 10000

def grid_array_create_df(grid_array):
    """
    Takes in a grid_array list of objects and creates a final dataframe from it. Using built in methods to generate attributes
    :param grid_array:
    :return: final_df
    """

    # Define final dataframe
    columns = ["h3_id", "centroid", "centroid_to_station_km", "population", "station_count", "station_list", "per_capita_10k"]
    rows = []

    # Iterate over grid info
    for grid_obj in grid_array:
        row = {
            "h3_id": grid_obj.get_id(),
            "centroid": grid_obj.get_centroid(),
            "centroid_to_station_km": grid_obj.get_centroid_to_station(),
            "population": grid_obj.get_population(),
            "station_count": grid_obj.get_station_count(),
            "station_list": grid_obj.get_station_list(),
            "per_capita_10k": grid_obj.get_per_capita()
        }
        rows.append(row)

    # Create final dataframe
    final_df = pd.DataFrame(rows, columns=columns)

    # Return dataframe
    return final_df

def grid_assign_stations(coastline_h3, current_stations, grid_array):
    """
    Assign stations to grid array based on coordinates
    :param coastline_h3:
    :param current_stations:
    :param grid_array:
    :return: grid_array (updated)
    """

    # Tracker for id
    grid_id = 0

    # Convert stations to gdf
    stations_gdf = gpd.GeoDataFrame(
        current_stations,
        geometry=gpd.points_from_xy(
            current_stations['longitude'],
            current_stations['latitude']
        ),
        crs="EPSG:4326"
    )

    # Spacial join the dataframes - left to keep coastline h3 cells geometry
    df_joined = gpd.sjoin(
        coastline_h3,
        stations_gdf,
        how='left',
        predicate='contains'
    )

    # Group stations by H3 cell
    stations_groups = (
        df_joined
        .groupby('h3_polyfill')['name']
        .apply(lambda stations: stations.dropna().tolist())
    )

    # Create GridInfo array
    for h3_cell, row in coastline_h3.iterrows():

        # GridInfo object
        grid_obj = GridInfo(grid_id, h3_cell, row['geometry'])

        # Check if the station latitude and longitude within the grid
        station_list = stations_groups.get(h3_cell, [])

        # Iterate over stations list
        for station in station_list:
            grid_obj.add_station(station)

        # Add grid object to array
        grid_array.append(grid_obj)

        # Increment custom grid_id counter
        grid_id += 1

    # Return updated df
    return grid_array

def grid_assign_classification(final_df):
    """
    Assign classification to grid, rural vs urban
    :param
        final_df:
    :return:
        classify_final_df (final dataframe with urban column added)
        data (heatmap data0
        m (map object)
    """

    # Copy dataframe
    classify_final_df = final_df.copy()

    # Add new df column
    classify_final_df.insert(len(classify_final_df.columns), 'classification', 0)

    # Iterate over dataframe and update classification
    classify_final_df['urban'] = np.where(classify_final_df['population'] > 50000, 1, 0)

    # Define folium map - centered on Toyama
    m = folium.Map(location=[36.70, 137.21], zoom_start=6)

    # Define data array for heatmap
    data = []

    # Iterate over station list and map on folium
    data_df = pd.DataFrame(classify_final_df['centroid'].tolist(), columns=['Longitude', 'Latitude'])

    # Add classification to dataframe
    data_df['urban'] = classify_final_df['urban'].astype(int)

    # Convert new df to array
    data = data_df.to_numpy()

    return classify_final_df, data, m

def assign_grid_centroid_neighbors(stations_list, grid_array):
    """
    Assign centroid distance to closest neighbor station
    :param stations_list:
    :param grid_array:
    :return: grid_array (updated)
    """

    # Pull all centroids from the grid_array
    centroids_geo_series = GeoSeries.from_xy(
        [grid_obj.get_centroid()[1] for grid_obj in grid_array],
        [grid_obj.get_centroid()[0] for grid_obj in grid_array],
        crs="EPSG:4326"
    )

    # Pull all latitude and longitude from stations list
    stations_geo_series = GeoSeries.from_xy(
        stations_list['longitude'],
        stations_list['latitude'],
        crs="EPSG:4326"
    )

    # Project to same CRS
    stations_proj = stations_geo_series.to_crs(crs="EPSG:32654")
    centroid_proj = centroids_geo_series.to_crs(crs="EPSG:32654")

    # Use built in GeoSeries comparison to calculate distances and take minimum
    distances = centroid_proj.apply(lambda x: stations_proj.distance(x).min())

    # Iterate over distances to pull the shortest distances for each grid
    for grid_obj, distance in zip(grid_array, distances):
        grid_obj.set_centroid_to_station(distance)

    # Return updated grid_array
    return grid_array