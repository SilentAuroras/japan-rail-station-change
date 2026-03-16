import os
import osmium
import pandas as pd

# API has issues with historical data
# Download the OSM and parse locally
#   - https://wiki.openstreetmap.org/wiki/Planet.osm
# Download specific country from mirror
#   - 2016 - https://download.geofabrik.de/asia/japan-160101.osm.pbf
#   - 2025 - https://download.geofabrik.de/asia/japan-250101.osm.pbf

# Parse locally with osmium
#   - https://wiki.openstreetmap.org/wiki/Osmium

class OsmiumHandler(osmium.SimpleHandler):
    """
    Handler for osmium parsing.
    """

    # Constructor with stations list
    def __init__(self):
        osmium.SimpleHandler.__init__(self)
        self.stations = []

    # Node handler function
    def node(self, n):
        if n.tags.get('railway') == 'station':
            self.stations.append({
                'name': n.tags.get('name', 'Unnamed'),
                'latitude': n.location.lat,
                'longitude': n.location.lon,
            })

def list_stations(year):
    """
    Retrieves all stations within a given year
    :param year: year to retrieve stations for
    :return: station_df: dataframe of all stations within a given year (name, latitude, longitude)
    """

    # Set file name with name in it
    file_name = f'data/stations-list-{year}.parquet'

    # If station list already exists, read in as does not change
    if os.path.exists(file_name):

        print("Stations parquet already exists, reading in...")
        stations_df = pd.read_parquet(file_name)

        # Return
        return stations_df

    # Save the station list as parquet backup
    else:

        # Stations list: name, lat, long
        stations = []

        # Create handler object
        handler = OsmiumHandler()

        # Read in the file
        if year == 2016:
            osm_file_name = 'public-datasets/japan-160101.osm.pbf'
        else:
            osm_file_name = 'public-datasets/japan-250101.osm.pbf'

        # Parse the file
        handler.apply_file(osm_file_name)

        # Iterate over results
        for station in handler.stations:

            # Pull station name, latitude, longitude
            name = station['name']
            latitude = station['latitude']
            longitude = station['longitude']

            # If the station name exists, generate row
            if name:
                stations.append({"name": name, "latitude": latitude, "longitude": longitude})

        # Generate dataframe for the results
        stations_df = pd.DataFrame(stations)

        # This parquet will not change, so only create if it doesn't exist
        stations_df.to_parquet(file_name, index=False, engine='fastparquet')

        # Return
        return stations_df