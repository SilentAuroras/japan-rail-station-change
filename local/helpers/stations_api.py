import os
import overpy
import pandas as pd

def list_stations(year):
    """
    Retrieves all stations within a given year
    :param year: year to retrieve stations for
    :return: stations_df: dataframe of all stations within a given year (name, latitude, longitude)
    """

    # Create Overpass API object
    api = overpy.Overpass()

    # Current dataset does not need year
    if year == 2025:
        query_date = ""

    # Pass in date to allow previous years
    else:
        query_date = f'[date:"{year}-01-01T00:00:00Z"]'

    # Query rail stations in Japan using ISO 3166-1 code for Japan (JP)
    # Do not return geometry, just latitude and longitude
    query = f'''
    [out:json][timeout:300]{query_date};
    area["ISO3166-1"="JP"]["admin_level"="2"]->.searchArea;
    node
      ["railway"="station"]
      ["public_transport"="station"]
      (area.searchArea);
    out;
    '''

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

        # Send the request to overpy
        results = api.query(query)

        # Stations list: name, lat, long
        stations = []

        # Iterate over nodes
        for node in results.nodes:

            # Pull station name, latitude, longitude
            name = node.tags.get("name")
            latitude = node.lat
            longitude = node.lon

            # If the station name exists, generate row
            if name:
                stations.append({"name": name, "latitude": latitude, "longitude": longitude})

        # Generate dataframe for the results
        stations_df = pd.DataFrame(stations)

        # This parquet will not change, so only create if it doesn't exist
        stations_df.to_parquet(file_name, index=False, engine='fastparquet')

        # Return
        return stations_df