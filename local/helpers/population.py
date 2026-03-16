import geopandas as gpd
from rasterstats import zonal_stats

def population_assign_grid_array(grid_array, raster_file):
    """
    Assigns population to the grid array per grid and return total for all grids
    :param grid_array: grid_array of grid objects for each h3 grid
    :param raster_file: population raster file
    :return: total population for all h3 grids in the raster file provided
    """

    # Read in h3 shape file, this will not change for the project
    shp = 'data/h3-polygons.shp'
    shp_gdf = gpd.read_file(shp)

    # Stats for each feature
    stats = zonal_stats(shp, raster_file, stats=['sum'])

    all_grids_total = 0.0

    # Create lookup dictionary for grids
    grid_lookup = {grid_obj.get_h3_id(): grid_obj for grid_obj in grid_array}

    # Print Stats for each zone
    for index, zone in enumerate(stats):

        # print(f'Zone {index}: {zone['sum']}')
        if zone['sum']:

            # Update the total sum
            all_grids_total += zone['sum']

            # Check the zone vs grid object
            h3_index = shp_gdf.iloc[index, 0]

            # Check that grid_array centroid matches population of the raster clip
            if h3_index in grid_lookup:

                # Add the population to the data object
                grid_lookup[h3_index].set_population(zone['sum'])

            else:
                print(f'[-] H3 Index {h3_index} not found in grid array')

    # return total count
    return all_grids_total