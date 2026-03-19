import h3pandas
import geopandas as gpd

def create_coastline_h3():
    """
    Generate H3 hexagonal grid cells of Japan by using the Japan's coastline.
    :return: coastline_h3: H3 hexagonal grid cells with h3_polyfill index and geometry columns.
    """

    # Load geometry file for bounding coastlines - EPSG:4326 - https://data.humdata.org/dataset/cod-xa-jpn
    coastline = gpd.read_file(
        "/Volumes/japan_rail/rail_station_changes/public_datasets/jpn_adm_2019_shp/jpn_admbnda_adm0_2019.shp"
    )

    # Set the grid size, 5 sets roughly 1700 grids
    resolution = 5

    # Convert the dataframe into h3 pandas
    coastline_h3 = coastline.h3.polyfill_resample(resolution)

    # Sort the h3 in same order every time to ensure order for grid numbers vs population stays same
    coastline_h3 = coastline_h3.sort_values('h3_polyfill')

    # Clean column names before saving, remove any newlines for h3_polyfill
    coastline_h3.columns = coastline_h3.columns.str.strip()

    # Return geopandas df
    return coastline_h3