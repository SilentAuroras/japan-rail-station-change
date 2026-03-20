import pandas as pd
from pyspark import pipelines as dp
from pyspark.sql import types as T
from grids import grid_assign_stations, grid_array_create_df, assign_grid_centroid_neighbors
from coastline_h3_generate import create_coastline_h3

# Define schema, pulled from grid_array_create_df
SCHEMA = T.StructType([
    T.StructField("h3_id", T.IntegerType(), True),
    T.StructField("centroid", T.ArrayType(T.DoubleType()), True),
    T.StructField("centroid_to_station_km", T.DoubleType(), True),
    T.StructField("population", T.IntegerType(), True),
    T.StructField("station_count", T.IntegerType(), True),
    T.StructField("station_list", T.ArrayType(T.StringType()), True),
    T.StructField("per_capita_10k", T.DoubleType(), True),
    T.StructField("year", T.StringType(), True),
])

@dp.table(
    name="grid_details"
)
def create_grid_array():

    # Get coastline h3 gdf
    coastline_h3 = create_coastline_h3()

    # Get all stations from the table
    stations_list_df = spark.read.table("japan_rail.rail_station_changes.stations_list")

    # Pull all unique years from the df
    years_df = stations_list_df.select('year').distinct()

    # Read population by grid parquet - hard code population due to serverless restrictions
    population_file = "/Volumes/japan_rail/rail_station_changes/public_datasets/population-by-grid.parquet"
    population_df = spark.read.parquet(population_file)

    # Create final df for all years
    final_df_all_years = pd.DataFrame()

    # Iterate over evey year and create grid array
    for row in years_df.collect():

        # Pull year from row
        current_year = row['year']

        # Filter population df on year, create lookup dictionary: h3_id -> population
        pop_year_df = population_df.filter(population_df.year == int(current_year)).select("h3_id","population").toPandas()
        pop_lookup = dict(zip(pop_year_df['h3_id'], pop_year_df['population']))

        # Create df with all stations for that year
        stations_list_year = stations_list_df \
            .filter((stations_list_df.year == current_year)) \
            .select("name", "latitude", "longitude")

        # Convert df to pandas for helper functions
        stations_list_year_pd = stations_list_year.toPandas()

        # Create empty grid array for this year
        current_grid_array = []

        # Assign stations to the grid_array objects
        current_grid_array = grid_assign_stations(coastline_h3, stations_list_year_pd, current_grid_array)

        # Iterate over each grid_obj and set population
        for grid_obj in current_grid_array:

            # Pull grid_id
            grid_id = grid_obj.get_id()

            # Grab population for a given grid_id
            pop_value = pop_lookup.get(grid_id, 0)

            # Call set_population method
            grid_obj.set_population(pop_value)

        # Assign grid neighbors
        current_grid_array = assign_grid_centroid_neighbors(stations_list_year_pd, current_grid_array)

        # Create df of grid_array using helper
        grid_array_df = grid_array_create_df(current_grid_array)

        # Add year to df
        grid_array_df['year'] = current_year

        # Append to final df
        final_df_all_years = pd.concat([final_df_all_years, grid_array_df], ignore_index=True)

    # Return final dataframe, check if empty first
    if final_df_all_years.empty:
        return spark.createDataFrame([], schema=SCHEMA)
    else:
        # Convert to spark df
        return spark.createDataFrame(final_df_all_years, schema=SCHEMA)
