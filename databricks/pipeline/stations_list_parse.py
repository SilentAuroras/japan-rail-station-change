import pyspark.sql.functions as F
from pyspark import pipelines as dp

# Define pipeline table for stations list all years
@dp.table(
    name="stations_list"
)
def read_stations_parquet():
    """
    Load and parse station list parquet files with year metadata.
    :return: stations_df: dataframe of all stations within a given year (name, latitude, longitude, year)
    """

    # Read in parquet file to dataframe
    df = (
        spark.read
            .format("parquet")
            .load("/Volumes/japan_rail/rail_station_changes/public_datasets/stations-list-*.parquet")
    )

    # Add a column for filepath
    df_with_filename = df.withColumn("source_file", df["_metadata.file_path"])

    # Change filename to year based on format stations-list-YEAR
    df_with_year = df_with_filename.withColumn(
        "year",
        F.regexp_extract(F.col("source_file"), r"stations-list-(\d{4})", 1)
    )

    # Return dataframe
    return df_with_year