import shutil
from os import path
import os
import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from os import walk


class Par:

    def __init__(self):
        self.exit = exit

    def process(self, folder):
        print(f"Folder name: {folder}")

        if not os.path.exists(folder):
            return {'statusCode': -1, 'body': json.dumps("File does not exist"), 'headers': {'Content-Type': 'application/json'}}

        sc = SparkContext(appName="CSV2Parquet")
        sqlContext = SQLContext(sc)


        _, _, filenames = next(walk(folder), (None, None, []))
        for file in filenames:
            self.processFile(file, sc, sqlContext)

        sc.stop()
        return {'statusCode': 0,
                'body': json.dumps("Files processed successfully"),
                'headers': {'Content-Type': 'application/json'}}


    def processFile(self, filename, sc, sqlContext):

        print(f"Processing file {filename}")

        schema = StructType([
    		StructField("ForecastSiteCode", IntegerType(), True),
    		StructField("ObservationTime", IntegerType(), True),
    		StructField("ObservationDate", DateType(), True),
    		StructField("WindDirection", IntegerType(), True),
    		StructField("WindSpeed", IntegerType(), True),
    		StructField("WindGust", IntegerType(), True),
    		StructField("Visibility", IntegerType(), True),
    		StructField("ScreenTemperature", FloatType(), True),
    		StructField("Pressure", IntegerType(), True),
    		StructField("SignificantWeatherCode", IntegerType(), True),
    		StructField("SiteName", StringType(), True),
    		StructField("Latitude", DoubleType(), True),
    		StructField("Longitude", DoubleType(), True),
    		StructField("Region", StringType(), True),
    		StructField("Country", StringType(), True)])


        infile = filename
        outdir = "out"


        spark = SparkSession \
            .builder \
            .appName("Protob Conversion to Parquet") \
            .config("header", "true") \
            .getOrCreate()

        # read csv
        df = spark.read.option("header", "true").schema(schema).csv(infile)

        if os.path.exists(outdir):
          shutil.rmtree(outdir)

        df.write.parquet(outdir)

        return {'statusCode': 0,
                'body': json.dumps("File processed successfully"),
                'headers': {'Content-Type': 'application/json'}}

