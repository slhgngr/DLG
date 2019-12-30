import unittest
from unittest.mock import patch
import warnings
import inspect
import c2p
import json
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.types import *
import shutil
import os
import datetime
from decimal import Decimal



class TestHandler(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        warnings.simplefilter("ignore", ResourceWarning)
        print("\nSETUP STARTED.")
        cls.textList = \
                        [ \
                        "ForecastSiteCode,ObservationTime,ObservationDate,WindDirection,WindSpeed,WindGust,Visibility,ScreenTemperature,Pressure,SignificantWeatherCode,SiteName,Latitude,Longitude,Region,Country", \
                        "3002,0,2016-02-01T00:00:00,12,8,,30000,2.10,997,8,BALTASOUND (3002),60.7490,-0.8540,Orkney & Shetland,SCOTLAND" \
                        ]
        cls.testCsvFile = "test.csv"
        cls.outdir = "out"
        cls.maxTemp = 15.8
        cls.hottestDate = '2016-03-17'
        cls.hottestRegion = 'Highland & Eilean Siar'
        cls.create_a_csv_file(cls, cls.testCsvFile, cls.textList)

        print("SETUP FINISHED.\n")

    @classmethod
    def tearDownClass(cls):
        print("\nTEAR DOWN STARTED")
        cls.clear_test_objects(cls)
        print("TEAR DOWN FINISHED\n")


    def test00_return_error_when_file_not_exists(self):
        print(f"\n{inspect.currentframe().f_code.co_name}. STARTED")
        foldername = "non_existing_folder"
        p = c2p.Par()
        response = p.process(foldername)
        print(response)
        self.assertEqual(response['statusCode'], -1)
        responseBody = json.loads(response['body'])
        self.assertEqual(responseBody, 'File does not exist')

    def test01_convert_csv_correctly(self):
        print(f"\n{inspect.currentframe().f_code.co_name}. STARTED")

        # convert test csv file to parquetFile
        p = c2p.Par()
        sc = SparkContext(appName="CSV2Parquet")
        sqlContext = SQLContext(sc)
        response = p.processFile(self.testCsvFile, sc, sqlContext)

        # check file creation results
        self.assertEqual(response['statusCode'], 0)
        responseBody = json.loads(response['body'])
        self.assertEqual(responseBody, 'File processed successfully')

        # check file content
        # read parquet file into a DataFrame
        spark = SparkSession \
            .builder \
            .appName("Protob Conversion to Parquet") \
            .config("header", "true") \
            .getOrCreate()

        parquetFile = spark.read.parquet(self.outdir)
        parquetFile.createOrReplaceTempView("parquetFile")
        df = spark.sql("SELECT * FROM parquetFile")
        dflist = df.collect()
        row = dflist[0]

        # put input test values into a list
        csvrow = self.textList[1]
        inputValues = csvrow.split(",")


        # Validate some of the values read from the file
        self.assertEqual(inputValues[10], row[10])
        self.assertEqual(inputValues[13], row[13])
        self.assertEqual(inputValues[14], row[14])
        sc.stop()

    def test02_convert_weather_file(self):
        print(f"\n{inspect.currentframe().f_code.co_name}. STARTED")
        # create test input file
        filename = "w1.csv"

        # convert test csv file to parquetFile
        p = c2p.Par()
        sc = SparkContext(appName="CSV2Parquet")
        sqlContext = SQLContext(sc)
        response = p.processFile(filename, sc, sqlContext)

        # check file creation results
        self.assertEqual(response['statusCode'], 0)
        responseBody = json.loads(response['body'])
        self.assertEqual(responseBody, 'File processed successfully')
        sc.stop()

    def test03_run_sql_for_hottest_day(self):
        print(f"\n{inspect.currentframe().f_code.co_name}. STARTED")
        # convert csv input to parquet
        inputFolder = "csv"
        p = c2p.Par()
        response = p.process(inputFolder)
        self.assertEqual(response['statusCode'], 0)
        responseBody = json.loads(response['body'])
        self.assertEqual(responseBody, 'Files processed successfully')

        # sql = "SELECT max(ScreenTemperature) as temp FROM parquetFile"
        spark = SparkSession \
            .builder \
            .appName("Protob Conversion to Parquet") \
            .config("header", "true") \
            .getOrCreate()

        parquetFile = spark.read.parquet(self.outdir)
        parquetFile.createOrReplaceTempView("parquetFile")
        sql = "SELECT ObservationDate as hday, max(ScreenTemperature) as temp FROM parquetFile GROUP BY hday ORDER BY temp desc LIMIT 1"
        df = spark.sql(sql)
        dflist = df.collect()
        df.show()
        row = dflist[0]
        print(f"Hottest Day : {row[0]} / {datetime.datetime.strptime(self.hottestDate, '%Y-%m-%d').date()}")
        self.assertEqual(datetime.datetime.strptime(self.hottestDate, '%Y-%m-%d').date(), row[0])

    def test04_run_sql_for_temperature_on_hottest_day(self):
        print(f"\n{inspect.currentframe().f_code.co_name}. STARTED")
        # convert csv input to parquet
        inputFolder = "csv"
        p = c2p.Par()
        response = p.process(inputFolder)
        self.assertEqual(response['statusCode'], 0)
        responseBody = json.loads(response['body'])
        self.assertEqual(responseBody, 'Files processed successfully')

        spark = SparkSession \
            .builder \
            .appName("Protob Conversion to Parquet") \
            .config("header", "true") \
            .getOrCreate()

        parquetFile = spark.read.parquet(self.outdir)
        parquetFile.createOrReplaceTempView("parquetFile")
        sql = "SELECT ObservationDate as hday, max(ScreenTemperature) as temp FROM parquetFile GROUP BY hday ORDER BY temp desc LIMIT 1"
        df = spark.sql(sql)
        dflist = df.collect()
        df.show()
        row = dflist[0]
        print(f"Temperature on Hottest Day : {round(Decimal(row[1]), 2)}")
        self.assertEqual(round(Decimal(self.maxTemp),2), round(Decimal(row[1]), 2))

    def test05_run_sql_for_region_of_hottest_day(self):
        print(f"\n{inspect.currentframe().f_code.co_name}. STARTED")
        # convert csv input to parquet
        inputFolder = "csv"
        p = c2p.Par()
        response = p.process(inputFolder)
        self.assertEqual(response['statusCode'], 0)
        responseBody = json.loads(response['body'])
        self.assertEqual(responseBody, 'Files processed successfully')

        spark = SparkSession \
            .builder \
            .appName("Protob Conversion to Parquet") \
            .config("header", "true") \
            .getOrCreate()

        parquetFile = spark.read.parquet(self.outdir)
        parquetFile.createOrReplaceTempView("parquetFile")
        sql = "SELECT ObservationDate as hday, Region as region, max(ScreenTemperature) as temp FROM parquetFile GROUP BY hday, region ORDER BY temp desc LIMIT 1"
        df = spark.sql(sql)
        dflist = df.collect()
        df.show()
        row = dflist[0]
        print(f"Region of Hottest Day : {row[1]}")
        self.assertEqual(self.hottestRegion, row[1])

    def create_a_csv_file(self, testCsvFile, textList):
        try:
            # open a (new) file to write
            outF = open(testCsvFile, "w")
            # textList = ["name,age,city", "salih,35,London"]
            for line in textList:
                # write line to output file
                outF.write(line)
                outF.write("\n")
            outF.close()
        except Exception as e:
            print(f"EXCEPTION: {e}")
            raise
        return

    def clear_test_objects(self):
        if os.path.exists(self.outdir):
            shutil.rmtree(self.outdir)
        if os.path.exists(self.testCsvFile):
            os.remove(self.testCsvFile)
