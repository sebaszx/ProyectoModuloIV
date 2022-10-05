from pyspark.sql import SparkSession
from pyspark.sql.types import StructField,StringType,IntegerType,StructType,DateType,FloatType
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from pyspark.ml.stat import Correlation
import seaborn as sns
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd
from pyspark.ml.feature import StringIndexer,VectorAssembler
from pyspark.ml.feature import StandardScaler
from pyspark.ml.classification import RandomForestClassifier,DecisionTreeClassifier
from pyspark.ml.evaluation import MulticlassClassificationEvaluator
from pyspark.ml.tuning import ParamGridBuilder, CrossValidator
from pyspark.sql.functions import col,isnan, when, count
spark = SparkSession \
    .builder \
    .appName("Proyecto") \
    .config("spark.driver.extraClassPath", "postgresql-42.2.14.jar") \
    .config("spark.executor.extraClassPath", "postgresql-42.2.14.jar") \
    .getOrCreate()
def ContarNulos(dataframe):
    dataframe.select([count(when(isnan(c) | col(c).isNull(), c)).alias(c) for c in dataframe.columns]
    ).show()
"""Se limpia el dataset"""
def EliminarNulos(DataFrame):
    DataFrame=DataFrame.na.drop()
    return DataFrame

def leerClima():
    return spark \
        .read \
        .format("csv") \
        .option("path", "Datos/WeatherEvents_Jan2016-Dec2021_solo_2021.csv") \
        .option("header", True) \
        .schema(StructType([
                    StructField("EventId", StringType()),
                    StructField("Type", StringType()),
                    StructField("Severity", StringType()),
                    StructField("StartTime(UTC)", StringType()),
                    StructField("EndTime(UTC)", StringType()),
                    StructField("Precipitation(in)", FloatType()),
                    StructField("TimeZone", StringType()),
                    StructField("AirportCode", StringType()),
                    StructField("LocationLat", FloatType()),
                    StructField("LocationLng", FloatType()),
                    StructField("City", StringType()),
                    StructField("County", StringType()),
                    StructField("State", StringType()),
                    StructField("ZipCode", StringType()),
                
                    ])) \
        .load()

def leerDatosAccidents():
    return spark \
        .read \
        .format("csv") \
        .option("path", "Datos/US_Accidents_Dec21_updated_solo_2021.csv") \
        .option("header", True) \
        .schema(StructType([
                    StructField("ID", StringType()),
                    StructField("Severity", IntegerType()),
                    StructField("Start_Time", StringType()),
                    StructField("End_Time", StringType()),
                    StructField("Start_Lat", FloatType()),
                    StructField("Start_Lng", FloatType()),
                    StructField("End_Lat", FloatType()),
                    StructField("End_Lng", FloatType()),
                    StructField("Distance(mi)", FloatType()),
                    StructField("Description", StringType()),
                    StructField("Number", IntegerType()),
                    StructField("Street", StringType()),
                    StructField("Side", StringType()),
                    StructField("City", StringType()),
                    StructField("County", StringType()),
                    StructField("State", StringType()),
                    StructField("Zipcode", StringType()),
                    StructField("Country", StringType()),
                    StructField("Timezone", StringType()),
                    StructField("Airport_Code", StringType()),
                    StructField("Weather_Timestamp", StringType()),
                    StructField("Temperature(F)", FloatType()),
                    StructField("Wind_Chill(F)", FloatType()),
                    StructField("Humidity(%)", FloatType()),
                    StructField("Pressure(in)", FloatType()),
                    StructField("Visibility(mi)", FloatType()),
                    StructField("Wind_Direction", StringType()),
                    StructField("Wind_Speed(mph)", FloatType()),
                    StructField("Precipitation(in)", FloatType()),
                    StructField("Weather_Condition", StringType())
                    ])) \
        .load()
def EscribirDatosEnTabla(dataframe,NombreTabla):
    dataframe \
    .write \
    .format("jdbc") \
    .mode('overwrite') \
    .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
    .option("user", "postgres") \
    .option("password", "testPassword") \
    .option("dbtable", NombreTabla) \
    .save()

def LeerDatosEnBD(NombreTabla):
    return spark \
    .read \
    .format("jdbc") \
    .option("url", "jdbc:postgresql://host.docker.internal:5433/postgres") \
    .option("user", "postgres") \
    .option("password", "testPassword") \
    .option("dbtable", NombreTabla) \
    .load()