from conftest import spark_session
import pytest
import datetime

#Importar las funciones creadas
from functions import leerClima, EliminarNulos, leerDatosAccidents,LeerDatosEnBD
from pyspark.sql.types import (IntegerType,StringType, StructField,StructType,FloatType,DateType)

#Definición de schemas

#Unit tests para 'join_dfs'
#Test 1
def test_read_accidents(spark_session):
    num_rows=665578
    df=leerDatosAccidents()
    assert num_rows==df.count()
def test_deleteNAN_accdents():
    num_rows=299182
    df=leerDatosAccidents()

    accidentsUSA=df.select(df["Severity"],
        df["Start_Time"],df["End_Time"],df["City"],
        df["County"],df["State"],df["Temperature(F)"],
        df["Wind_Chill(F)"],
        df["Humidity(%)"],df["Pressure(in)"],df["Visibility(mi)"],
        df["Zipcode"])
    accidentsUSA=accidentsUSA.withColumnRenamed('Severity', 'Severity_accident')

    df=EliminarNulos(df)
    assert num_rows==df.count()
def test_read_weather(spark_session):
    num_rows=168132
    df=leerClima()
    assert num_rows==df.count()
def test_deleteNAN_weather():
    num_rows=166132
    df=leerClima()
    weatherUSA=df.select(df["Type"],
        df["Severity"],df["StartTime(UTC)"],df["EndTime(UTC)"],
        df["Precipitation(in)"],df["State"],df["County"],
        df["City"],
        df["Zipcode"])
    df=EliminarNulos(weatherUSA)
    assert num_rows==df.count()

