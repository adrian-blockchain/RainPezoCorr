from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType


def process_weather_data(weather_data):
    spark = SparkSession.builder.getOrCreate()

    # Créer un DataFrame Spark à partir des données brutes
    weather_df = spark.createDataFrame(weather_data['days'])

    # Sélectionner les colonnes requises
    weather_df = weather_df.select(col('datetime').alias('date'), col('precip'))

    # Convertir la colonne de date en format DateType
    weather_df = weather_df.withColumn('date', col('date').cast(DateType()))


    return weather_df
