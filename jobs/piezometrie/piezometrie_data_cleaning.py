from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark.sql.types import DateType


def process_piezometrie_data(piezometrie_data):
    spark = SparkSession.builder.getOrCreate()

    # Créer un DataFrame Spark à partir des données de piézométrie
    piezometrie_df = spark.createDataFrame(piezometrie_data['data'])

    # Sélectionner les colonnes requises
    piezometrie_df = piezometrie_df.select(col('date_mesure').alias('date'), col('niveau_nappe_eau'))

    # Renommer les colonnes pour la jointure
    piezometrie_df = piezometrie_df.withColumnRenamed('date_mesure', 'date')

    # Convertir la colonne de date en format DateType
    piezometrie_df = piezometrie_df.withColumn('date', col('date').cast(DateType()))


    return piezometrie_df
