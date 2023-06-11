from pyspark.sql import SparkSession


def join_data(weather_df, piezometrie_df):
    """
    Combine les DataFrames weather_df et piezometrie_df en utilisant Spark.

    Args:
        weather_df (pyspark.sql.DataFrame): DataFrame contenant les données météorologiques.
        piezometrie_df (pyspark.sql.DataFrame): DataFrame contenant les données de piézométrie.

    Returns:
        pyspark.sql.DataFrame: DataFrame contenant les données combinées.
    """
    spark = SparkSession.builder.getOrCreate()

    # Convertir les DataFrames pandas en DataFrames Spark
    weather_spark_df = spark.createDataFrame(weather_df)
    piezometrie_spark_df = spark.createDataFrame(piezometrie_df)

    # Effectuer la jointure en utilisant la colonne 'date'
    combined_spark_df = weather_spark_df.join(piezometrie_spark_df, on='date', how='inner')

    return combined_spark_df
