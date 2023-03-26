from pyspark.sql import SparkSession


def extract(spark: SparkSession, type: str, source: str):
    """
    Return Extracted Dataframe
    :param spark: SparkSession
    :param type: type of target
    :param source: source path where data is going to extract
    :return: DataFrame
    """

    if type == "JDBC":
        # Read data from mysql database
        output_df = spark.read.format("JDBC").options(url='jdbc:mysql://localhost/world', \
                                                      dbtable=source, driver='com.mysql.cj.jdbc.Driver', user='root',
                                                      password='root').load()
        return output_df

    if type == "CSV":
        # read data from filesystem
        output_df = spark.read.format("CSV").options(header=True, inferSchema=True).load(source)
        return output_df
