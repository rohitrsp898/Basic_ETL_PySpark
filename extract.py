
from pyspark.sql import SparkSession


def extract(spark: SparkSession, type: str, source: str):
    # Read data from mysql database 
    if type=="JDBC":
        output_df = spark.read.format("JDBC").options(url='jdbc:mysql://localhost/world',\
            dbtable=source,driver='com.mysql.cj.jdbc.Driver',user='root',password='root').load()
        return output_df
    
    if type=="CSV":
        # read data from filesystem
        output_df = spark.read.format("CSV").options(header=True,inferSchema=True).load(source)
        return output_df





