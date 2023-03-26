# Import required libraries and modules
from pyspark.sql import SparkSession
from metadata.constant import CITY_COL_DICT, COUNTRY_LANGUAGE_COL_DICT, COUNTRY_COL_DICT, \
    JOIN_TYPE, JOIN_ON_COLUMNS, SPEC_COLS, spark_inst

from extract import extract
from transform import rename_cols, join_df, specific_cols
from load import load

# Initiating and Calling SparkSession
SPARK = spark_inst()

#### Extract ####

# Extracting CITY and COUNTRY data from MYSQL
city_df = extract(SPARK, "JDBC", "city")
country_df = extract(SPARK, "JDBC", "country")

# Extracting COUNTRYLANGUAGE data from FileSystem
country_language_df = extract(SPARK, "CSV", "filesystem/countrylanguage.csv")

#### Transformation ####

# 1. Rename Columns
city_df = rename_cols(city_df, CITY_COL_DICT)
country_df = rename_cols(country_df, COUNTRY_COL_DICT)
country_language_df = rename_cols(country_language_df, COUNTRY_LANGUAGE_COL_DICT)

# 2. Join DF with common column "country_code"
country_city_df = join_df(country_df, city_df, JOIN_ON_COLUMNS, JOIN_TYPE)
country_city_language_df = join_df(country_city_df, country_language_df, JOIN_ON_COLUMNS, JOIN_TYPE)

# 3. Get specific cols
country_city_language_df = specific_cols(country_city_language_df, SPEC_COLS)

#### Load Data ####

# MySQL 
load("JDBC", country_city_language_df, "CountryCityLanguage")

# FileSystem
load("CSV", country_city_language_df, "output/countrycitylanguage.csv")
