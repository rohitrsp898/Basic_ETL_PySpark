from pyspark.sql import DataFrame

# Load the data based on type
'''
:param type: Input Storage type (JDBC|CSV) Based on type data stored in MySQL or FileSystem
:param df: Input Dataframe
:param target: Input target -For filesystem - Location where to store the data
                            -For MySQL - table name
'''

def load(type: str, df: DataFrame, target: str):
    
    # Write data on mysql database with table name
    if type=="JDBC":
        df.write.format("JDBC").mode("overwrite").options(url='jdbc:mysql://localhost/world',\
            dbtable=target,driver='com.mysql.cj.jdbc.Driver',user='root',password='root').save()
        print(f"Data succesfully loaded to MySQL Database !!")
    
    if type=="CSV":
        # Write data on filesystem
        df.write.format("CSV").mode("overwrite").options(header=True).save(target)
        print(f"Data succesfully loaded to filesystem !!")