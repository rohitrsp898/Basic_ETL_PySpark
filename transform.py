from pyspark.sql import DataFrame

# Rename all the columns
'''
:param df: input dataframe
:param mapping_dict: dict of columns names
:return: ouput dataframe
'''
def rename_cols(df: DataFrame, mapping_dict: dict) ->DataFrame:
    
    for key in mapping_dict.keys():
        df=df.withColumnRenamed(key,mapping_dict.get(key))
        
    return df


# get specific cols df
'''
:param df: input dataframe
:param specific_cols: list of columns names
:return: ouput dataframe
'''
def specific_cols(df: DataFrame, specific_cols: list):
    return df.select(specific_cols)


# Join two dataframes
'''
:param left_df: input dataframe
:param right_df: input dataframe
:param ON_COLUMNS: list of columns to perform join
:param JOIN_TYPE: Join type 
:return: ouput dataframe
'''
def join_df(left_df: DataFrame, right_df: DataFrame, ON_COLUMNS:list, JOIN_TYPE: str)->DataFrame:
    
    output_df=left_df.alias("left_df").join(right_df.alias("right_df"), ON_COLUMNS, JOIN_TYPE)
    
    return output_df

