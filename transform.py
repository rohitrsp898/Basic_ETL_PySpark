from pyspark.sql import DataFrame


def rename_cols(df: DataFrame, mapping_dict: dict) -> DataFrame:
    """
    # Rename all the columns
    :param df: input dataframe
    :param mapping_dict: Key value pair of column names (old:new)
    :return: output Dataframe
    """
    for key in mapping_dict.keys():
        df = df.withColumnRenamed(key, mapping_dict.get(key))

    return df


def specific_cols(df: DataFrame, specific_cols: list):
    """
    Return only required columns
    :param df: input Dataframe
    :param specific_cols:
    :return: Dataframe
    """
    return df.select(specific_cols)


def join_df(left_df: DataFrame, right_df: DataFrame, ON_COLUMNS: list, JOIN_TYPE: str) -> DataFrame:
    """
    Return Join dataframe
    :param left_df: left Dataframe
    :param right_df: right Dataframe
    :param ON_COLUMNS: Join Column
    :param JOIN_TYPE: Join Type
    :return: FInal Dataframe
    """
    output_df = left_df.alias("left_df").join(right_df.alias("right_df"), ON_COLUMNS, JOIN_TYPE)

    return output_df
