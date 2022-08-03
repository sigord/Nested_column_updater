from typing import Dict, Tuple
from pyspark.sql.types import StructType
from pyspark.sql.functions import lit, col, struct, row_number
from pyspark.sql import Column
from pyspark.sql import DataFrame
from pyspark.sql import Window


def check_if_path_exists(input_df: DataFrame, struct_path: list) -> Tuple[bool, list, list]:
    """
    Check if provided path exists
    Args:
        input_df (DataFrame): Input dataframe
        path (str): Path to check
    Returns:
        bool (bool): Exists path or not
        exists_path (list): Exists part of path
        new_path (list): New part of path
    """
    exists_path = []
    new_path = struct_path.copy()
    temp_df = input_df.select('*')
    for i in struct_path:
        if i in temp_df.columns:
            exists_path.append(i)
            new_path.remove(i)
            temp_df = temp_df.select(col(i + '.*'))
    return (len(new_path) == 0), exists_path, new_path


def connect_dfs(left_df: DataFrame, right_df: DataFrame) -> DataFrame:
    """
    Connect two dataframes with new column and then remove it.
    Args:
        left_df (DataFrame):
        right_df (DataFrame):
    Returns:
        DataFrame:
    """
    w=Window.orderBy(lit(1))
    rdf=right_df.withColumn("rn",row_number().over(w)-1)
    ldf=left_df.withColumn("rn",row_number().over(w)-1)
    ldf = ldf.join(rdf,["rn"]).drop("rn")
    return ldf


def update_df_column_old(input_df: DataFrame, path: str, value: Column) -> DataFrame:
    """
    Update column with any lvl of complexity by provided Column object
    Args:
        input_df (DataFrame): _description_
        path (str): _description_
        value (Column): _description_
    Returns:
        DataFrame: _description_
    """
    pl = path.split('.')
    struct_path_str = '.'.join(pl[:-1])
    struct_path = pl[:-1]
    column_name = pl[-1]
    # STATE FOR SIMPLE COLUMN
    if len(struct_path) == 0:
        updated_df = input_df.select('*').withColumn(column_name, value)
        return updated_df
    # STATE FOR STRUCT COLUMN
    # CHECK IF STRUCT PATH EXISTS
    path_exists_bool, exists_path, new_path = check_if_path_exists(input_df, struct_path)
    r_exists_path = list(reversed(exists_path))
    # STATE FOR EXISTS STRUCT PATH
    if path_exists_bool:
        updated_df = input_df.select(col(struct_path_str +'.*')).withColumn(column_name, value)
        updated_df = updated_df.select(struct('*').alias(r_exists_path.pop(0)))
    # STATE FOR NEW STRUCT PATH
    else:
        for num, not_exists_fieald in enumerate(reversed(new_path)):
            # CREATE STRUCT WITH NEW COLUMN
            if num == 0:
                updated_df = input_df.select(
                    struct(value.alias(column_name)).alias(not_exists_fieald)
                )
            # RECOVER NOT EXISTS STRUCT
            else:
                updated_df = input_df.select(struct('*').alias(not_exists_fieald))
    # RECOVER STRUCTURE UP TO LVL 1
    for columnn in r_exists_path:
        # WE NEED FIND ALL OTHER COLUMNS AT THIS LVL AND RECOVER THE STRUCTURE
        join_temp_df = input_df.select(col('.'.join(pl[:pl.index(columnn)+1])+'.*'))
        # GET ALL COLUMNS WITHOUT THE ONE WE ARE UPDATING
        other_columns = [i for i in join_temp_df.columns if i != updated_df.columns[0]]
        join_temp_df = join_temp_df.select(*other_columns)
        # CONNECT UPDATED COLUMN WITH OTHERS
        updated_df = connect_dfs(join_temp_df, updated_df)
        # RECOVER STRUCTURE
        updated_df = updated_df.select(struct('*').alias(columnn))
    # RECOVER LVL 1 STRUCTURE
    join_temp_df = input_df.select('*')
    other_columns = [i for i in join_temp_df.columns if i != updated_df.columns[0]]
    join_temp_df = join_temp_df.select(*other_columns)
    updated_df = connect_dfs(join_temp_df, updated_df)
    return updated_df


def update_df_old(df: DataFrame, columns_dict: Dict[str, Column]) -> DataFrame:
    """
    Updates existing columns or creates new in dataframe df using
    columns from columns_dict.
    :param df: input dataframe
    :type df: pyspark.sql.Dataframe
    :param columns_dict: Key-value dictionary of columns which need to
    be updated. Key is a column name in
    the format of path.to.col
    :type param: Dict[str, pyspark.sql.Column]
    :return: dataframe with updated columns
    :rtype pyspark.sql.DataFrame
    """
    updated_df = df
    for fiealds, value in columns_dict.items():
        updated_df = update_df_column_old(updated_df, fiealds, value)
    return updated_df