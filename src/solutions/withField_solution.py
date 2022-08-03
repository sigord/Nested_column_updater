from typing import Dict
from pyspark.sql.types import StructType
from pyspark.sql.functions import lit, col
from pyspark.sql import Column
from pyspark.sql import DataFrame

def update_df(df: DataFrame, columns_dict: Dict[str, Column]) -> DataFrame:
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
        fiealds_full_list = fiealds.split(".")
        # STATE FOR SIMPLE COLUMN
        if len(fiealds_full_list) == 1:
            updated_df = updated_df.withColumn(fiealds_full_list[0], lit(value))
        # STATE FOR STRUCT COLUMN
        elif len(fiealds_full_list) > 1:
            fiealds_list = fiealds.split('.', 1)
            # STATE FOR NEW STRUCT COLUMN (CREATE NEW STRUCT COLUMN)
            if fiealds_list[0] not in updated_df.columns:
                updated_df = (
                    updated_df
                    .withColumn(fiealds_list[0], lit(None).cast(StructType()))
                )
            # STATE FOR EXISTS STRUCT COLUMN
            # DEEP STRUCTURE CREATING AUTOMATICALLY DUE TO withField METHOD
            updated_df = (
                updated_df
                .withColumn(fiealds_list[0],
                            col(fiealds_list[0]).withField(fiealds_list[1], lit(value)))
            )
    # RETURN UPDATED SPARK DATAFRAME
    return updated_df