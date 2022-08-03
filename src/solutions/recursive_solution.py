from pyspark.sql.functions import col, struct


def get_heirs(nestings, parent, level):
    heirs = set()
    for nesting in nestings.keys():
        # если начинается с названия и это структурированная колонка то добавлем все колонки нижнего уровня
        if nesting.startswith(parent) and len(nesting.split(".")) >= level:
            heir = ".".join([val for val in nesting.split(".")[:level]])
            heirs.add(heir)
    return heirs


def get_available_cols(df, name):
    fields = [field for field in df.schema.jsonValue()['fields'] if field['name'] == name]
    if fields and isinstance(fields[0]['type'], dict):
        fields = [field['name'] for field in fields[0]['type']['fields']]
    else:
        fields = []
    return fields


def recursive_update(df, cols, mapper, level=1):
    structures = []
    for col_name in cols:
        #получаем все колонки на уровень ниже
        heirs = get_heirs(mapper, col_name, level + 1)
        next_level = set([col_name + "." + field
                          for field in get_available_cols(df, col_name.split(".")[-1])])
        short_name = col_name.split(".")[-1]
        if heirs:
            all_next_cols = next_level.union(heirs)
            query = df.select(f"{short_name}.*") if short_name in df.columns and next_level else df
            new_struct = struct(
                *recursive_update(
                    query,
                    all_next_cols,
                    mapper,
                    level + 1
                )
            ).alias(short_name)
            structures.append(new_struct)
        else:
            if col_name in mapper.keys():
                new_col = mapper[col_name].alias(short_name)
            else:
                new_col = col(col_name).alias(short_name)
            structures.append(new_col)
    return structures


def update_df(df, columns_dict):
    new_one_level = set([val.split(".")[0] for val in columns_dict.keys()])
    old_one_level = set(df.columns)
    one_level = new_one_level.union(old_one_level)
    updated_df = df.select(*recursive_update(df, one_level, columns_dict))
    return updated_df