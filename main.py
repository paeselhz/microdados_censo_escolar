import os
import glob
import json
import gc
import pyarrow as pa
from dask import dataframe as dd

ano_base = 2014

general_dtype_dict = json.load(open("support/files_dtype.json"))

list_of_files=glob.glob("data/raw/microdados_educacao_basica_{ano}/**".format(ano=ano_base),
                        recursive=True)

list_of_csv_files=[filename for filename in list_of_files if filename.endswith((".csv", ".CSV"))]

print("List of csv files to convert to parquet")

for file_to_import in list_of_csv_files:
    print("Converting to parquet")
    print(file_to_import)

    file_filter=file_to_import.split(os.sep)[-1].replace(".csv", "").replace(".CSV", "").split("_")[0]

    dict_for_file_loading=general_dtype_dict. \
        get(str(ano_base)). \
        get(file_filter)

    pyarrow_schema={name: pa.from_numpy_dtype(value) for name, value in dict_for_file_loading.items()}

    tbl=dd.read_csv(
        file_to_import,
        sep="|",
        encoding="latin_1",
        dtype=dict_for_file_loading
    )

    pyarrow_schema_filtered={"index": pa.from_numpy_dtype("Int64")}

    pyarrow_schema_filtered.update({pa_key: pyarrow_schema.get(pa_key) for pa_key in tbl.columns})

    pyarrow_tuples=pyarrow_schema_filtered.items()
    pyarrow_list=list(pyarrow_tuples)

    print("File Read")

    file_name_save=file_to_import.replace('.CSV', '.parquet').replace('.csv', '.parquet')

    tbl.reset_index().to_parquet(file_name_save, compression="snappy", engine="pyarrow", schema=pa.schema(pyarrow_list))

    print("Cleaning mess")

    del tbl
    gc.collect()

    os.remove(file_to_import)

# for year in range(2007, 2011):
#     print(year)
#     download_and_extract_censo_escolar(year)
