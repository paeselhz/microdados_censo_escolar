import os
import glob
import requests
import zipfile
import pandas as pd
import json

os.environ["UNRAR_LIB_PATH"] = "/usr/lib/libunrar.so"

from unrar import rarfile

ano_base = 2007

with open("support/files_dtype.json") as json_data:
    general_dtype_dict = json.load(json_data)

os.makedirs('data/raw',
            exist_ok = True)

url_download = "http://download.inep.gov.br/microdados/micro_censo_escolar_{ano}.zip".format(
    ano = ano_base
)

url_check = requests.head(url_download)

if url_check.status_code is not 200:
    url_download = "http://download.inep.gov.br/microdados/microdados_educacao_basica_{ano}.zip".format(
        ano = ano_base
    )

req = requests.get(url_download)

open("data/raw/microdados_educacao_basica_{ano}.zip".format(ano = ano_base), 'wb').write(req.content)

with zipfile.ZipFile("data/raw/microdados_educacao_basica_{ano}.zip".format(ano = ano_base), "r") as zip_file:
    zip_file.extractall("data/raw/microdados_educacao_basica_{ano}/".format(ano = ano_base))

try:
    os.remove("data/raw/microdados_educacao_basica_{ano}.zip".format(ano = ano_base))
except:
    print("File {filename} could not be removed".format(
        filename = "microdados_educacao_basica_{ano}.zip".format(
             ano = ano_base
        )
    ))

list_of_files = glob.glob("data/raw/microdados_educacao_basica_{ano}/**".format(ano = ano_base),
                          recursive = True)

list_of_rar_files = [filename for filename in list_of_files if filename.endswith('.rar')]

for file_to_extract in list_of_rar_files:
    with rarfile.RarFile(file_to_extract) as rar_file:
        rar_file.extractall(os.path.dirname(file_to_extract))
    try:
        os.remove(file_to_extract)
    except:
        print("File {file} could not be removed").format(
            file = file_to_extract.split(os.sep)[-1]
        )

list_of_files = glob.glob("data/raw/microdados_educacao_basica_{ano}/**".format(ano = ano_base),
                          recursive = True)

list_of_csv_files = [filename for filename in list_of_files if filename.endswith((".csv", ".CSV"))]

for file_to_import in list_of_csv_files:
    dict_for_file_loading = general_dtype_dict.\
        get(str(ano_base)).\
        get(file_to_import.split(os.sep)[-1].replace(".csv", "").replace(".CSV", ""))

    tbl = pd.read_csv(
        file_to_import,
        sep = "|",
        encoding = "latin_1",
        dtype = dict_for_file_loading
    )

    file_name_save = file_to_import.replace('.CSV', '.parquet').replace('.csv', '.parquet')

    tbl.reset_index().to_parquet(file_name_save, compression = "snappy")

    os.remove(file_to_import)
