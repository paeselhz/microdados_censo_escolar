import gc
import os
import glob
import requests
import zipfile

os.environ["UNRAR_LIB_PATH"] = "/usr/lib/libunrar.so"

from unrar import rarfile

def download_and_extract_censo_escolar(ano_base):

    os.makedirs('data/raw',
                exist_ok = True)

    url_download = "http://download.inep.gov.br/microdados/micro_censo_escolar_{ano}.zip".format(
        ano = ano_base
    )

    url_check = requests.head(url_download)

    if url_check.status_code is not 200:
        print("Changing download url")
        url_download = "http://download.inep.gov.br/microdados/microdados_educacao_basica_{ano}.zip".format(
            ano = ano_base
        )

    print("Starting get request")

    req = requests.get(url_download)

    open("data/raw/microdados_educacao_basica_{ano}.zip".format(ano = ano_base), 'wb').write(req.content)

    print("Data exported to zip file")

    with zipfile.ZipFile("data/raw/microdados_educacao_basica_{ano}.zip".format(ano = ano_base), "r") as zip_file:
        zip_file.extractall("data/raw/microdados_educacao_basica_{ano}/".format(ano = ano_base))

    del req
    gc.collect()

    try:
        os.remove("data/raw/microdados_educacao_basica_{ano}.zip".format(ano = ano_base))
    except:
        print("File {filename} could not be removed".format(
            filename = "microdados_educacao_basica_{ano}.zip".format(
                 ano = ano_base
            )
        ))

    print("Zip file extracted and removed")

    list_of_files=glob.glob("data/raw/microdados_educacao_basica_{ano}/**".format(ano=ano_base),
                            recursive=True)

    print("List of rar files to extract:")

    list_of_rar_files=[filename for filename in list_of_files if filename.endswith('.rar')]

    print(list_of_rar_files)

    for file_to_extract in list_of_rar_files:
        with rarfile.RarFile(file_to_extract) as rar_file:
            rar_file.extractall(os.path.dirname(file_to_extract))
        try:
            os.remove(file_to_extract)
        except:
            print("File {file} could not be removed").format(
                file=file_to_extract.split(os.sep)[-1]
            )

    list_of_zip_files=[filename for filename in list_of_files if filename.endswith('.rar')]

    print(list_of_zip_files)

    for file_to_extract in list_of_zip_files:

        with zipfile.ZipFile(file_to_extract, "r") as zip_file:
            zip_file.extractall(os.path.dirname(file_to_extract))
        try:
            os.remove(file_to_extract)
        except:
            print("File {file} could not be removed").format(
                file=file_to_extract.split(os.sep)[-1]
            )
