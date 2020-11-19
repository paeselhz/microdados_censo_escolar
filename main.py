from functions.download_and_extract_censo_escolar import download_and_extract_censo_escolar
from functions.convert_censo_escolar_to_parquet import convert_censo_escolar_to_parquet

for year in range(2007, 2019):
    print(year)
    download_and_extract_censo_escolar(year)
    convert_censo_escolar_to_parquet(year)
