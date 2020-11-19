# Download microdados Censo Escolar - Inep 

---

A proposta desse projeto é automatizar o processo de download dos microdados
do Censo Escolar da Educação Básica do site do INEP/MEC. Além disso, visando
a otimização do espaço de armazenamento, o script já possui conjuntos de 
dicionários de dados (para fazer a leitura dos dados conforme é esperado) e
exporta os mesmos em `.parquet`, reduzindo em até 1/4 o tamanho final de cada
arquivo.

O processo de download e extração dos arquivos se dá no arquivo `main.py`,
bem como a conversão dos mesmos para `parquet`, entretanto o processo pode
ser demorado, dado que são aproximadamente 200 GB em CSV que precisam ser
lidos e convertidos para parquet (somando os anos de 2007 a 2019), visto isso
também é possível encontrar os arquivos já convertidos em parquet no
Google Cloud Storage, em `gs://microdados-inep/microdados-censo-escolar`.