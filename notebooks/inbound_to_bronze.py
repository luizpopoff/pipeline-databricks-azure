# Databricks notebook source
# Define o caminho do arquivo de origem no Data Lake, contendo os dados brutos de imóveis no formato JSON
path_source = 'abfss://imoveis@datalakepopoff.dfs.core.windows.net/inbound/dados_brutos_imoveis.json'

# Lê os dados do arquivo JSON utilizando o Spark
# A função read.json é usada para carregar os dados no DataFrame
df = spark.read.json(path_source)

# COMMAND ----------

# Lista os arquivos e diretórios disponíveis no caminho especificado no Data Lake
dbutils.fs.ls(path_source)

# COMMAND ----------

# Exibe os dados carregados no DataFrame para inspeção e validação
display(df)

# COMMAND ----------

# Remove as colunas "imagens" e "usuario" do DataFrame, pois elas não são relevantes para o processamento
df = df.drop("imagens", "usuario")
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

# Adiciona uma nova coluna "id" ao DataFrame, extraindo o campo "id" de dentro do objeto "anuncio"
df_bronze = df.withColumn("id", col("anuncio.id"))
display(df_bronze)

# COMMAND ----------

# Define o esquema (camada Bronze) e o nome da tabela para salvar os dados transformados
schema ="bronze"
table = "dataset_imoveis"

# Define o caminho de destino no Data Lake para armazenar os dados transformados na camada Bronze
path_target = 'abfss://imoveis@datalakepopoff.dfs.core.windows.net/bronze/dataset_imoveis'

# COMMAND ----------

# Lista os arquivos e diretórios no nível principal do Data Lake para verificar a estrutura do ambiente
dbutils.fs.ls("abfss://imoveis@datalakepopoff.dfs.core.windows.net/")

# COMMAND ----------

# Cria o banco de dados 'bronze' no catálogo, se ele ainda não existir
spark.sql(" CREATE DATABASE IF NOT EXISTS bronze")

# Grava os dados transformados no formato Delta, salvando como uma tabela na camada Bronze
df_bronze.write \
    .format('delta') \  # Especifica o formato Delta para armazenamento
    .mode('overwrite') \  # Sobrescreve os dados existentes na tabela, se houver
    .option('overwriteSchema', 'true') \  # Atualiza o esquema, se necessário
    .saveAsTable(f'{schema}.{table}')

# COMMAND ----------

# Grava os dados transformados diretamente no caminho especificado no Data Lake no formato Delta
df_bronze.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .save(path_target)

