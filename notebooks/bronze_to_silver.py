# Databricks notebook source

# COMMAND ----------

# Consulta os dados armazenados na tabela 'dataset_imoveis' da camada bronze
df = spark.sql(
f"""
    select * from bronze.dataset_imoveis
""")

# COMMAND ----------

# Exibe as colunas aninhadas do DataFrame, desestruturando as informações do campo 'anuncio' 
# e de seu subcampo 'endereco' para visualização
df.select('anuncio.*','anuncio.endereco.*').display()

# COMMAND ----------

# Cria um novo DataFrame contendo as informações detalhadas desestruturadas do campo 'anuncio' e 'endereco'
df_detalhados = df.select('anuncio.*','anuncio.endereco.*')

# COMMAND ----------

# Exibe o DataFrame detalhado, removendo as colunas 'endereco' e 'caracteristicas' que não são necessárias
df_detalhados.drop('endereco','caracteristicas').display()

# COMMAND ----------

# Atualiza o DataFrame, removendo as colunas 'endereco' e 'caracteristicas' permanentemente
df_detalhados = df_detalhados.drop('endereco','caracteristicas')

# COMMAND ----------

# Define o esquema (camada Silver) e o nome da tabela que será criada
schema ="silver"
table = "dataset_imoveis"

# Define o caminho de destino no Data Lake onde os dados transformados serão armazenados
path_target = 'abfss://imoveis@datalakepopoff.dfs.core.windows.net/silver/dataset_imoveis'

# COMMAND ----------

# Cria o banco de dados 'silver' caso ele ainda não exista
spark.sql(" CREATE DATABASE IF NOT EXISTS silver")

# Grava o DataFrame transformado na camada Silver, utilizando o formato Delta
df_detalhados.write \
    .format('delta') \
    .mode('overwrite') \  # Sobrescreve os dados existentes na tabela, se houver
    .option('overwriteSchema', 'true') \  # Atualiza o esquema, caso necessário
    .saveAsTable(f'{schema}.{table}')


# COMMAND ----------

df_detalhados.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .save(path_target)
