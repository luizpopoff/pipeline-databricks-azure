# Databricks notebook source
path_source = 'abfss://imoveis@datalakepopoff.dfs.core.windows.net/inbound/dados_brutos_imoveis.json'

#df = spark.read.format("json").option("header", True).load(path_source)
df = spark.read.json(path_source)

# COMMAND ----------

dbutils.fs.ls(path_source)

# COMMAND ----------

display(df)

# COMMAND ----------

df = df.drop("imagens", "usuario")
display(df)

# COMMAND ----------

from pyspark.sql.functions import col

# COMMAND ----------

df_bronze = df.withColumn("id", col("anuncio.id"))
display(df_bronze)

# COMMAND ----------

schema ="bronze"
table = "dataset_imoveis"

path_target = 'abfss://imoveis@datalakepopoff.dfs.core.windows.net/bronze/dataset_imoveis'



# COMMAND ----------

dbutils.fs.ls("abfss://imoveis@datalakepopoff.dfs.core.windows.net/")

# COMMAND ----------

# Gravar os dados no formato Delta
spark.sql(" CREATE DATABASE IF NOT EXISTS bronze")

df_bronze.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f'{schema}.{table}')

# COMMAND ----------

df_bronze.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .save(path_target)
