# Databricks notebook source
# MAGIC %sql
# MAGIC select * from bronze.dataset_imoveis

# COMMAND ----------

df = spark.sql(
f"""
    select * from bronze.dataset_imoveis
""")

# COMMAND ----------

df.select('anuncio.*','anuncio.endereco.*').display()

# COMMAND ----------

df_detalhados = df.select('anuncio.*','anuncio.endereco.*')

# COMMAND ----------

df_detalhados.drop('endereco','caracteristicas').display()

# COMMAND ----------

df_detalhados = df_detalhados.drop('endereco','caracteristicas')

# COMMAND ----------

schema ="silver"
table = "dataset_imoveis"

path_target = 'abfss://imoveis@datalakepopoff.dfs.core.windows.net/silver/dataset_imoveis'

# COMMAND ----------

# Gravar os dados no formato Delta
spark.sql(" CREATE DATABASE IF NOT EXISTS silver")

df_detalhados.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .saveAsTable(f'{schema}.{table}')

# COMMAND ----------

df_detalhados.write \
    .format('delta') \
    .mode('overwrite') \
    .option('overwriteSchema', 'true') \
    .save(path_target)
