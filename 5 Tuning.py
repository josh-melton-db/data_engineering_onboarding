# Databricks notebook source
# MAGIC %md
# MAGIC # Performance Tuning on Databricks
# MAGIC </br>
# MAGIC Use a <a href='https://docs.databricks.com/en/clusters/photon.html'>Photon cluster</a> to see the full performance improvements outlined in this notebook
# MAGIC </br>
# MAGIC Use Databricks Runtime 13.3 LTS or higher to take advantage of the latest performance tuning features

# COMMAND ----------

# DBTITLE 1,Install the Library
pip install dbldatagen

# COMMAND ----------

# DBTITLE 1,Run setup
from utils.onboarding_setup import get_config, iot_data_generator, reset_tables
config = get_config(spark)
reset_tables(spark, config, dbutils)

# COMMAND ----------

# DBTITLE 1,Create test ETL
def test_etl(target):
  ''' This function runs a test ETL process against the specified target. It intentionally uses bad practices for demonstration, do not copy!! '''
  iot_data_generator(spark, 1).write.mode('append').saveAsTable(target)
  for i in range(3):
    tuning_test_data = iot_data_generator(spark, 10000).dropDuplicates(['injectionID']).createOrReplaceTempView("new_data")
    spark.sql(f'''
          MERGE INTO {target} t
          USING new_data s
          ON s.injectionID = t.injectionID
          WHEN MATCHED THEN UPDATE SET *
          WHEN NOT MATCHED THEN INSERT *
    ''')
  iot_data_generator(spark, 1000000).write.mode('append').option('maxRecordsPerFile', '10000').saveAsTable(target)

# COMMAND ----------

# DBTITLE 1,Create a test query
def test_query(source_table):
  ''' This function is a nonsensical "no operation" query intended to test performance against a source table '''
  df = spark.read.table(source_table)
  agg_df = df.where('press="temperature > 220" or temperature < 185').groupby('injectionID', 'press').count()
  joined_df = agg_df.join(df, 'injectionID')
  joined_df.write.format("noop").mode("overwrite").save()

# COMMAND ----------

# DBTITLE 1,Time the test ETL
# MAGIC %timeit test_etl(config['bronze_table'])  # Time the etl to the regular table. This may take a few minutes

# COMMAND ----------

# DBTITLE 1,Clear the cache for testing
# MAGIC %sql CLEAR CACHE

# COMMAND ----------

# DBTITLE 1,Time the test query
# MAGIC %timeit test_query(config['bronze_table'])  # Time the the query on the regular table. This may take a couple minutes

# COMMAND ----------

# DBTITLE 1,Create tuned table
# To improve performance, we can make changes to the layout of our data. Let's create a tuned aggregate table
spark.sql(f'''
  CREATE OR REPLACE TABLE {config['tuned_bronze_table']} 
  CLUSTER BY (injectionID, temperature, press) -- By using clustering keys we speed up queries that use those columns
  AS SELECT * FROM {config['bronze_table']};
''')
spark.sql(f''' -- By turning on deletion vectors we speed up merges, updates, deletes, and "needle in the haystack" lookups
  ALTER TABLE {config['tuned_bronze_table']} SET TBLPROPERTIES ('delta.enableDeletionVectors' = true);
''')

# COMMAND ----------

# DBTITLE 1,Time the test ETL
# MAGIC %timeit test_etl(config['tuned_bronze_table'])  # Time the etl to the tuned table. This may take a few minutes

# COMMAND ----------

# DBTITLE 1,Compact small files
spark.sql(f''' -- By running OPTIMIZE we compact small files which speeds up reads
  OPTIMIZE {config['tuned_bronze_table']}; 
''').display()

# COMMAND ----------

# DBTITLE 1,Clear the cache for testing
# MAGIC %sql CLEAR CACHE

# COMMAND ----------

# DBTITLE 1,Time the test query
# MAGIC %timeit test_query(config['tuned_bronze_table']) # Time the querying the tuned table. This may take a minute

# COMMAND ----------

# https://docs.databricks.com/en/delta/tune-file-size.html#what-is-auto-optimize-on-databricks
# https://docs.databricks.com/en/delta/deletion-vectors.html
