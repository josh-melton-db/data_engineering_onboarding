import random

def get_config(spark):
  num_rows = random.randint(1000, 2000)
  current_user = spark.sql('select current_user()').collect()[0][0].split('@')[0].replace('.', '_')
  schema = f'{current_user}_onboarding'
  return {
      'current_user': current_user,
      'schema' : schema,
      'bronze_table' : f'{schema}.sensor_bronze',
      'defect_table' : f'{schema}.defect_bronze',
      'silver_table' : f'{schema}.sensor_silver',
      'gold_table' : f'{schema}.sensor_gold',
      'tuned_bronze_table' : f'{schema}.sensor_bronze_clustered',
      'csv_staging' : f'/{current_user}_onboarding/csv_staging',
      'checkpoint_location' : f'/{current_user}_onboarding/sensor_checkpoints',
      'rows_per_run' : num_rows,
      'model_name' : 'iot_streaming_model'
  }

def reset_tables(spark, config, dbutils):
  spark.sql(f"drop schema if exists {config['schema']} CASCADE")
  spark.sql(f"create schema {config['schema']}")
  dbutils.fs.rm(config['checkpoint_location'], True)