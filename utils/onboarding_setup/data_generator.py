import dbldatagen as dg
import dbldatagen.distributions as dist
import pyspark.sql.types
from pyspark.sql.functions import *

def iot_data_generator(spark, num_rows):
  factory_ls = ['A06', 'A18', 'A15', 'A04', 'A10', 'A08', 'A14', 'A03', 'A11', 'A20', 'A07', 'A01', 'A13', 'A16']
  model_ls = ['W14-5100-6595', 'W11-5244-7671', 'W20-4800-7050', 'W01-5634-6927', 'W10-5310-8505', 'W15-5076-5887', 'W03-5310-5762', 'W06-5700-5388',
               'W03-5700-6979', 'W07-5634-6309', 'W10-5400-7804', 'W06-5100-4230', 'W18-5310-7592', 'W03-5310-6273', 'W04-5076-7612', 'W20-5700-8667',
                'W14-5400-5028', 'W11-5604-6115', 'W07-5634-6625', 'W04-4800-6607', 'W06-4800-6293', 'W20-5310-6323', 'W13-5604-7619', 'W15-5244-5368',
                 'W11-5100-7011', 'W03-5310-4498', 'W03-5100-5620', 'W08-5634-7828', 'W18-5244-7374', 'W03-5100-5859', 'W01-5700-4913', 
                 'W11-5076-7857', 'W13-5244-8282', 'W20-5634-6384', 'W07-5100-6362', 'W20-5604-8398', 'W04-5310-7592']

  generation_spec = (
      dg.DataGenerator(sparkSession=spark, name='synthetic_data', rows=num_rows, random=True, randomSeed=num_rows)
      .withColumn('device_id', 'bigint', minValue=num_rows*.2, maxValue=num_rows*.8)
      .withColumn('timestamp', 'timestamp', begin="2020-01-01 00:00:00", end="2021-12-31 00:00:00", interval="1 day")
      .withColumn('factory_id', 'string', values=factory_ls)
      .withColumn('option_number', 'bigint', minValue=4800, maxValue=6144)
      .withColumn('model_id', 'string', values=model_ls)
      .withColumn('rotation_speed', 'double', minValue=11.9, maxValue=75.83, step=0.1, distribution=dist.Normal(45, 10))
      .withColumn('pressure', 'double', minValue=323.0, maxValue=433.53, step=0.01, distribution=dist.Gamma(1.0, 2.0))
      .withColumn('temperature', 'double', minValue=181.0, maxValue=225.71, step=0.01, distribution=dist.Normal(200, 20))
      .withColumn('airflow_rate', 'double', minValue=8.0, maxValue=13.3, step=0.1, distribution=dist.Beta(5,1))
      .withColumn('delay', 'double', minValue=1.39, maxValue=8.87, step=0.01)
      .withColumn('density', 'double', minValue=1.05, maxValue=1.33, step=0.01)
  )

  return (
      generation_spec.build()
      .withColumn('device_id', col('device_id').cast('string'))
  )

def defect_data_generator(spark, iot_data):
    defect_df = iot_data.withColumn('defect', when(col('temperature') > 222, 1).when(col('delay') > 8.5, round(rand(1)/1.1))
                              .when(col('density') > 1.28, round(rand(1)/1.7)).when(col('airflow_rate') < 8.2, round(rand(1)/1.5))
                              .when(col('rotation_speed') > 65, round(rand(1)/1.2)).otherwise(0))
    return defect_df.select('defect', 'device_id', 'timestamp')
    
