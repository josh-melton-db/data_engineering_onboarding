import dbldatagen as dg
import dbldatagen.distributions as dist
import pyspark.sql.types
from pyspark.sql.functions import *

def iot_data_generator(spark, num_rows):
  press_ls = ['W06', 'W18', 'W15', 'W04', 'W10', 'W08', 'W14', 'W03', 'W11', 'W20', 'W07', 'W01', 'W13', 'W16']
  campaignId_ls = ['W14-5100-6595', 'W11-5244-7671', 'W20-4800-7050', 'W01-5634-6927', 'W10-5310-8505', 'W15-5076-5887', 'W03-5310-5762', 'W06-5700-5388', 'W03-5700-6979', 'W07-5634-6309', 'W10-5400-7804', 'W06-5100-4230', 'W18-5310-7592', 'W03-5310-6273', 'W04-5076-7612', 'W20-5700-8667', 'W14-5400-5028', 'W11-5604-6115', 'W07-5634-6625', 'W04-4800-6607', 'W06-4800-6293', 'W20-5310-6323', 'W13-5604-7619', 'W15-5244-5368', 'W11-5100-7011', 'W03-5310-4498', 'W03-5100-5620', 'W08-5634-7828', 'W18-5244-7374', 'W03-5100-5859', 'W01-5700-4913', 'W11-5076-7857', 'W13-5244-8282', 'W20-5634-6384', 'W07-5100-6362', 'W20-5604-8398', 'W04-5310-7592', 'W04-5700-4348', 'W16-5400-7093', 'W11-5076-5798', 'W16-6144-7520', 'W07-5244-8674', 'W14-5244-8681', 'W04-5244-6151', 'W16-5400-5416', 'W06-5310-6107', 'W15-5634-4682', 'W18-5700-5190', 'W18-5244-6168', 'W01-5700-6975', 'W10-5100-7823', 'W11-5310-6796', 'W15-4800-5153', 'W14-4800-6053', 'W18-5634-7190']

  generation_spec = (
      dg.DataGenerator(sparkSession=spark, name='synthetic_data', rows=num_rows, random=True, randomSeed=num_rows)
      .withColumn('injectionID', 'bigint', minValue=num_rows*.2, maxValue=num_rows*.8)
      .withColumn('timestamp', 'timestamp', begin="2020-01-01 00:00:00", end="2021-12-31 00:00:00", interval="1 day")
      .withColumn('press', 'string', values=press_ls)
      .withColumn('recipe', 'bigint', minValue=4800, maxValue=6144)
      .withColumn('campaignID', 'string', values=campaignId_ls)
      .withColumn('injectionTime', 'double', minValue=11.9, maxValue=75.83, step=0.1, distribution=dist.Normal(45, 10))
      .withColumn('pressure', 'double', minValue=323.0, maxValue=433.53, step=0.01, distribution=dist.Gamma(1.0, 2.0))
      .withColumn('temperature', 'double', minValue=181.0, maxValue=225.71, step=0.01, distribution=dist.Normal(200, 20))
      .withColumn('waterFlowRate', 'double', minValue=8.0, maxValue=13.3, step=0.1, distribution=dist.Beta(5,1))
      .withColumn('heatingTime', 'double', minValue=1.39, maxValue=8.87, step=0.01)
      .withColumn('density', 'double', minValue=1.05, maxValue=1.33, step=0.01)
  )

  return (
      generation_spec.build()
      .withColumn('injectionID', col('injectionID').cast('string'))
  )

def defect_data_generator(spark, iot_data):
    defect_df = iot_data.withColumn('defect', when(col('temperature') > 222, 1).when(col('heatingTime') > 8.5, round(rand(1)/1.1))
                              .when(col('density') > 1.28, round(rand(1)/1.7)).when(col('waterFlowRate') < 8.2, round(rand(1)/1.5))
                              .when(col('injectionTime') > 65, round(rand(1)/1.2)).otherwise(0))
    return defect_df.select('defect', 'injectionID', 'timestamp')
    
