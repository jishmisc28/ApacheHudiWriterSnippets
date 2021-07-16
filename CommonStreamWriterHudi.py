import os
import sys 
import json
import argparse
import datetime
from pyspark.sql.types import *
from pyspark import SparkContext, SparkConf
from pyspark.sql import SQLContext
#sys.path.append("..")

from pyspark.sql.functions import *
from pyspark.sql import functions as F

from uuid import uuid1
import time as t
import shutil
import sys, os
#sys.path.append(os.path.join(os.path.dirname(__file__), '..', 'jobs'))
from shared.get_spark import spark_create


"""
db = '$db$'
s3bucket = '$bucket$'"""

db = 'athena_schema'
s3bucket = 'datalake-dev'
kafka_server = "172.XX.X.XXX:9092,172.XX.X.XXX:9092"

primary_key = "composite_pk"
partition_column = "part_dt"


parser = argparse.ArgumentParser()
parser.add_argument("--streamEntity", help="Stream Entity Name ")
#parser.add_argument("--jobRunDate", help="Job Run Date Will be used to create Partition")

args = parser.parse_args()
streamEntity = args.streamEntity
#job_run_date = args.job_run_date


try:
    if streamEntity == '"RAF"':
        module = __import__('jobs_config_raf')
    
    elif streamEntity == '"COMM"':
        module = __import__('jobs_config_comm')
    
    entity_config = getattr(module, streamEntity.replace('"',''))
    topic = entity_config.get("topic")
    payloads = entity_config.get("payloads_list")
    
    spark_config = entity_config.get('spark-config')

    
    primary_key_composed_of = entity_config.get("primary_key_composed_of")
    cpk01=primary_key_composed_of.split(',')[0]
    cpk02=primary_key_composed_of.split(',')[1]

except AttributeError as e:
    print(e)
    raise e


def getSqlContextInstance(sparkContext):
		if ('sqlContextSingletonInstance' not in globals()):
			globals()['sqlContextSingletonInstance'] = SQLContext(sparkContext)
		return globals()['sqlContextSingletonInstance']


def get_timestamp_format(date_time):
    format ='%Y-%m-%d %I:%M:%S %Z'
    if date_time is None:
       date_time='9999-02-05 11:08:34 UTC'
    datetime_str = datetime.datetime.strptime(date_time, format)
    return datetime_str

		
def json_parser(df,schema):
    
    timestamp_format=udf(get_timestamp_format,DateType())

    string_df = df.select(from_json(col("value").cast("string"), schema).alias("parsed_value"))
    exec(parser_fields)
    
    return parsed_df

if __name__ == "__main__":
    
    masterAppName = streamEntity+'_Streaming_Job'
    spark = spark_create(spark_config,masterAppName)
    spark.sparkContext.setLogLevel("ERROR")

    # kafka readStream
    readStream_df = spark.readStream.format("kafka")\
    .option("kafka.bootstrap.servers",kafka_server)\
    .option("subscribe",topic)\
    .option("startingOffsets", "earliest")\
    .load() \
    .selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    """
    #debug using memory stream
    qname = "strq6"

    readStream_df \
    .coalesce(1)\
    .writeStream \
    .format("memory") \
    .queryName(qname) \
    .outputMode("append") \
    .start()
    
    from time import sleep
    for x in range(15):
        spark.sql("select * from {}".format(qname)).show(3,False)
        sleep(2)
    # initializing dicts for saving different eventTypes data
    """

    df = parsed_df = string_df= {}

    for appName in list(payloads.split(',')):

        # get respective payload configs
        config = getattr(module, appName.replace('"',''))
        appName = config.get("appName")
        parser_fields=config.get("parser_fields")
        schema_config = config.get('schema')
        exec(schema_config)
        appfolder=appName.replace('"','')        
        df[appName] = readStream_df.select(col('value')).where((split(split(col('key'),',').getItem(0),':').getItem(1).alias('domainName')==streamEntity) \
             & (split(split(col('key'),',').getItem(2),':').getItem(1).alias('eventType')==appName))
        
        # parse the data using schema
        #raf_parsed_df = json_parser(df,schema) -> with exec we have to chuck out this function! 

        # parse the data using schema; here we perform some steps for timestamp conversion and exec code snippet
        timestamp_format=udf(get_timestamp_format,DateType())
        #timestamp_format_ref=udf(get_timestamp_format,DateType())
        string_df[appName] = df[appName].select(from_json(col("value").cast("string"), schema).alias("parsed_value"))
        #exec will create parsed_df
        exec(parser_fields)
        print(appName)

        
        # hudi writer for maintaing non duplicated entries in S3
        def get_hudiOptions(table, pk, partition_col):
            tableName = table
            primaryKeyColumn = pk
            partitionColumn = partition_col
            
            tablePath = "s3://{}/{}/RAF_MULTIPLE_OUT_HUDI/{}".format(s3bucket,db,appfolder)

            hudi_options = {
                'hoodie.table.name': tableName,
                'hoodie.datasource.write.operation':'upsert',
                'hoodie.datasource.write.table.type': 'COPY_ON_WRITE',
                'hoodie.datasource.write.recordkey.field': primaryKeyColumn,
                'hoodie.datasource.write.partitionpath.field': partitionColumn,
                'hoodie.datasource.write.precombine.field': partitionColumn,
                
                'hoodie.datasource.hive_sync.enable': 'true',
                'hoodie.datasource.hive_sync.table': tableName,
                'hoodie.datasource.hive_sync.jdbcurl': 'jdbc:hive2://172.31.4.37:10000',
                'hoodie.datasource.hive_sync.database': 'jwr_dev',
                'hoodie.datasource.hive_sync.assume_date_partitioning': 'true',
                'hoodie.datasource.write.hive_style_partitioning': 'true',
                'hoodie.datasource.hive_sync.partition_fields': partitionColumn,
                'hoodie.datasource.hive_sync.partition_extractor_class':'org.apache.hudi.hive.MultiPartKeysValueExtractor',
                
                'hoodie.upsert.shuffle.parallelism': '25',
                'hoodie.bloom.index.update.partition.path': 'true'
            }
            return hudi_options,tablePath 

        parsed_df[appName] = parsed_df[appName].withColumn(primary_key, F.concat(F.col(cpk01),F.lit('_'), F.col(cpk02)))
        hudi_options, tablePath = get_hudiOptions(appfolder, primary_key,partition_column)

        def process_row_hudi(df,epoch_id):
            df.persist()
            
            df.write.format("hudi") \
            .options(**hudi_options) \
            .mode("append") \
            .save(tablePath)
            
            df.unpersist()
            pass

        df_raw_kafka = parsed_df[appName] \
        .coalesce(1)\
        .writeStream.foreachBatch(process_row_hudi)\
        .option("checkpointLocation", "/tmp/chkpt/rafpayloadMultiHudi/{}".format(appfolder))\
        .trigger(processingTime='20 seconds')\
        .start()  
        """

        df_raw_kafka=parsed_df[appName] \
            .coalesce(1)\
            .writeStream.format("parquet")\
            .option("checkpointLocation", "/tmp/chkpt/rafpayloadMulti/{}".format(appfolder))\
            .option("path","s3://{}/{}/RAF_MULTIPLE_OUT/{}".format(s3bucket,db,appfolder))\
            .partitionBy(partition_column)\
            .trigger(processingTime='10 seconds')\
            .start()

        print("******************************************** Writing {} Data as Parquet ****************************************************************".format(appName))
        """ 
        print("******************************************** Writing {} Data as HUDI ****************************************************************".format(appName))
    
