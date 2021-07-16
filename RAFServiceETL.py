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

from shared.get_spark import spark_create

parser = argparse.ArgumentParser()
#parser.add_argument("--hivetable", help="Hive Table Name")
parser.add_argument("--partDt", help="Partition Date")

args = parser.parse_args()
partDt = args.partDt[:10]
#hive_table = args.hivetable

db = 'athena_schema'
s3bucket = 'datalake-dev'
entity='"RAF"'
output_folder = 'RAF_SERVICES_OUTPUT'

try:
    module = __import__('jobs_config_raf')
    entity_config = getattr(module, entity.replace('"',''))
    payloads = entity_config.get("payloads_list")

except AttributeError as e:
    print(e)
    raise e

masterAppName = "RAF_ETL_Job"
hive_table = entity_config.get("hive_table")
partition_column_name = entity_config.get("partition_column_name")

# below configs would be required for Hudi only 
"""primary_key = config.get("primary_key_name")
primary_key_composed_of = config.get("primary_key_composed_of")
cpk01=primary_key_composed_of.split(',')[0]
cpk02=primary_key_composed_of.split(',')[1]
"""

spark_config = entity_config.get('spark-config-etl')

if __name__ == "__main__":

        spark = spark_create(spark_config,masterAppName)
        spark.sparkContext.setLogLevel("ERROR")
        
        df = {}

        for appName in payloads:

            config = getattr(module, appName.replace('"',''))
            schema_config = config.get('schema')
            exec(schema_config)

            #appName = appName.replace('"','')    
            print ("appName is {}".format(appName))

            try:
                df[appName] = spark \
                .read \
                .parquet('s3://{}/{}/{}/{}/part_dt={}/'.format(s3bucket,db,output_folder,appName,partDt))
            
            except Exception as e:
                df[appName] = spark.createDataFrame(spark.sparkContext.emptyRDD(),schema)
                print("reading empty {} dataframe".format(appName))

        
        print("df dictionary contents {}".format(df))

        payload_join_df=df['Invited'].alias('parent').join(df['ReferralJoined'].alias('child'),[df['Invited']['userid'] == df['ReferralJoined']['referralid']],"left")\
        .join(df['ReminderSent'].alias('b'),["userid"],"left")\
        .join(df['InviteFailed'].alias('c'),["userid"],"left")\
        .join(df['InviteRejected'].alias('d'),["userid"],"left")\
        .join(df['ReferralPaymentMade'].alias('e'),[df['Invited']['userid'] == df['ReferralPaymentMade']['referralid']],"left")\
        .select('parent.*','child.*',col('e.date'),col('b.invitecount').alias('invitecount'),col('c.reason').alias('FailedReason'),col('d.reason').alias('RejectedReason'))\
        .select(col('parent.userid').alias('parent_userid'),\
                 col('child.date').alias('register_timestamp'),\
                 col('invitecount').alias('invitecount'),\
                 col("parent.type").alias("invite_type"),\
                 col('child.userid').alias('userid'),\
                 col('child.referralid').alias('child_friendreferral'),\
                 col('parent.date').alias('referreddate'),\
                 col('parent.referralsrc').alias('parent_l1_source'),\
                 col('parent.referralmedium').alias('parent_l2_medium'),
                 col('e.date').alias('first_deposit_timestamp'))

        #payload_join_df = payload_join_df.withColumn('parent_emailaddress',F.lit('parent_email_tobe_pulled')).withColumn('child_emailaddress',F.lit('child_email_tobe_pulled'))

        payload_join_df.coalesce(1).createOrReplaceTempView('raf_payload_merge_temp')
        
        parentusersql = """select raf.*, userdevicetype,registerdate,firstdepositdate,friendreferral,email as parent_emailaddress, adkey, st.name as statename from raf_payload_merge_temp raf left join jwr.users usr on raf.parent_userid = usr.userId left join jwr.states st on usr.stateid = st.stateid"""
        parent_users_df=spark.sql(parentusersql)
        parent_users_df.coalesce(1).createOrReplaceTempView('raf_parent_users_temp')

        childusersql = """select raf.*,email as child_emailaddress from raf_parent_users_temp raf left join jwr.users usr on raf.userid = usr.userId """
        users_df=spark.sql(childusersql)
        users_df.coalesce(1).createOrReplaceTempView('raf_users_temp')

        user_mkt_vals_sql = """select rut.*, channel,offer,campaignname,adgroupname,landingpage from raf_users_temp rut left join jwr.adkeysourcecsvmapping ad on rut.adkey = ad.adkey"""
        user_mkt_vals_df = spark.sql(user_mkt_vals_sql)
         
         
        raf_dataset = user_mkt_vals_df.select(col('parent_userid').alias('parent_userid'),\
              col('parent_emailaddress').alias('parent_emailaddress'),\
              col('referreddate').alias('referreddate'),\
              col('invitecount').alias('invitecount'),\
              col('invite_type').alias('invite_type'),\
              col('userdevicetype').alias('invitedevice'),\
              col('userid').alias('userid'),\
              col('child_emailaddress').alias('child_emailaddress'),\
              col('child_friendreferral').alias('child_friendreferral'),\
              col('register_timestamp').alias('register_timestamp'),\
              col('first_deposit_timestamp').alias('first_deposit_timestamp'),\
              col('registerdate').alias('parent_register_timestamp'),\
              coalesce(col('firstdepositdate'),lit('9999-12-31 12:00:00.100')).alias('parent_first_deposit_timestamp'),\
              col('friendreferral').alias('parent_friend_referral_id'),\
              col('parent_l1_source').alias('parent_l1_source'),\
              col('parent_l2_medium').alias('parent_l2_medium'),\
              col('offer').alias('parent_l7_offer'),\
              col('adkey').alias('parent_l8_adkey'),\
              col('statename').alias('parent_state'),\
              col('channel').alias('parent_l3_channel'),\
              col('campaignname').alias('parent_l4_campaign'),\
              col('adgroupname').alias('parent_l5_adgroup'),\
              col('landingpage').alias('parent_l6_landingpage'),\
          )

          
        raf_dataset = raf_dataset.withColumn(partition_column_name,to_date(col("referreddate")))

        #raf_dataset.show(5)

        #hudi_options, tablePath = get_hudiOptions(hive_table, primary_key,partition_column_name)

        print("********************* !!!! EVERYTHING DONE, JUST WAITING FOR INSERT INTO HIVE TABLE HERE!!!! ******************************")

        """ raf_dataset.coalesce(1).write.format("org.apache.hudi") \
                .option('hoodie.datasource.write.operation','insert',) \
                .options(**hudi_options) \
                .mode("append") \
                .save(tablePath)"""

        raf_dataset.repartition(1).createOrReplaceTempView('tab')

        sqlq="""INSERT INTO TABLE {} PARTITION ({}) 
                    select * 
                    FROM tab
                """.format (hive_table,partition_column_name)
        spark.sql(sqlq)

        #raf_dataset.coalesce(1).write.partitionBy(partition_column_name).mode("append").parquet("s3://{}/{}/rafinviteesandnominees_test".format(s3bucket,db))
        #raf_dataset.repartition(partition_column_name).write.partitionBy(partition_column_name).mode("append").parquet("s3://{}/{}/rafinviteesandnominees_test".format(s3bucket,db))

        print("********************* WRITE Into HIVE TABLE SUCCESSFUL ******************************")

