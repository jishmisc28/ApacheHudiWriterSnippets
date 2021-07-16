RAF = {
    "kafka_server": "172.XX.X.XXX:9092,172.XX.X.XXX:9092",
    "topic": "raf-events",
    "payloads_list":["Invited","InviteFailed","InviteRejected","ReminderSent","ReferralJoined","ReferralPaymentMade"],
    "primary_key_composed_of":"userid,productid",
    "hive_table":"schema.rafinviteesandnominees",
    "partition_column_name":"part_dt",
    "chkptLocation":"rafpayloadMulti",
    "spark-config" :
        {
            "spark.executor.memory" : "3g",
             "spark.executor.cores" : "3",
             "hive.exec.dynamic.partition" :"true",
             "hive.exec.dynamic.partition.mode" :"nonstrict",
             "hive.exec.max.dynamic.partitions" :"100000",
             "hive.exec.max.dynamic.partitions.pernode" :"100000",
             "hive.mv.files.threads" :"25",
             "hive.blobstore.use.blobstore.as.scratchdir" :"true"
        },
	"spark-config-etl" :
        {
            "spark.driver.memory" : "4g",
            "spark.executor.memory" : "6g",
            "spark.driver.cores" : "2",
            "spark.executor.cores" : "3",
            "spark.dynamicAllocation.initialExecutors": "6",
            "spark.dynamicAllocation.maxExecutors": "12",
            "spark.dynamicAllocation.minExecutors" : "6",
            "spark.executor.memoryOverhead": "3g",
            "spark.driver.memoryOverhead": "3g",
            "spark.yarn.executor.memoryOverhead":"3g",
            "spark.dynamicAllocation.enabled": "true",
            "hive.mv.files.threads" :"25",
            "hive.exec.dynamic.partition" :"true",
            "hive.exec.dynamic.partition.mode" :"nonstrict",
            "hive.exec.max.dynamic.partitions" :"100000",
            "hive.exec.max.dynamic.partitions.pernode" :"100000",
            "hive.blobstore.use.blobstore.as.scratchdir" :"true"
        }

    } 

ReminderSent ={
    "appName": '"ReminderSent"',
    "schema": '''schema=StructType([ \
        StructField("userId", StringType(), True), \
        StructField("productId", StringType(), True), \
        StructField("contactAt", StringType(), True), \
        StructField("type", StringType(), True), \
        StructField("referralsrc", StringType(), True), \
        StructField("referralmedium", StringType(), True), \
        StructField("referralsubmedium", StringType(), True), \
        StructField("date", StringType(), True), \
        StructField("invitecount", StringType(), True)])''',
        "primary_key_composed_of":"userId,productID",
    "parser_fields": '''parsed_df[appName] = string_df[appName].select(col("parsed_value.userId").alias("userid"),\
                            col("parsed_value.productId").alias("productid"),\
                            col("parsed_value.date").alias("date"),\
                            col("parsed_value.contactAt").alias("contactat"),\
                            col("parsed_value.type").alias("type"),\
                            col("parsed_value.referralsrc").alias("referralsrc"),\
                            col("parsed_value.referralmedium").alias("referralmedium"),\
                            col("parsed_value.referralsubmedium").alias("referralsubmedium"),\
                            col("parsed_value.invitecount").alias("invitecount"),\
                            timestamp_format(col("parsed_value.date")).alias("part_dt"))\
                            .select ("userid",
                                "productid",
                                "date",
                                "contactat",
                                "type",
                                "referralsrc",
                                "referralmedium",
                                "referralsubmedium",
                                "invitecount",
                                "part_dt"
                                 )''',
    "partition_column": "part_dt",
    "spark-config" :
        {
            "spark.executor.memory" : "3g",
             "spark.executor.cores" : "3",
             "hive.exec.dynamic.partition" :"true",
             "hive.exec.dynamic.partition.mode" :"nonstrict",
             "hive.exec.max.dynamic.partitions" :"100000",
             "hive.exec.max.dynamic.partitions.pernode" :"100000",
             "hive.mv.files.threads" :"25",
             "hive.blobstore.use.blobstore.as.scratchdir" :"true"
        }
}
InviteFailed ={
    "appName": '"InviteFailed"',
    "schema": '''schema=StructType([ \
        StructField("userId", StringType(), True), \
        StructField("productId", StringType(), True), \
        StructField("contactAt", StringType(), True), \
        StructField("type", StringType(), True), \
        StructField("referralsrc", StringType(), True), \
        StructField("referralmedium", StringType(), True), \
        StructField("referralsubmedium", StringType(), True), \
        StructField("date", StringType(), True), \
        StructField("reason", StringType(), True)])''',
    "parser_fields": '''parsed_df[appName] = string_df[appName].select(col("parsed_value.userId").alias("userid"),\
                            col("parsed_value.productId").alias("productid"),\
                            col("parsed_value.date").alias("date"),\
                            col("parsed_value.contactAt").alias("contactat"),\
                            col("parsed_value.type").alias("type"),\
                            col("parsed_value.referralsrc").alias("referralsrc"),\
                            col("parsed_value.referralmedium").alias("referralmedium"),\
                            col("parsed_value.referralsubmedium").alias("referralsubmedium"),\
                            col("parsed_value.reason").alias("reason"),\
                            timestamp_format(col("parsed_value.date")).alias("part_dt"))\
                            .select ("userid",
                                "productid",
                                "date",
                                "contactat",
                                "type",
                                "referralsrc",
                                "referralmedium",
                                "referralsubmedium",
                                "reason",
                                "part_dt"
                                 )''',
    "partition_column": "part_dt",
    "spark-config" :
        {
            "spark.executor.memory" : "3g",
             "spark.executor.cores" : "3",
             "hive.exec.dynamic.partition" :"true",
             "hive.exec.dynamic.partition.mode" :"nonstrict",
             "hive.exec.max.dynamic.partitions" :"100000",
             "hive.exec.max.dynamic.partitions.pernode" :"100000",
             "hive.mv.files.threads" :"25",
             "hive.blobstore.use.blobstore.as.scratchdir" :"true"
        }
}

InviteRejected ={
    "appName": '"InviteRejected"',
    "schema": '''schema=StructType([ \
        StructField("userId", StringType(), True), \
        StructField("productId", StringType(), True), \
        StructField("contactAt", StringType(), True), \
        StructField("type", StringType(), True), \
        StructField("referralsrc", StringType(), True), \
        StructField("referralmedium", StringType(), True), \
        StructField("referralsubmedium", StringType(), True), \
        StructField("date", StringType(), True), \
        StructField("reason", StringType(), True)])''',
    "parser_fields": '''parsed_df[appName] = string_df[appName].select(col("parsed_value.userId").alias("userid"),\
                            col("parsed_value.productId").alias("productid"),\
                            col("parsed_value.date").alias("date"),\
                            col("parsed_value.contactAt").alias("contactat"),\
                            col("parsed_value.type").alias("type"),\
                            col("parsed_value.referralsrc").alias("referralsrc"),\
                            col("parsed_value.referralmedium").alias("referralmedium"),\
                            col("parsed_value.referralsubmedium").alias("referralsubmedium"),\
                            col("parsed_value.reason").alias("reason"),\
                            timestamp_format(col("parsed_value.date")).alias("part_dt"))\
                            .select ("userid",
                                "productid",
                                "date",
                                "contactat",
                                "type",
                                "referralsrc",
                                "referralmedium",
                                "referralsubmedium",
                                "reason",
                                "part_dt"
                                 )''',
    "partition_column": "part_dt",
    "spark-config" :
        {
            "spark.executor.memory" : "3g",
             "spark.executor.cores" : "3",
             "hive.exec.dynamic.partition" :"true",
             "hive.exec.dynamic.partition.mode" :"nonstrict",
             "hive.exec.max.dynamic.partitions" :"100000",
             "hive.exec.max.dynamic.partitions.pernode" :"100000",
             "hive.mv.files.threads" :"25",
             "hive.blobstore.use.blobstore.as.scratchdir" :"true"
        }
}

Invited ={
    "appName": '"Invited"',
    "schema": '''schema=StructType([ \
        StructField("userId", StringType(), True), \
        StructField("productId", StringType(), True), \
        StructField("contactAt", StringType(), True), \
        StructField("type", StringType(), True), \
        StructField("referralsrc", StringType(), True), \
        StructField("referralmedium", StringType(), True), \
        StructField("referralsubmedium", StringType(), True), \
        StructField("date", StringType(), True)])''',
    "parser_fields": '''parsed_df[appName] = string_df[appName].select(col("parsed_value.userId").alias("userid"),\
                            col("parsed_value.productId").alias("productid"),\
                            col("parsed_value.date").alias("date"),\
                            col("parsed_value.contactAt").alias("contactat"),\
                            col("parsed_value.type").alias("type"),\
                            col("parsed_value.referralsrc").alias("referralsrc"),\
                            col("parsed_value.referralmedium").alias("referralmedium"),\
                            col("parsed_value.referralsubmedium").alias("referralsubmedium"),\
                            timestamp_format(col("parsed_value.date")).alias("part_dt"))\
                            .select ("userid",
                                "productid",
                                "date",
                                "contactat",
                                "type",
                                "referralsrc",
                                "referralmedium",
                                "referralsubmedium",
                                "part_dt"
                                 )''',
    "partition_column": "part_dt",
    "spark-config" :
        {
            "spark.executor.memory" : "3g",
             "spark.executor.cores" : "3",
             "hive.exec.dynamic.partition" :"true",
             "hive.exec.dynamic.partition.mode" :"nonstrict",
             "hive.exec.max.dynamic.partitions" :"100000",
             "hive.exec.max.dynamic.partitions.pernode" :"100000",
             "hive.mv.files.threads" :"25",
             "hive.blobstore.use.blobstore.as.scratchdir" :"true"
        }
}

ReferralJoined ={
    "appName": '"ReferralJoined"',
    "schema": '''schema=StructType([ \
        StructField("userId", StringType(), True), \
        StructField("productId", StringType(), True), \
        StructField("contactAt", StringType(), True), \
        StructField("type", StringType(), True), \
        StructField("referralsrc", StringType(), True), \
        StructField("referralId", StringType(), True), \
        StructField("referralmedium", StringType(), True), \
        StructField("referralsubmedium", StringType(), True), \
        StructField("date", StringType(), True)])''',
    "parser_fields": '''parsed_df[appName] = string_df[appName].select(col("parsed_value.userId").alias("userid"),\
                            col("parsed_value.productId").alias("productid"),\
                            col("parsed_value.date").alias("date"),\
                            col("parsed_value.contactAt").alias("contactat"),\
                            col("parsed_value.type").alias("type"),\
                            col("parsed_value.referralsrc").alias("referralsrc"),\
                            col("parsed_value.referralId").alias("referralid"),\
                            col("parsed_value.referralmedium").alias("referralmedium"),\
                            col("parsed_value.referralsubmedium").alias("referralsubmedium"),\
                            timestamp_format(col("parsed_value.date")).alias("part_dt"))\
                            .select ("userid",
                                "productid",
                                "date",
                                "contactat",
                                "type",
                                "referralsrc",
                                "referralid",
                                "referralmedium",
                                "referralsubmedium",
                                "part_dt"
                                 )''',
    "partition_column": "part_dt",
    "spark-config" :
        {
            "spark.executor.memory" : "3g",
             "spark.executor.cores" : "3",
             "hive.exec.dynamic.partition" :"true",
             "hive.exec.dynamic.partition.mode" :"nonstrict",
             "hive.exec.max.dynamic.partitions" :"100000",
             "hive.exec.max.dynamic.partitions.pernode" :"100000",
             "hive.mv.files.threads" :"25",
             "hive.blobstore.use.blobstore.as.scratchdir" :"true"
        }
}
ReferralPaymentMade ={
    "appName": '"ReferralPaymentMade"',
    "schema": '''schema=StructType([ \
        StructField("userId", StringType(), True), \
        StructField("productId", StringType(), True), \
        StructField("contactAt", StringType(), True), \
        StructField("type", StringType(), True), \
        StructField("referralsrc", StringType(), True), \
        StructField("referralId", StringType(), True), \
        StructField("referralmedium", StringType(), True), \
        StructField("referralsubmedium", StringType(), True), \
        StructField("date", StringType(), True)])''',
    "parser_fields": '''parsed_df[appName] = string_df[appName].select(col("parsed_value.userId").alias("userid"),\
                            col("parsed_value.productId").alias("productid"),\
                            col("parsed_value.date").alias("date"),\
                            col("parsed_value.contactAt").alias("contactat"),\
                            col("parsed_value.type").alias("type"),\
                            col("parsed_value.referralsrc").alias("referralsrc"),\
                            col("parsed_value.referralId").alias("referralid"),\
                            col("parsed_value.referralmedium").alias("referralmedium"),\
                            col("parsed_value.referralsubmedium").alias("referralsubmedium"),\
                            timestamp_format(col("parsed_value.date")).alias("part_dt"))\
                            .select ("userid",
                                "productid",
                                "date",
                                "contactat",
                                "type",
                                "referralsrc",
                                "referralid",
                                "referralmedium",
                                "referralsubmedium",
                                "part_dt"
                                 )''',
    "partition_column": "part_dt",
    "spark-config" :
        {
            "spark.executor.memory" : "3g",
             "spark.executor.cores" : "3",
             "hive.exec.dynamic.partition" :"true",
             "hive.exec.dynamic.partition.mode" :"nonstrict",
             "hive.exec.max.dynamic.partitions" :"100000",
             "hive.exec.max.dynamic.partitions.pernode" :"100000",
             "hive.mv.files.threads" :"25",
             "hive.blobstore.use.blobstore.as.scratchdir" :"true"
        }
}
