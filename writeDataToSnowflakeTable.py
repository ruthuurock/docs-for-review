from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import json
from datetime import datetime
spark = SparkSession.builder.config("spark.jars",
                                    "C:\\Users\\muppa\\Downloads\\snowflake-jdbc-3.13.30.jar,C:\\Users\\muppa\\Downloads\\spark-snowflake_2.12-2.11.3-spark_3.1.jar") \
    .master("local").appName("PySpark_snowflake_test").getOrCreate()
schema = StructType(
    [
        StructField("empid", StringType(), False),
        StructField("empname", StringType(), False),
        StructField("joined_at", TimestampType(), False)
    ]
)
sfparams = {
    "sfURL": "qo15550.west-us-2.azure.snowflakecomputing.com",
    "sfUser": "SURYATEJA19",
    "sfAccount": "qo15550",
    "sfPassword": "Snowflake@123",
    "sfDatabase": "mytestdb",
    "sfSchema": "myschema",
    "sfWarehouse": "COMPUTE_WH",
    "TIMESTAMP_TYPE_MAPPING": "TIMESTAMP_LTZ"
}
#hour, minute,second
#datetime.strptime('2023-06-01 01:32:46.050794', '%Y-%m-%d %H:%M:%S.%f')
data=[["1","vamsikrm",datetime.strptime('2023-06-01 01:32:46.050794', '%Y-%m-%d %H:%M:%S.%f')]]
#data=[["1","vamsikrm","2020-02-01 11:01:19.06"],["2","srinadh","2019-03-01 12:01:19.406"],["3","karhikt","2021-03-01 12:01:19.406"]]

df3=spark.createDataFrame(data,["empid","empname","joined_at"])
#Timestamp String to DateType
df3.withColumn("joined_at",to_timestamp("joined_at")) \
  .show(truncate=False)

df3.show()
df3.printSchema()
empJson = '{  "empid": "100",  "empname": "vamsikrm","joined_at":"2023-06-01 01:32:46.050794"}'
employeesDataFrame = spark.createDataFrame(data=spark.sparkContext.parallelize([json.loads(empJson)]), schema=schema)
employeesDataFrame.show(truncate=False)
df3.write.format("net.snowflake.spark.snowflake").options(**sfparams).option("dbtable",
                                                                                            "mytestdb.myschema.employee").mode(
    "append").save()
