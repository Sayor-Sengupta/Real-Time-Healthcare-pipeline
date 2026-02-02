from pyspark.sql.functions import *

# Azure Event Hub Configuration
event_hub_namespace = ""
event_hub_name = ""
event_hub_conn_str = dbutils.secrets.get(
    scope="healthcaremanagementscopevault",
    key="eventhubconnection"
)

kafka_options = {
    "kafka.bootstrap.servers": f"{event_hub_namespace}:9093",
    "subscribe": event_hub_name,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule '
        'required username="$ConnectionString" '
        f'password="{event_hub_conn_str}";'
    ),
    "startingOffsets": "latest",
    "failOnDataLoss": "false"
}

raw_df = (
    spark.readStream
         .format("kafka")
         .options(**kafka_options)
         .load()
)

json_df = raw_df.selectExpr("CAST(value AS STRING) as raw_json")

# ADLS auth
spark.conf.set(
    "fs.azure.account.key.healthcarestorage13.dfs.core.windows.net",
    dbutils.secrets.get(
        scope="healthcaremanagementscopevault",
        key="storageaccesskey"
    )
)

bronze_path = "abfss://bronze@healthcarestorage13.dfs.core.windows.net/patient_flow"
checkpoint_path = "abfss://bronze@healthcarestorage13.dfs.core.windows.net/_checkpoints/patient_flow"

query = (
    json_df.writeStream
           .format("delta")
           .outputMode("append")
           .option("checkpointLocation", checkpoint_path)
           .start(bronze_path)
)
