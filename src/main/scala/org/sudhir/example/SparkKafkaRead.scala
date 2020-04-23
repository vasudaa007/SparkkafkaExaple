package org.sudhir.example
import org.apache.spark.sql.avro._
import org.apache.spark.sql.SparkSession
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.kafka.common.config.SslConfigs
import org.apache.spark.sql.streaming.ProcessingTime
import org.apache.spark.SparkConf
import java.util.Properties
import org.apache.spark.SparkContext
import org.apache.avro.SchemaBuilder
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import org.apache.spark.sql.SaveMode
object example {
  def main(args: Array[String]): Unit = {

    val sparkSession = org.apache.spark.sql.SparkSession.builder
      .config(new SparkConf().setMaster("local").setAppName("myApp")).getOrCreate()
    val df = sparkSession.readStream
      .format("kafka")
      .option("group.id", "SitecatRealtimeGrp11")
      .option("kafka.bootstrap.servers", "loalhsot:9092")
      .option("kafka.security.protocol", "SSL")
      .option("kafka.ssl.truststore.location", "/home/ec2-user/iaas.jks")
      .option("kafka.ssl.truststore.password", "#33!!@@ABC")
      .option("kafka.ssl.endpoint.identification.algorithm", "")
      .option("subscribe", "test")
      .option("failOnDataLoss", "false")
      .option("startingOffsets", "latest")
      .load()
    df.printSchema()
    var onlyMessge = df.select("key", "value").selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)") 
    val sc = sparkSession.sparkContext
    val sqs = new AmazonSQSClient()
    val query = onlyMessge.writeStream.foreachBatch { (batchDF: DataFrame, batchId: Long) => sendMessages(batchDF.rdd.map(r => (r.getString(0), r.getString(1))), sc, sqs)}.start()
    query.awaitTermination()

  }
}