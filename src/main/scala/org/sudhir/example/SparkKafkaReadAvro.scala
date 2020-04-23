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
object SparkKafkaReadAvro
 {
   def main(args: Array[String]): Unit = {
       val spark = SparkSession.builder.appName("SparkStreamingKafka").config("spark.executor.memory", "4g").config("spark.executor.instances", 2).config("spark.driver.maxResultSize","4g").config("spark.jars","/home/sudhir/workspace/spark-avro_2.11-2.4.0.jar,/home/sudhir/workspace/spark-sql-kafka-0-10_2.11-2.1.0.jar").getOrCreate()


        val jsonFormatSchema = """{"type": "record","name": "struct","fields": [{"name": "key", "type": "long"},{"name": "value", "type": "string"}]}"""
        val df = spark.readStream
					.format("kafka")
					.option("kafka.bootstrap.servers", "localhost:9092")
					.option("subscribe", "test")
					.option("startingOffsets", "latest")
					.option("failOnDataLoss" ,"false")
					.option("kafka.client.id" ,"xyz")
					.option("kafka.security.protocol", "SSL")
					.option("kafka.ssl.truststore.location", "/home/sudhir/truststore.jks")
					.option("kafka.ssl.keystore.location", "/home/sudhir/test.p12")
					.option("kafka.ssl.keystore.type", "PKCS12")
					.option("kafka.ssl.keystore.password", "ABC123")
					.option("kafka.ssl.truststore.password","changeme")
					.option("kafka.ssl.endpoint.identification.algorithm", "")
					.load()
		val output = df.select(form_avro($"key",jsonFormatSchema).as("key"),from_avro($"value", jsonFormatSchema).as("value"))
		val query = output.writeStream.format("console").option("truncate","false").start().awaitTermination()


   }

}