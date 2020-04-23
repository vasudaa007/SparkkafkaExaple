package org.sudhir.example

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