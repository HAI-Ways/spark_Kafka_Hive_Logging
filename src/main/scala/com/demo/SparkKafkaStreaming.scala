package com.demo

import java.util.Properties
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.SparkSession
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SparkKafkaStreaming {

  val spark = SparkSession.builder.appName("SparkStreaming").getOrCreate()
  import spark.implicits._
  val sc = spark.sparkContext
  val ssc = new StreamingContext(sc, Seconds(5))

  val p = new Properties
  p.load(this.getClass.getResourceAsStream("./config.properties"))
  val hiveDB:String = p.getProperty("hiveDB")
  val hiveTbl:String = p.getProperty("hiveTbl")
  val bootstrapServers:String = p.getProperty("bootstrapServers")
  val topic:String = p.getProperty("topic")

  val hiveDbTbl = s"$hiveDB.$hiveTbl"

  //push static data into Kafka in a batch
  def logToKafka(info:String):Unit = {
    println("------------------------START FUNCTION logToKafka--------------------")

    val infoSql =  s"""
         SELECT '$topic' as topic,
         '$info' as value,
         current_timestamp() as timesatmp
     """.stripMargin

    try{
      val df = spark.sql(infoSql)

      df.selectExpr("topic","CAST(value AS STRING)","timestamp")
        .write
        .format("kafka")
        .option("kafka.bootstrap.servers",bootstrapServers)
        .save()
    } catch {
      case e:Exception => println(s"FUNCTION logToKafka FAILED WITH ERROR: $e")
    }

    println("------------------------FINISH FUNCTION logToKafka--------------------")
  }

  //create a hive table to store fetched Kafka data for the first run
  def createHiveTbl(hiveTblNm:String):Unit = {
    println("------------------------START FUNCTION createHiveTbl--------------------")

    try{
      if(!spark.catalog.tableExists(hiveTblNm)){
        val sql = s"""
        SELECT current_timestamp() as LogTime,
        'hive table $hiveTblNm is created' as logInfo
        """

        spark.sql(sql).write.saveAsTable(hiveTblNm)
      }
    } catch {
      case e:Exception => println(s"FUNCTION createHiveTbl FAILED WITH ERROR: $e")
    }

    println("------------------------FINISH FUNCTION createHiveTbl--------------------")
  }

  //write kafka stream data into hive table
  def kafkaToHive(hiveTblNm:String):Unit = {
    println("------------------------START FUNCTION kafkaToHive--------------------")

    try{
      val df = spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers",bootstrapServers)
        .option("subscribe",topic)
        .load()

      //for testing and debugging
      //println(s"df schema: ${df.schema.treeString}")
      //df.show(false)
      //val lines = df.selectExpr("CAST(value AS STRING)").as[String]
      //val wordCounts = lines.flatMap(_.split(" ")).groupBy("value").count()
      //val query = wordCounts.writeStream.outputMode("append").format("console").start()
      //query.stop()

      df.selectExpr("CAST(timestamp AS STRING)","CAST(value AS STRING)")
        .toDF("logTime","logInfo")
        .writeStream
        .foreachBatch{(batchDF:DataFrame, batchId:Long) =>
          batchDF.write.mode("append").saveAsTable(hiveTblNm)
        }
        .outputMode("update")
        .start()
        .awaitTermination()
    } catch {
      case e:Exception => println(s"FUNCTION kafkaToHive FAILED WITH ERROR: $e")
    }

    println("------------------------FINISH FUNCTION kafkaToHive--------------------")

  }

  def main(args:Array[String]):Unit = {
    println("------------------------START FUNCTION main--------------------")

    createHiveTbl(hiveDbTbl)

    logToKafka("push this test info into kafka")

    kafkaToHive(hiveDbTbl)


    println("------------------------FINISH FUNCTION main--------------------")
  }


}
