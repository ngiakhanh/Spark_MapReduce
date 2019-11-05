package main.scala.com.spark.deep

import java.time._;
import java.sql.Timestamp
import java.util.Properties;
import java.io.File 
import java.io.PrintWriter 
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._
import java.io.PrintWriter
import java.io.FileOutputStream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.producer._
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark._
import org.apache.spark.streaming._

import org.apache.spark.sql.expressions.UserDefinedFunction
import org.apache.spark.sql.SparkSession
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

object fourProgram {
  def main(args: Array[String]): Unit={
    val spark = SparkSession.builder
                            .appName("Spark-Kafka-Integration")
                            .master("local[*]")
                            .getOrCreate()
    
    val sc = spark.sparkContext
               
    import spark.implicits._

    val mySchema = StructType(Array(
                     StructField("id", StringType),
                     StructField("age", IntegerType),
                     StructField("cv", StringType)
    ))      
    
    val df = spark.readStream
                  .format("kafka")
                  .option("kafka.bootstrap.servers", "localhost:9092")
                  .option("subscribe", "test")
                  .option("startingOffsets", "latest")
                  .load()
    
    val personDf = df.selectExpr("CAST(value AS STRING)")
                     .select(from_json($"value", mySchema).as("person"))
                     .selectExpr("person.id", "person.age", "person.cv")
    
    personDf.writeStream
            .outputMode("append")
            .format("console")
            .start()
        
    val wcFunc: String => String = para => {
      val text = StringBuilder.newBuilder
      val spark = SparkSession.builder
                              .appName("Spark-Kafka-Integration")
                              .master("local[*]")
                              .getOrCreate()
                              
      val lines = para.split("\n").map(a=>a.replaceAll("[^a-zA-Z0-9]+"," "))
      var distLines = spark.sparkContext.parallelize(lines)
      val words = distLines.flatMap(_.split(" "))
                           .map((_, 1))
                           .reduceByKey((i, j) => i + j)
                           
      words.collect().foreach(a=>text.append(a.toString()))                     
      text.toString()
    }
    
    val ageUdf: UserDefinedFunction = udf(wcFunc, DataTypes.StringType)
    
    val processedDf = personDf.withColumn("wc", ageUdf.apply($"cv"))
    
    //val value_fields = resDf.columns
    val resDf = processedDf.withColumn("value", to_json(struct(col("*"))))
                           .select("value")
           
    resDf.writeStream
         .outputMode("append")
         .format("console")
         .start()
    
            
    resDf.writeStream
          .format("kafka")
          .option("kafka.bootstrap.servers", "localhost:9092")
          .option("topic", "mongo")
          .option("checkpointLocation", "C:/Users/GNG04/.kafka/Checkpoints")
          .start()
                         
    spark.streams.awaitAnyTermination()                            
  }
}