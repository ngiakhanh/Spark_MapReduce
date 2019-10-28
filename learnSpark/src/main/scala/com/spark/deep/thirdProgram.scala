package main.scala.com.spark.deep

import java.time.Duration;
import java.time.Instant;
import java.io.File 
import java.io.PrintWriter 
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._
import java.io.PrintWriter
import java.io.FileOutputStream

import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.common.serialization.StringDeserializer
import org.apache.spark.streaming.kafka010._
import org.apache.spark.streaming.kafka010.LocationStrategies.PreferConsistent
import org.apache.spark.streaming.kafka010.ConsumerStrategies.Subscribe

import org.apache.spark._
import org.apache.spark.streaming._

import scala.collection.JavaConverters._

import org.apache.spark.sql.SparkSession

object thirdProgram {
  def main(args: Array[String]): Unit={
        val input = args(0)
        val output = "output"
        val fileName = "results.txt"
        val text = StringBuilder.newBuilder
        Logger.getLogger("org").setLevel(Level.ERROR)
        
        System.out.println("Working directory: "+System.getProperty("user.dir"));
        
        //Check if the path exists and create one if not
        var directory = new File(input.split("/")(0))
        if (!directory.exists()){
          directory.mkdirs();
        }
        
        directory = new File(output)
        if (!directory.exists()){
          directory.mkdirs();
        }
             
        //Create config
        val conf = new SparkConf().setAppName("Word Count").setMaster("local[*]")
 
        //Create Spark Context with the config
        
        val sc = new SparkContext(conf)
        val ssc = new StreamingContext(sc, Seconds(1))
        
        //val spark = SparkSession
                    //.builder()
                    //.appName("Spark structured streaming Kafka example")
                    //.master("local[*]")
                    //.getOrCreate()
        //import spark.implicits._
                    
        //val df = spark
                //.readStream
                //.format("kafka")
                //.option("kafka.bootstrap.servers", "localhost:9092")
                //.option("subscribe", "test")
                //.option("startingOffsets", "earliest")
                //.load()
        //df.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
          //.as[(String, String)]
        //df.writeStream
          //.format("console")
          //.option("truncate","false")
          //.start()
          //.awaitTermination()
        
        
        val kafkaParams = Map[String, Object](
        "bootstrap.servers" -> "localhost:9092",
        "key.deserializer" -> classOf[StringDeserializer],
        "value.deserializer" -> classOf[StringDeserializer],
        "group.id" -> "test1",
        "auto.offset.reset" -> "earliest",
        "enable.auto.commit" -> (false: java.lang.Boolean)
        )

        val topic = Array("test")
        val kafkaRawStream =
            KafkaUtils.createDirectStream[String, String](
            ssc,
            PreferConsistent,
            Subscribe[String, String](topic, kafkaParams)
        )
        
        val lines = kafkaRawStream.map(consumerRecord => consumerRecord.value)
        val words = lines.flatMap(line => line.split(" "))

        val wordMap = words.map(word => (word, 1))
        
        val wordCount = wordMap.reduceByKey((i, j) => i + j)
        
        val print_Writer = new PrintWriter(new FileOutputStream(new File(output+"/"+fileName), false))
        print_Writer.write("")
        print_Writer.close()
        
        wordCount.foreachRDD(rdd => {
          if (rdd.count() > 0){
            //rdd.keys.foreach(println)
            text.setLength(0)
            rdd.collect().foreach(a=>{
                text.append(a.toString()+"\n")
            })
            text.append("-----------\n")
            println(text)
            //Write to the file
            val print_Writer = new PrintWriter(new FileOutputStream(new File(output+"/"+fileName), true))  
            print_Writer.write(text.toString())
            
            //Close printwriter 
            print_Writer.close() 
          }
        })
        
        ssc.start()
        ssc.awaitTermination()
        
        //val stream = KafkaUtils.createDirectStream[String, String](
          //ssc,
          //PreferConsistent,
          //Subscribe[String, String](topics, kafkaParams)
        //)

        //stream.map(record => (record.key, record.value))
        
        
        //val offsetRanges = Array(
        // topic, partition, inclusive starting offset, exclusive ending offset
        OffsetRange("test", 1, 1, 100)
        //)

        //val rdd = KafkaUtils.createRDD[String, String](sc, kafkaParams.asJava,
        //offsetRanges, LocationStrategies.PreferConsistent)
        
        //stream.foreachRDD { rdd =>
          //val offsetRanges = rdd.asInstanceOf[HasOffsetRanges].offsetRanges
          //rdd.foreachPartition { iter =>
            //val o: OffsetRange = offsetRanges(TaskContext.get.partitionId)
            //println(s"${o.topic} ${o.partition} ${o.fromOffset} ${o.untilOffset}")
          //}
          //println("OK")
        //}
        
        //println("OK")
        
        
        //Start timewatch
        //val start = Instant.now();
        
        //Loading a text file
        //val contents = sc.textFile(args(0))
 
        //Separate each word in each line
        //val words = contents.flatMap(line => line.split(" "))
 
        //Create the key/value tuple with the word as key and value as 1
        //val counts = words.map(word => Tuple2(word, 1))
 
        //Sum all item that has the same word
        //val wordCounts = counts.reduceByKey((i, j) => i + j).sortByKey().collect()    

        //Sort and print list
        //wordCounts.foreach(a=>text.append(a.toString() + "\n"))
        
        //println(text)

        //Write to the file
        //val print_Writer = new PrintWriter(new File(output+"/"+fileName))  
        //print_Writer.write(text.toString())
        
        //Close printwriter 
        //print_Writer.close() 
        
        //Stop timewatch
        //val finish = Instant.now();
        
        //println(Duration.between(start, finish).toMillis()+"ms");
        
        //Stop the spark context
        //sc.stop()
        
        
 
    }
}