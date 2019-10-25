package main.scala.com.spark.deep

import java.time.Duration;
import java.time.Instant;
import java.io.File 
import java.io.PrintWriter 
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.log4j._
import java.io.PrintWriter

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
        val conf = new SparkConf()
        conf.setAppName("Word Count").setMaster("local")
 
        //Create Spark Context with the config
        val sparkCtx = new SparkContext(conf)
        
        //Start timewatch
        val start = Instant.now();
        
        //Loading a text file
        val contents = sparkCtx.textFile(args(0))
 
        //Separate each word in each line
        val words = contents.flatMap(line => line.split(" "))
 
        //Create the key/value tuple with the word as key and value as 1
        val counts = words.map(word => Tuple2(word, 1))
 
        //Sum all item that has the same word
        val wordCounts = counts.reduceByKey((i, j) => i + j).sortByKey().collect()    

        //Sort and print list
        wordCounts.foreach(a=>text.append(a.toString() + "\n"))
        
        //println(text)

        //Write to the file
        val print_Writer = new PrintWriter(new File(output+"/"+fileName))  
        print_Writer.write(text.toString())
        
        //Close printwriter 
        print_Writer.close() 
        
        //Stop timewatch
        val finish = Instant.now();
        
        println(Duration.between(start, finish).toMillis()+"ms");
        
        //Stop the spark context
        sparkCtx.stop()
 
    }
}