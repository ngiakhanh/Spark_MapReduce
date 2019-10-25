package main.scala.com.spark.deep
import org.apache.spark.mllib.feature.Word2VecModel
import org.apache.spark.mllib.feature.Word2Vec
import org.apache.log4j._
import org.apache.spark.sql.SparkSession
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.mllib.linalg.Vectors


object firstProgram {
  def main(args:Array[String])={
    
    Logger.getLogger("org").setLevel(Level.ERROR)
    
    val spark = new SparkConf().setAppName("word2vec").setMaster("local")
    var sc = new SparkContext(spark)
    val input = sc.textFile("text8/text8.txt").map(line => line.split(" ").toSeq)

    val word2vec = new Word2Vec()
    
    //val model = word2vec.fit(input)
    val model = Word2VecModel.load(sc, "model")
    
    //var vector1 = model.getVectors("data")
    //var vector2 = model.getVectors("scientist")
    //val vector3 = vector1.zip(vector2).map { case (x, y) => x - y }

    //val synonyms = model.findSynonyms(Vectors.dense(vector1.map(_.toDouble)), 5)
    var word = "earth"
    val synonyms = model.findSynonyms(word, 5)
    
    for((synonym, cosineSimilarity) <- synonyms) {
      println(s"$synonym")
    }
    
    // Save and load model
    //model.save(sc, "C:/Users/GNG04/Desktop/eclipse/model")
    //val sameModel = Word2VecModel.load(sc, "C:/Users/GNG04/Desktop/eclipse/model")

    sc.stop()
  }
}