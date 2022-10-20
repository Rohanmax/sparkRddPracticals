import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object wordcount extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("D:/weeknine/dataset.txt")
  
  val words = input.flatMap(x => x.split(" "))
  
  val wordMap = words.map(x => (x,1))
  
  val finalCount = wordMap.reduceByKey((a,b) => a+b)
  
  finalCount.collect.foreach(println)


  scala.io.StdIn.readLine()
  
}