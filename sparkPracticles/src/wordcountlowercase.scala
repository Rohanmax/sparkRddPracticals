import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.log4j.Logger


object wordcountlowercase extends App {

 Logger.getLogger("org").setLevel(Level.ERROR)
  
val sc = new SparkContext("local[*]","wordcount")
 
val input = sc.textFile("D:/weeknine/dataset.txt")

val words = input.flatMap(_.split(" "))

val wordsLower = words.map(_.toLowerCase())	

val wordMap = wordsLower.map((_,1))

val finalCount = wordMap.reduceByKey(_+_)

val sortedResults = finalCount.sortBy(x =>x._2)

val results = sortedResults.collect

for (result <-results) {
val word = result._1
val count = result._2
println(s"$word:$count")
}


scala.io.StdIn.readLine()
}