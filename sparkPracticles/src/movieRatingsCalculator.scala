import org.apache.log4j.Level
import org.apache.spark.SparkContext
import org.apache.log4j.Logger


object movieRatingsCalculator extends App {

Logger.getLogger("org").setLevel(Level.ERROR)
  
val sc = new SparkContext("local[*]","wordcount")

val input = sc.textFile("D:/weeknine/movie.data")

val mappedInput = input.map(x => (x.split("\t")(2)))

val ratings = mappedInput.map(x =>(x,1))

val reducedRatings = ratings.reduceByKey((x,y)=> x+y)

val results = reducedRatings.collect

results.foreach(println)


}