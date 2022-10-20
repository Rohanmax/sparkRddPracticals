import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object frendsAgeCategory extends App {
  
  def parseLine(line:String) ={
    
    val fields = line.split("::")
    val age = fields(2).toInt
    val numOfFriends = fields(3).toInt
    (age,numOfFriends)
  }
  
   Logger.getLogger("org").setLevel(Level.ERROR)
  
  val sc = new SparkContext("local[*]","wordcount")
  
  val input = sc.textFile("D:/weeknine/friendsdata.csv")
  
  val mappedInput = input.map(parseLine)
  
  //(33, 100)
  
  val mappedFinal = mappedInput.map(x => (x._1,(x._2,1)))
  
  //(33, (100,1))
  //(33, (200,1))
  
  val totalByAge = mappedFinal.reduceByKey((x,y) => (x._1 + y._1 , x._2 + y._2))
  
   //(33, (200,2))
  //(34, (400,4))
  
  val averageconnections = totalByAge.map(x => (x._1,x._2._1 / x._2._2)).sortBy(x => x._2)
  
  // (33, (200 / 2)) = (33, (100))
    // (34, (400 / 4)) = (34, (100))
  
  averageconnections.collect.foreach(println)
  
  
}
