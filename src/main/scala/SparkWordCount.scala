/* SparkWordCount.scala */
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SimpleApp {
  def main(args: Array[String]) {
    val logFile = "/user/root/inputfile.txt" // Should be some file on your system
    val conf = new SparkConf().setAppName("Spark Word Count")
    val sc = new SparkContext(conf)

    val logData = sc.textFile(logFile, 2).cache()

    // read in text file and split each document into words
    val tokenized = sc.textFile(logFile).flatMap(_.split(" "))

    // count the occurrence of each word
    val wordCounts = tokenized.map((_, 1)).reduceByKey(_ + _)

    // filter out words with fewer than threshold occurrences
    val filtered = wordCounts.filter(_._2 >= 2)

    // count characters
    val charCounts = filtered.flatMap(_._1.toCharArray).map((_, 1)).reduceByKey(_ + _)

    System.out.println(filtered.collect().mkString(", "))

    System.out.println(charCounts.collect().mkString(", "))

    val numAs = logData.filter(line => line.contains("a")).count()
    val numBs = logData.filter(line => line.contains("b")).count()
    println("Lines with a: %s, Lines with b: %s".format(numAs, numBs))
  }
}

