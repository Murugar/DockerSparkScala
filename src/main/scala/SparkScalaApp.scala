
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf

object SparkScalaApp {
    def main(args: Array[String]) {
        val logFile = "/usr/local/spark_2.1.1/README.md" 
        val conf = new SparkConf().setAppName("A Very Simple Spark Scala Application")
        val sc = new SparkContext(conf)
        val logData = sc.textFile(logFile, 2).cache()
        val numAs = logData.filter(line => line.contains("a")).count()
        val numBs = logData.filter(line => line.contains("b")).count()
        sc.stop()
        println(s"==========================================")
        println(s"Spark Scala Lines with a: $numAs, Lines with b: $numBs")
        println(s"==========================================")
    }
}

