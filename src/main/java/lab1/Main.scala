package lab1

import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by Evegeny on 12/10/2016.
  */
object Main {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\util\\hadoop-common-2.2.0-bin-master\\")
    val conf: SparkConf = new SparkConf().setAppName("taxi").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("data/taxi/trips.txt")
    rdd.foreach(println)
  }
}
