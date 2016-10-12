package lab1

import org.apache.spark.rdd.RDD
import org.apache.spark.{Accumulator, SparkConf, SparkContext}

/**
  * Created by Evegeny on 12/10/2016.
  */
object Main {
  def main(args: Array[String]): Unit = {
    System.setProperty("hadoop.home.dir", "C:\\util\\hadoop-common-2.2.0-bin-master\\")
    val conf: SparkConf = new SparkConf().setAppName("taxi").setMaster("local[*]")
    val sc: SparkContext = new SparkContext(conf)
    val rdd: RDD[String] = sc.textFile("data/taxi/trips.txt")
    val driverTrips: RDD[(String, Int)] = rdd.map(line => {
      val arr = line.split(" ")
      (arr(0), arr(2).toInt)
    })

    val bigTrips: Accumulator[Int] = sc.accumulator(0,"big trips")
    driverTrips.foreach(tuple=>{
     if(tuple._2>10) bigTrips+=1
    })

    println(s"big trips is:  ${bigTrips.value}")
    val take: Array[(String, Int)] = driverTrips.reduceByKey(_+_).sortBy(_._2,ascending = false).take(3)
    val rdd1: RDD[(String, Int)] = sc.parallelize(take)
    val rddDrivers: RDD[String] = sc.textFile("data/taxi/drivers.txt")
    val rdd2: RDD[(String, String)] = rddDrivers.map(line => {
      val strings: Array[String] = line.split(",")
      (strings(0), strings(1))
    })
    rdd2.join(rdd1).foreach(println)

  }
}
