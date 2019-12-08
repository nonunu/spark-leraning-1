package org.qzhd.spark

import org.apache.spark.sql.SparkSession


object FlatMapDemo {

  def main(args : Array[String]) {
    val spark = SparkSession.builder()
      .master("local")
      .appName("rddDemo")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.parallelize(Array(Array(1,2,3),Array(2,5)))

    rdd.flatMap(x=>x).foreach(println)
    //rdd.foreach(x => println(x.mkString(",")))
    //rdd.take(4).foreach(println)

  }

}
