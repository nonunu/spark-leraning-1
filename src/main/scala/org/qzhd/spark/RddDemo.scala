package org.qzhd.spark

import org.apache.spark.sql.SparkSession


object RddDemo {

  def main(args : Array[String]) {
    val spark = SparkSession.builder()
      .master("local")
      .appName("rddDemo")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.parallelize(Array(1,2,3,4,9))

    val rdd1 = rdd.map(x=>{
      println(x)
      x * 2
    })
    rdd1.foreach(println)
    //rdd.take(4).foreach(println)

  }

}
