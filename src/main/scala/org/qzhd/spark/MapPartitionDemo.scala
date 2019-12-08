package org.qzhd.spark

import org.apache.spark.sql.SparkSession


object MapPartitionDemo {

  def main(args : Array[String]) {
    val spark = SparkSession.builder()
      .master("local")
      .appName("rddDemo")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.parallelize(Array(Array(1,2,3),Array(2,5)), 2)

    rdd.mapPartitions(x=>{
      x.map(arr => arr.filter(_ > 2))
    }).foreach(x=>{
      println(x.mkString(","))
    })
  }

}
