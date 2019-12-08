package org.qzhd.spark

import org.apache.spark.sql.SparkSession


object GroupByDemo {

  def main(args : Array[String]) {
    val spark = SparkSession.builder()
      .master("local")
      .appName("rddDemo")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.parallelize(Array(("OK",1),("OK",3),("yes",4),("No",6)))

    rdd.groupBy(x=>x._1).foreach(println)

  }

}
