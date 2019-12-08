package org.qzhd.spark

import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession


object UnionDemo {

  def main(args : Array[String]) {
    //读取一个文本文件
    val spark = SparkSession.builder()
      .master("local")
      .appName("wordcount")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd1 = sc.parallelize(Array(1,2,3,5))
    val rdd2 = sc.parallelize(Array(3,5))

    // union
    //rdd1.union(rdd2).foreach(println)

    rdd1.cartesian(rdd2).foreach(println)
  }

}
