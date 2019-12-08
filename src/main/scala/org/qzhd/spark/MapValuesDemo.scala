package org.qzhd.spark

import org.apache.spark.sql.SparkSession


object MapValuesDemo {

  def main(args : Array[String]) {
    val spark = SparkSession.builder()
      .master("local")
      .appName("rddDemo")
      .getOrCreate()

    val sc = spark.sparkContext

    val rdd = sc.parallelize(Array(("OK","pp"),("OK","pp"),("yes","pp"),("yes","pp")))

//    rdd.map(x=>(x._1,1)).reduceByKey(_+_)
//    rdd.mapValues(_=>1).reduceByKey(_+_)
    rdd.mapValues(_=>1).reduceByKey(_+_).saveAsTextFile("/Users/zhaosl/workspace/company/sparkdemo/data/demo")

  }

}
