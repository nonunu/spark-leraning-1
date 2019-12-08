package org.qzhd.spark

import org.apache.spark.sql.SparkSession


object App {

  def main(args : Array[String]) {
    //读取一个文本文件
    val spark = SparkSession.builder()
      .master("local")
      .appName("wordcount")
      .getOrCreate()

    val path="/Users/zhaosl/workspace/spark-leraning-1/data/iris.data"
    val rdd = spark.sparkContext.textFile(path)
    //逻辑处理
    //5.1,3.5,1.4,0.2,Iris-setosa
    //思路：首先用逗号分隔，每个词标记一个1，相同的词放到一块然后让这些1加和
    val result = rdd.flatMap(_.split(",")).map(x=>(x,1)).reduceByKey(_+_)
    //展现结果
    result.collect().foreach(println)
  }

}
