package com.qunar.spark.transmit.base

import org.apache.spark.sql.SparkSession

/**
  * spark上下文创建的工厂
  */
object SparkSessions {

  private val propertiesLoader = PropertiesLoader("spark")

  val session = SparkSession.builder()
    .master(propertiesLoader.getStr("spark.master"))
    .appName(propertiesLoader.getStr("spark.app.name"))
    .config("spark.serializer", propertiesLoader.getStr("spark.serializer"))
    .getOrCreate

  session.sparkContext.hadoopConfiguration.set("mapreduce.input.fileinputformat.input.dir.recursive", "true")

  /**
    * 获取全局的[[SparkSession]]
    */
  def getSparkSession: SparkSession = session

}
