package com.qunar.spark.transmit.task.imports

import org.apache.spark.rdd.RDD

/**
  * 数据导入任务的抽象
  */
trait DataImportTask {

  /**
    * 导入数据的方法
    */
  def writeData(data:RDD[String],)

}
