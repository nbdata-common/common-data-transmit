package com.qunar.spark.transmit

import com.qunar.spark.transmit.bean.TaskInfo
import com.qunar.spark.transmit.service.elasticsearch.SearchBuilder
import com.qunar.spark.transmit.task.exports.ElasticSearchExport
import com.qunar.spark.transmit.task.imports.ElasticSearchImport

class Main {

  def main(args: Array[String]) {
    val taskInfo = readConfig(args)
    val queryStr = SearchBuilder(taskInfo.rangeFieldName, taskInfo.startTime, taskInfo.endTime).build
    val data = new ElasticSearchExport().readDataFromEs(taskInfo.index, taskInfo.`type`, queryStr)
    new ElasticSearchImport().writeDataToEs(data, taskInfo.index, taskInfo.`type`)
  }

  private def readConfig(args: Array[String]): TaskInfo = {
    if (args.length < 3) {
      throw new IllegalArgumentException("参数数量不够,至少需要:index,type,rangeFieldName.")
    } else if (args.length >= 5) {
      TaskInfo(args(0), args(1), args(2), args(3).toLong, args(4).toLong)
    } else {
      new TaskInfo(args(0), args(1), args(2))
    }
  }


}
