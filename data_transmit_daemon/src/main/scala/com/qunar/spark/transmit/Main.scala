package com.qunar.spark.transmit

import com.qunar.spark.transmit.phase.elasticsearch.ElasticsearchExportPhase$
import com.qunar.spark.transmit.service.elasticsearch.SearchBuilder
import com.qunar.spark.transmit.task.exports.ext.ElasticSearchExport
import com.qunar.spark.transmit.task.imports.ext.ElasticSearchImport

class Main {

  def main(args: Array[String]) {
    val taskInfo = readConfig(args)
    val queryStr = SearchBuilder(taskInfo.rangeFieldName, taskInfo.startTime, taskInfo.endTime).build
    val data = new ElasticSearchExport().readDataFromEs(taskInfo.index, taskInfo.`type`, queryStr)
    new ElasticSearchImport().writeDataToEs(data, taskInfo.index, taskInfo.`type`)
  }

  private def readConfig(args: Array[String]): Task = {
    if (args.length < 3) {
      throw new IllegalArgumentException("参数数量不够,至少需要:index,type,rangeFieldName.")
    } else if (args.length >= 5) {
      Task(args(0), args(1), args(2), args(3).toLong, args(4).toLong)
    } else {
      new Task(args(0), args(1), args(2))
    }
  }

}
