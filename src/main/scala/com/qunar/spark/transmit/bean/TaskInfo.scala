package com.qunar.spark.transmit.bean

/**
  * 任务信息
  */
case class TaskInfo(index: String,
                    `type`: String,
                    rangeFieldName: String,
                    startTime: Long,
                    endTime: Long) {

  def this(index: String,
           `type`: String,
           rangeFieldName: String) = this(index, `type`, rangeFieldName, null, null)

}
