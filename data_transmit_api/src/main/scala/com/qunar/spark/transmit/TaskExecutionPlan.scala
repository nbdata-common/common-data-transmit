package com.qunar.spark.transmit

import scala.collection.immutable.HashMap

/**
  * 任务执行计划表
  * 包括导出阶段[[com.qunar.spark.transmit.phase.ExportPhase]]与导入阶段[[com.qunar.spark.transmit.phase.ImportPhase]]
  */
class TaskExecutionPlan private[transmit](private val exportPhaseInfo: HashMap[String, String],
                                          private val importPhaseInfo: HashMap[String, String]) {

  /**
    * 将执行计划写入事务预写日志
    *
    * @return 是否预写成功
    */
  def writeToTransLog: Boolean = {
    false
  }

}

object TaskExecutionPlan {

  def apply(exportPhaseInfo: HashMap[String, String],
            importPhaseInfo: HashMap[String, String]): TaskExecutionPlan =
    new TaskExecutionPlan(exportPhaseInfo, importPhaseInfo)

}
