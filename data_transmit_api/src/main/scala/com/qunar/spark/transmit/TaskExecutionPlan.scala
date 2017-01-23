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
    //todo jackson serialize task info
    false
  }

}

object TaskExecutionPlan {

  def apply(exportPhaseInfo: HashMap[String, String],
            importPhaseInfo: HashMap[String, String]): TaskExecutionPlan =
    new TaskExecutionPlan(exportPhaseInfo, importPhaseInfo)

}

class TaskInfo {

  private var exportPhaseType: ExportPhaseType = _

  private var exportPhaseInfo: HashMap[String, String] = _

  private var importPhaseType: ImportPhaseType = _

  private var importPhaseInfo: HashMap[String, String] = _

  def getExportPhaseType = exportPhaseType

  def setExportPhaseType(exportPhaseType: ExportPhaseType) = {
    this.exportPhaseType = exportPhaseType
  }

  def getExportPhaseInfo = exportPhaseInfo

  def setExportPhaseInfo(exportPhaseInfo: HashMap[String, String]) = {
    this.exportPhaseInfo = exportPhaseInfo
  }

  def getImportPhaseType = importPhaseType

  def setImportPhaseType(importPhaseType: ImportPhaseType) = {
    this.importPhaseType = importPhaseType
  }

  def getImportPhaseInfo = importPhaseInfo

  def setImportPhaseInfo(importPhaseInfo: HashMap[String, String]) = {
    this.importPhaseInfo = importPhaseInfo
  }

}
