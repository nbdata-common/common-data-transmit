package com.qunar.spark.transmit

import com.qunar.spark.transmit.phase._

import scala.language.implicitConversions

/**
  * 任务信息的快照,用以生成执行计划
  */
class Task private[transmit](private val exportPhase: ExportPhase,
                             private val importPhase: ImportPhase) {

  /**
    * 开始传输数据
    */
  def transmitData(): Unit = {
    val exportPhasePlan = exportPhase.genPhaseExecutionPlan
    val importPhasePlan = importPhase.genPhaseExecutionPlan
    val taskExecutionPlan = TaskExecutionPlan(exportPhasePlan, importPhasePlan)
    // 写入执行计划,通知Daemon服务执行任务
    if (!taskExecutionPlan.writeToTransLog) {
      //todo: extends Logging and log error here
    }
  }

}

object Task {

  def builder = new TaskBuilder

  class TaskBuilder {

    private var exportPhaseBuilder: ExportPhaseBuilder = _

    private var importPhaseBuilder: ImportPhaseBuilder = _

    def exportPhaseBuilder(exportPhaseType: ExportPhaseType): ExportPhaseBuilder = {
      exportPhaseType match {
        case TaskPhaseType.ELASTIC_SEARCH_EXPORT_PHASE =>
          exportPhaseBuilder = TaskPhaseBuilder.elasticSearchExportPhaseBuilder(this)
          exportPhaseBuilder
        case TaskPhaseType.HDFS_EXPORT_PHASE => null
      }
    }

    def importPhaseBuilder(importPhaseType: ImportPhaseType): ImportPhaseBuilder = {
      importPhaseType match {
        case TaskPhaseType.ELASTIC_SEARCH_IMPORT_PHASE =>
          importPhaseBuilder = TaskPhaseBuilder.elasticSearchImportPhaseBuilder(this)
          importPhaseBuilder
        case TaskPhaseType.HDFS_IMPORT_PHASE => null
      }
    }

    def buildTask: Task = {
      new Task(exportPhaseBuilder.buildPhase, importPhaseBuilder.buildPhase)
    }

  }

}
