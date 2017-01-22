package com.qunar.spark.transmit

import com.qunar.spark.transmit.phase.{ExportPhase, ImportPhase}

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

    var exportPhase: ExportPhase = _

    var importPhase: ImportPhase = _

    def setExportPhase(exportPhase: ExportPhase): this.type = {
      this.exportPhase = exportPhase

      this
    }

    def setImportPhase(exportPhase: ExportPhase): this.type = {
      this.importPhase = importPhase

      this
    }

    def build: Task = {
      new Task(exportPhase, importPhase)
    }

  }

}
