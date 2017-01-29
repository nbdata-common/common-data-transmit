package com.qunar.spark.transmit.phase

import com.qunar.spark.transmit.{ExportPhaseType, ImportPhaseType}
import com.qunar.spark.transmit.phase.TaskPhaseType.TaskPhaseType

import scala.collection.immutable.HashMap

/**
  * 任务的阶段,共分为两种:导出阶段[[ExportPhase]]和导入阶段[[ImportPhase]]
  */
sealed trait TaskPhase {

  def phaseType: TaskPhaseType

  def genPhaseExecutionPlan: HashMap[String, String]

}

/**
  * 导出阶段
  */
abstract class ExportPhase private[transmit] extends TaskPhase {

  override def phaseType: ExportPhaseType

}

/**
  * 导入阶段
  */
abstract class ImportPhase private[transmit] extends TaskPhase {

  override def phaseType: ImportPhaseType

}
