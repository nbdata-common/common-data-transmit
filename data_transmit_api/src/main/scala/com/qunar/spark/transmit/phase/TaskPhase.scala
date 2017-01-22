package com.qunar.spark.transmit.phase

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
  * 任务阶段的枚举类型
  */
object TaskPhaseType extends Enumeration {

  type TaskPhaseType = Value

  val EXPORT_PHASE, IMPORT_PHASE = Value

}

/**
  * 导出阶段
  */
abstract class ExportPhase private[transmit] extends TaskPhase {

  override def phaseType = TaskPhaseType.EXPORT_PHASE

}

/**
  * 导入阶段
  */
abstract class ImportPhase private[transmit] extends TaskPhase {

  override def phaseType = TaskPhaseType.IMPORT_PHASE

}
