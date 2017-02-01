package com.qunar.spark.transmit.phase.hdfs

import com.google.common.base.Strings
import com.qunar.spark.base.log.Logging
import com.qunar.spark.transmit.Task.TaskBuilder
import com.qunar.spark.transmit._
import com.qunar.spark.transmit.phase.{ImportPhase, ImportPhaseBuilder, PhaseConstants, TaskPhaseType}

import scala.collection.immutable.HashMap

/**
  * 针对hdfs的数据导入服务
  */
class HdfsImportPhase(private val path: String) extends ImportPhase with Logging {

  override def phaseType: ExportPhaseType = TaskPhaseType.HDFS_IMPORT_PHASE

  override def genPhaseExecutionPlan: HashMap[String, String] = {
    val planBuilder = HashMap.newBuilder[String, String]

    if (Strings.isNullOrEmpty(path)) {
      logError("HdfsImportPhase genPhaseExecutionPlan: path is empty")
    }

    planBuilder += (PhaseConstants.HDFS_PATH -> path)
    planBuilder.result
  }

  override def genExtraInfo: HashMap[String, String] = null

}

final class HdfsImportPhaseBuilder(private val hostTask: TaskBuilder) extends ImportPhaseBuilder(hostTask) with Logging {

  private var path: String = _

  def setPath(path: String): this.type = {
    this.path = path
    this
  }

  protected[transmit] override def buildPhase: HdfsImportPhase = {
    new HdfsImportPhase(path)
  }

}
