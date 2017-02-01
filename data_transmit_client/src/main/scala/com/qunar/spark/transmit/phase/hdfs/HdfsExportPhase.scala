package com.qunar.spark.transmit.phase.hdfs

import com.google.common.base.Strings
import com.qunar.spark.base.log.Logging
import com.qunar.spark.transmit.ExportPhaseType
import com.qunar.spark.transmit.Task.TaskBuilder
import com.qunar.spark.transmit.phase.{ExportPhase, ExportPhaseBuilder, PhaseConstants, TaskPhaseType}

import scala.collection.immutable.HashMap

/**
  * 针对hdfs的数据导出服务
  */
final class HdfsExportPhase(private val path: String) extends ExportPhase with Logging {

  override def phaseType: ExportPhaseType = TaskPhaseType.HDFS_EXPORT_PHASE

  override def genPhaseExecutionPlan: HashMap[String, String] = {
    val planBuilder = HashMap.newBuilder[String, String]

    if (Strings.isNullOrEmpty(path)) {
      logError("HdfsExportPhase genPhaseExecutionPlan: path is empty")
    }

    planBuilder += (PhaseConstants.HDFS_PATH -> path)
    planBuilder.result
  }

  override def genExtraInfo: HashMap[String, String] = null

}

final class HdfsExportPhaseBuilder(private val hostTask: TaskBuilder) extends ExportPhaseBuilder(hostTask) with Logging {

  private var path: String = _

  def setPath(path: String): this.type = {
    this.path = path
    this
  }

  protected[transmit] override def buildPhase: HdfsExportPhase = {
    new HdfsExportPhase(path)
  }

}
