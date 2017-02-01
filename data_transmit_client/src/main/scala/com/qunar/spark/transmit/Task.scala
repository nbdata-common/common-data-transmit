package com.qunar.spark.transmit

import com.qunar.spark.base.io.HdfsService
import com.qunar.spark.base.json.JsonMapper
import com.qunar.spark.base.log.Logging
import com.qunar.spark.transmit.phase._
import org.apache.hadoop.fs.Path
import org.joda.time.DateTime

import scala.collection.immutable.HashMap

/**
  * 任务信息的快照,用以生成执行计划
  */
class Task private[transmit](private val taskName: String,
                             private val exportPhase: ExportPhase,
                             private val importPhase: ImportPhase) extends Logging {

  /**
    * 提交传输数据的任务快照至Daemon服务,触发服务执行任务
    */
  def transmitData(): Unit = {
    if (!writeToTransLog(exportPhase, importPhase)) {
      logError(s"taskExecutionPlan writeToTransLog fail\n " +
        s"exportPhasePlan = ${JsonMapper.writeValueAsString(exportPhase.genPhaseExecutionPlan)}\n" +
        s"importPhasePlan = ${JsonMapper.writeValueAsString(importPhase.genPhaseExecutionPlan)}")
    }
  }

  /**
    * 将执行计划写入事务预写日志
    *
    * @return 是否预写成功
    */
  private def writeToTransLog(exportPhase: ExportPhase, importPhase: ImportPhase): Boolean = {
    val exportPhasePlan = exportPhase.genPhaseExecutionPlan
    val exportPhaseType = exportPhase.phaseType
    val importPhasePlan = importPhase.genPhaseExecutionPlan
    val importPhaseType = importPhase.phaseType
    val taskInfo = new TaskInfo(exportPhaseType, exportPhasePlan, importPhaseType, importPhasePlan)

    writeToTransLogInternal(taskInfo)
  }

  /**
    * 内部具体实现hdfs落地的逻辑
    */
  private def writeToTransLogInternal(taskInfo: TaskInfo): Boolean = {
    val transLogPath = "/data-transmit/translog/" + DateTime.now().toString("yyyyMMddHHmmss") + "/" + taskInfo
    HdfsService.writeContentToPath(new Path(transLogPath), JsonMapper.writeValueAsString(taskInfo))
  }

}

object Task {

  def builder = new TaskBuilder

  class TaskBuilder {

    private var taskName: String = _

    private var exportPhaseBuilder: ExportPhaseBuilder = _

    private var importPhaseBuilder: ImportPhaseBuilder = _

    def setTaskName(taskName: String): this.type = {
      this.taskName = taskName
      this
    }

    def elasticsearchExportPhaseBuilder = {
      val taskPhaseBuilder = TaskPhaseBuilder.elasticSearchExportPhaseBuilder(this)
      exportPhaseBuilder = taskPhaseBuilder

      taskPhaseBuilder
    }

    def elasticsearchImportPhaseBuilder = {
      val taskPhaseBuilder = TaskPhaseBuilder.elasticSearchImportPhaseBuilder(this)
      importPhaseBuilder = taskPhaseBuilder

      taskPhaseBuilder
    }

    def hdfsExportPhaseBuilder = {
      val taskPhaseBuilder = TaskPhaseBuilder.hdfsExportPhaseBuilder(this)
      exportPhaseBuilder = taskPhaseBuilder

      taskPhaseBuilder
    }

    def hdfsImportPhaseBuilder = {
      val taskPhaseBuilder = TaskPhaseBuilder.hdfsImportPhaseBuilder(this)
      importPhaseBuilder = taskPhaseBuilder

      taskPhaseBuilder
    }

    def buildTask: Task = {
      new Task(taskName, exportPhaseBuilder.buildPhase, importPhaseBuilder.buildPhase)
    }

  }

}

/**
  * 任务信息的标准java bean,与daemon服务通信的唯一媒介
  * 通过将其写入hdfs,引起daemon端监听服务active,执行任务
  * NOTICE: 这里不使用scala case class,是考虑到jackson的兼容性限制.因为
  * 采用jackson序列化,使用标准的java bean是最稳定的.
  */
class TaskInfo(exportType: ExportPhaseType,
               exportInfo: HashMap[String, String],
               importType: ImportPhaseType,
               importInfo: HashMap[String, String]) {

  def this() = this(null, null, null, null)

  // 数据导出阶段的类型
  private var exportPhaseType: ExportPhaseType = exportType

  // 数据导出阶段的信息
  private var exportPhaseInfo: HashMap[String, String] = exportInfo

  // 数据导入阶段的类型
  private var importPhaseType: ImportPhaseType = importType

  // 数据导入阶段的信息
  private var importPhaseInfo: HashMap[String, String] = importInfo

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
