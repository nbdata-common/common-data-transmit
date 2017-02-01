package com.qunar.spark.transmit.phase.memory

import com.qunar.spark.base.io.{HdfsFileType, HdfsSparkService}
import com.qunar.spark.base.json.JsonMapper
import com.qunar.spark.base.log.Logging
import com.qunar.spark.transmit.ExportPhaseType
import com.qunar.spark.transmit.Task.TaskBuilder
import com.qunar.spark.transmit.phase.{ExportPhase, ExportPhaseBuilder, PhaseConstants, TaskPhaseType}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Dataset
import org.joda.time.DateTime

import scala.collection.immutable.HashMap

/**
  * 针对内存数据([[org.apache.spark.rdd.RDD]]或[[org.apache.spark.sql.Dataset]])的数据导出服务
  */
final class MemoryExportPhase[T](private val data: RDD[T]) extends ExportPhase with Logging {

  def this(data: Dataset[T]) = this(data.rdd)

  private val hdfsBufferPath = "/data-transmit/mem-buffer/" + DateTime.now().toString("yyyyMMddHHmmss")

  // 对于内存数据,首先需要将数据写入hdfs以防止任务失败而丢失数据
  HdfsSparkService.writeEntityToHdfs(data, data => JsonMapper.writeValueAsString(data),
    hdfsBufferPath, HdfsFileType.LZO)

  // 内存数据导出本质上还是hdfs的数据导出
  override def phaseType: ExportPhaseType = TaskPhaseType.HDFS_EXPORT_PHASE

  override def genPhaseExecutionPlan: HashMap[String, String] = {
    val planBuilder = HashMap.newBuilder[String, String]
    planBuilder += (PhaseConstants.HDFS_PATH -> hdfsBufferPath)
    planBuilder.result
  }

  override def genExtraInfo: HashMap[String, String] = null

}

final class MemoryExportPhaseBuilder[T](private val hostTask: TaskBuilder) extends ExportPhaseBuilder(hostTask) with Logging {

  private var rdd: RDD[T] = _

  private var dataset: Dataset[T] = _

  def setData(rdd: RDD[T]): this.type = {
    this.rdd = rdd
    this
  }

  def setData(dataset: Dataset[T]): this.type = {
    this.dataset = dataset
    this
  }

  protected[transmit] override def buildPhase: MemoryExportPhase[T] = {
    if (dataset != null) {
      new MemoryExportPhase[T](dataset)
    } else if (rdd != null) {
      new MemoryExportPhase[T](rdd)
    } else {
      logError("none of rdd or dataset has been set either")
      throw new IllegalArgumentException("none of rdd or dataset has been set either")
    }
  }

}
