package com.qunar.spark.transmit.phase

import com.qunar.spark.transmit.Task.TaskBuilder
import com.qunar.spark.transmit.phase.elasticsearch.{ElasticSearchExportPhaseBuilder, ElasticSearchImportPhaseBuilder}
import com.qunar.spark.transmit.phase.hdfs.{HdfsExportPhaseBuilder, HdfsImportPhaseBuilder}

import scala.language.implicitConversions

/**
  * [[TaskPhase]]的建造者
  */
abstract sealed class TaskPhaseBuilder private[transmit](private val hostTask: TaskBuilder) {

  /**
    * 配合隐式转换[[TaskPhaseBuilder.backToTaskBuilder]],以确保本[[TaskPhaseBuilder]]能回到自己的宿主[[TaskBuilder]]
    */
  protected[transmit] def backToHost = hostTask

  protected[transmit] def buildPhase: TaskPhase

}

abstract class ExportPhaseBuilder private[transmit](private val hostTask: TaskBuilder) extends TaskPhaseBuilder(hostTask) {

  override def buildPhase: ExportPhase

}

abstract class ImportPhaseBuilder private[transmit](private val hostTask: TaskBuilder) extends TaskPhaseBuilder(hostTask) {

  override def buildPhase: ImportPhase

}

object TaskPhaseBuilder {

  /**
    * 构造一个针对elasticsearch的[[ExportPhaseBuilder]]
    */
  def elasticSearchExportPhaseBuilder(taskBuilder: TaskBuilder) = new ElasticSearchExportPhaseBuilder(taskBuilder)

  /**
    * 构造一个针对elasticsearch的[[ImportPhaseBuilder]]
    */
  def elasticSearchImportPhaseBuilder(taskBuilder: TaskBuilder) = new ElasticSearchImportPhaseBuilder(taskBuilder)

  /**
    * 构造一个针对hdfs的[[ExportPhaseBuilder]]
    */
  def hdfsExportPhaseBuilder(taskBuilder: TaskBuilder) = new HdfsExportPhaseBuilder(taskBuilder)

  /**
    * 构造一个针对hdfs的[[ImportPhaseBuilder]]
    */
  def hdfsImportPhaseBuilder(taskBuilder: TaskBuilder) = new HdfsImportPhaseBuilder(taskBuilder)

  /**
    * 从[[TaskPhaseBuilder]]到[[TaskBuilder]]的隐式转换
    * 用于方便开发者使用[[TaskBuilder]]构建定制化的数据传输任务
    */
  implicit def backToTaskBuilder(taskPhaseBuilder: TaskPhaseBuilder): TaskBuilder = {
    taskPhaseBuilder.backToHost
  }

}
