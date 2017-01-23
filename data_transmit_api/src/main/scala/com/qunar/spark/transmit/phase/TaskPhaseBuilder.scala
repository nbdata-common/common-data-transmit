package com.qunar.spark.transmit.phase

import com.qunar.spark.transmit.Task.TaskBuilder
import com.qunar.spark.transmit.phase.elasticsearch.{ElasticSearchExportPhaseBuilder, ElasticSearchImportPhaseBuilder}

/**
  * [[TaskPhase]]的建造者
  */
abstract sealed class TaskPhaseBuilder private[transmit](private val hostTask: TaskBuilder) {

  def build: TaskPhase

}

abstract class ExportPhaseBuilder private[transmit](private val hostTask: TaskBuilder) extends TaskPhaseBuilder(hostTask) {

  override def build: ExportPhase

}

abstract class ImportPhaseBuilder private[transmit](private val hostTask: TaskBuilder) extends TaskPhaseBuilder(hostTask) {

  override def build: ImportPhase

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

}
