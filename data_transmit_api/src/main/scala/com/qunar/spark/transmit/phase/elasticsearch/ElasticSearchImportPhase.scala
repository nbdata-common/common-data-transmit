package com.qunar.spark.transmit.phase.elasticsearch

import com.google.common.base.Strings
import com.qunar.spark.transmit.ImportPhaseType
import com.qunar.spark.transmit.Task.TaskBuilder
import com.qunar.spark.transmit.phase.{ImportPhase, ImportPhaseBuilder, PhaseConstants, TaskPhaseType}

import scala.collection.immutable.HashMap

/**
  * 针对elasticsearch的导入阶段的配置及任务执行计划生成
  */
final class ElasticSearchImportPhase(private val index: String,
                                     private val `type`: String) extends ImportPhase {

  override def phaseType: ImportPhaseType = TaskPhaseType.ELASTIC_SEARCH_IMPORT_PHASE

  override def genPhaseExecutionPlan: HashMap[String, String] = {
    val planBuilder = HashMap.newBuilder[String, String]
    planBuilder.sizeHint(10)

    if (Strings.isNullOrEmpty(index)) {
      //todo logging
    }
    if (Strings.isNullOrEmpty(`type`)) {
      //todo logging
    }

    planBuilder += (PhaseConstants.ELASTIC_SEARCH_INDEX -> index)
    planBuilder += (PhaseConstants.ELASTIC_SEARCH_TYPE -> `type`)

    planBuilder.result
  }

}

final class ElasticSearchImportPhaseBuilder(private val hostTask: TaskBuilder) extends ImportPhaseBuilder(hostTask) {

  override def build: ElasticSearchImportPhase = {
    null
  }

}
