package com.qunar.spark.transmit.phase.elasticsearch

import com.google.common.base.Strings
import com.qunar.spark.transmit.ExportPhaseType
import com.qunar.spark.transmit.Task.TaskBuilder
import com.qunar.spark.transmit.phase._

import scala.collection.immutable.HashMap

/**
  * 针对elasticsearch的导出阶段的配置及任务执行计划生成
  */
final class ElasticSearchExportPhase(private val index: String,
                                     private val `type`: String,
                                     private val fetchDSL: String) extends ExportPhase {

  override def phaseType: ExportPhaseType = TaskPhaseType.ELASTIC_SEARCH_EXPORT_PHASE

  override def genPhaseExecutionPlan: HashMap[String, String] = {
    val planBuilder = HashMap.newBuilder[String, String]
    planBuilder.sizeHint(3)

    if (Strings.isNullOrEmpty(index)) {
      //todo logging
    }
    if (Strings.isNullOrEmpty(`type`)) {
      //todo logging
    }
    if (Strings.isNullOrEmpty(fetchDSL)) {
      //todo logging
    }

    planBuilder += (PhaseConstants.ELASTIC_SEARCH_INDEX -> index)
    planBuilder += (PhaseConstants.ELASTIC_SEARCH_TYPE -> `type`)
    planBuilder += (PhaseConstants.ELASTIC_SEARCH_DSL -> fetchDSL)

    planBuilder.result
  }

}

final class ElasticSearchExportPhaseBuilder(private val hostTask: TaskBuilder) extends ExportPhaseBuilder(hostTask) {

  private var index: String = _

  private var `type`: String = _

  private var fetchConditionBuilder: EsFetchConditionBuilder = _

  def setIndex(index: String): this.type = {
    this.index = index
    this
  }

  def setType(`type`: String): this.type = {
    this.`type` = `type`
    this
  }

  def rangeFetchBuilder: EsRangeFetchBuilder = {
    val fetchBuilder = new EsRangeFetchBuilder(this)
    fetchConditionBuilder = fetchBuilder
    fetchBuilder
  }

  override def buildPhase: ElasticSearchExportPhase = {
    new ElasticSearchExportPhase(index, `type`, fetchConditionBuilder.genDSL)
  }

}
