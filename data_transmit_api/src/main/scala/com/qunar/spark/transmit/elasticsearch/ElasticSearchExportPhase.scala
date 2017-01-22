package com.qunar.spark.transmit.elasticsearch

import com.qunar.spark.transmit.phase.ExportPhase

import scala.collection.immutable.HashMap

/**
  * 针对elasticsearch的导出阶段的配置及任务执行计划生成
  */
final class ElasticSearchExportPhase(private val index: String,
                               private val `type`: String,
                               rangeFieldName: String,
                               startTime: Long,
                               endTime: Long) extends ExportPhase {


  override def genPhaseExecutionPlan: HashMap[String, String] = {
    val planBuilder = HashMap.newBuilder[String, String]
    planBuilder.sizeHint(10)

    planBuilder.result
  }

}

object ElasticsearchExportPhase {


}
