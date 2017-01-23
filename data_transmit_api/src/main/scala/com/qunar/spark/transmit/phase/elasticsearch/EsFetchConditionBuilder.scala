package com.qunar.spark.transmit.phase.elasticsearch

import com.qunar.spark.transmit.phase.TaskPhaseBuilder
import org.elasticsearch.index.query.{AndFilterBuilder, FilterBuilders, QueryBuilders, RangeFilterBuilder}

import scala.language.implicitConversions

/**
  * elasticsearch导出数据的DSL构建者
  */
abstract sealed class EsFetchConditionBuilder private[transmit](private val hostPhaseBuilder: TaskPhaseBuilder) {

  def backToHost = hostPhaseBuilder

  /**
    * 生成elasticsearch的DSL语句
    */
  def genDSL: String

}

object EsFetchConditionBuilder {

  def rangeFetchBuilder(hostPhaseBuilder: TaskPhaseBuilder) = new EsRangeFetchBuilder(hostPhaseBuilder)

  implicit def backToTaskPhaseBuilder(esFetchConditionBuilder: EsFetchConditionBuilder): TaskPhaseBuilder = {
    esFetchConditionBuilder.backToHost
  }

}

/**
  * elasticsearch按字段range作条件过滤取数
  */
final class EsRangeFetchBuilder private[transmit](private val hostPhaseBuilder: TaskPhaseBuilder
                                                 ) extends EsFetchConditionBuilder(hostPhaseBuilder) {

  private var rangeFieldName: String = _

  private var beginValue: AnyVal = _

  private var endValue: AnyVal = _

  def setRangeFieldName(fieldName: String) = {
    rangeFieldName = fieldName
  }

  def setStartTime(start: AnyVal) = {
    beginValue = start
  }

  def setEndTime(end: AnyVal) = {
    endValue = end
  }

  override def genDSL: String = {
    val rangeFilterBuilder: RangeFilterBuilder = FilterBuilders.rangeFilter(rangeFieldName).gte(beginValue).lt(endValue)
    val andFilterBuilder: AndFilterBuilder = FilterBuilders.andFilter(rangeFilterBuilder)
    QueryBuilders.filteredQuery(null, andFilterBuilder).toString
  }

}
