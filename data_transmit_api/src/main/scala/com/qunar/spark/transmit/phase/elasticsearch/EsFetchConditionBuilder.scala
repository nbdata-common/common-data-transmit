package com.qunar.spark.transmit.phase.elasticsearch

import com.qunar.spark.transmit.Task.TaskBuilder
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

  /**
    * 从[[EsFetchConditionBuilder]]到[[TaskPhaseBuilder]]的隐式转换
    */
  implicit def backToTaskPhaseBuilder(esFetchConditionBuilder: EsFetchConditionBuilder): TaskPhaseBuilder = {
    esFetchConditionBuilder.backToHost
  }

  /**
    * 从[[EsFetchConditionBuilder]]到[[TaskBuilder]]的隐式转换
    *
    */
  implicit def backToTaskBuilder(esFetchConditionBuilder: EsFetchConditionBuilder): TaskBuilder = {
    esFetchConditionBuilder.backToHost.backToHost
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

  def setRangeFieldName(fieldName: String): this.type = {
    rangeFieldName = fieldName
    this
  }

  def setStartTime(start: AnyVal): this.type = {
    beginValue = start
    this
  }

  def setEndTime(end: AnyVal): this.type = {
    endValue = end
    this
  }

  override def genDSL: String = {
    val rangeFilterBuilder: RangeFilterBuilder = FilterBuilders.rangeFilter(rangeFieldName).gte(beginValue).lt(endValue)
    val andFilterBuilder: AndFilterBuilder = FilterBuilders.andFilter(rangeFilterBuilder)
    QueryBuilders.filteredQuery(null, andFilterBuilder).toString
  }

}
