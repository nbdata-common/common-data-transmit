package com.qunar.spark.transmit.phase.elasticsearch

import org.elasticsearch.index.query.{AndFilterBuilder, FilterBuilders, QueryBuilders, RangeFilterBuilder}

/**
  * elasticsearch导出数据的DSL构建者
  */
sealed trait EsFetchConditionBuilder {

  /**
    * 生成elasticsearch的DSL语句
    */
  def genDSL: String

}

/**
  * elasticsearch按字段range作条件过滤取数
  */
final class EsRangeFetchBuilder extends EsFetchConditionBuilder {

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
