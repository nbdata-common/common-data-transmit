package com.qunar.spark.transmit.service.elasticsearch

import org.elasticsearch.index.query.{AndFilterBuilder, FilterBuilders, QueryBuilders, RangeFilterBuilder}

/**
  * 针对ElasticSearch的取数逻辑建造者
  */
class SearchBuilder(private var rangeFieldName: String,
                    private var startTime: Long,
                    private var endTime: Long) {

  def this() = this(null, 0L, 0L)

  def this(rangeFieldName: String) = this(rangeFieldName, 0, System.currentTimeMillis)

  def setRangeFieldName(fieldName: String) = {
    rangeFieldName = fieldName
  }

  def setStartTime(start: Long) = {
    startTime = start
  }

  def setEndTime(end: Long) = {
    endTime = end
  }

  def build: String = {
    val rangeFilterBuilder: RangeFilterBuilder = FilterBuilders.rangeFilter(rangeFieldName).gte(startTime).lt(endTime)
    val andFilterBuilder: AndFilterBuilder = FilterBuilders.andFilter(rangeFilterBuilder)
    QueryBuilders.filteredQuery(null, andFilterBuilder).toString
  }

}

object SearchBuilder {

  def apply(rangeFieldName: String, startTime: Long, endTime: Long): SearchBuilder = new SearchBuilder(rangeFieldName, startTime, endTime)

}
