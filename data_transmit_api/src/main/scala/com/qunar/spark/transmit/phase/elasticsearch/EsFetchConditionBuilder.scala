package com.qunar.spark.transmit.phase.elasticsearch

import com.qunar.spark.transmit.Task.TaskBuilder
import com.qunar.spark.transmit.phase.TaskPhaseBuilder
import org.elasticsearch.index.query._

import scala.language.implicitConversions

/**
  * elasticsearch导出数据的DSL构建者
  */
abstract sealed class EsFetchConditionBuilder private[transmit](private val hostPhaseBuilder: TaskPhaseBuilder) {

  protected[transmit] def backToHost = hostPhaseBuilder

  /**
    * 生成elasticsearch的DSL语句
    */
  protected[transmit] def genDSLInternal: FilterBuilder

}

object EsFetchConditionBuilder {

  /**
    * 从[[EsFetchConditionBuilder]]到[[TaskPhaseBuilder]]的隐式转换
    */
  implicit def backToTaskPhaseBuilder(esFetchConditionBuilder: EsFetchConditionBuilder): TaskPhaseBuilder = {
    esFetchConditionBuilder.backToHost
  }

  /**
    * 从[[EsFetchConditionBuilder]]到[[TaskBuilder]]的隐式转换
    */
  implicit def backToTaskBuilder(esFetchConditionBuilder: EsFetchConditionBuilder): TaskBuilder = {
    esFetchConditionBuilder.backToHost.backToHost
  }

}

/**
  * elasticsearch按字段范围作条件过滤取数
  */
final class EsRangeFetchBuilder[T <: AnyVal] private[transmit](private val hostPhaseBuilder: TaskPhaseBuilder
                                                              ) extends EsFetchConditionBuilder(hostPhaseBuilder) {

  private var rangeFieldName: String = _

  private var beginValue: T = _

  private var endValue: T = _

  def setRangeFieldName(fieldName: String): this.type = {
    rangeFieldName = fieldName
    this
  }

  def setStartValue(start: T): this.type = {
    beginValue = start
    this
  }

  def setEndValue(end: T): this.type = {
    endValue = end
    this
  }

  protected[transmit] override def genDSLInternal: FilterBuilder = {
    FilterBuilders.rangeFilter(rangeFieldName).gte(beginValue).lt(endValue)
  }

}

/**
  * elasticsearch自定义的取数条件构造
  */
final class EsCustomFetchBuilder private[transmit](private val hostPhaseBuilder: TaskPhaseBuilder
                                                  ) extends EsFetchConditionBuilder(hostPhaseBuilder) {

  private var queryDSL: String = _

  def setQueryDSL(queryDSL: String): this.type = {
    this.queryDSL = queryDSL
    this
  }

  protected[transmit] override def genDSLInternal: FilterBuilder = {

    /**
      * 这里使用继承[[BoolFilterBuilder]]并重写[[toString]]方法的方式
      * 携带上[[queryDSL]],同时兼容了父类方法,便于父类公共方法回调子类
      * 逻辑.
      */
    new BoolFilterBuilder {
      override def toString = queryDSL
    }

  }

}
