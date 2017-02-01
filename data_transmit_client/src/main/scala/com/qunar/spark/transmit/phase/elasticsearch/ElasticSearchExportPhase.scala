package com.qunar.spark.transmit.phase.elasticsearch

import com.google.common.base.{Preconditions, Strings}
import com.qunar.spark.base.log.Logging
import com.qunar.spark.transmit.ExportPhaseType
import com.qunar.spark.transmit.Task.TaskBuilder
import com.qunar.spark.transmit.phase._
import com.qunar.spark.transmit.phase.elasticsearch.LogicOperatorType.LogicOperatorType
import org.elasticsearch.index.query.{FilterBuilders, QueryBuilders}

import scala.collection.immutable.HashMap

/**
  * 针对elasticsearch的导出阶段的配置及任务执行计划生成
  */
final class ElasticSearchExportPhase(private val index: String,
                                     private val `type`: String,
                                     private val fetchDSL: String) extends ExportPhase with Logging {

  override def phaseType: ExportPhaseType = TaskPhaseType.ELASTIC_SEARCH_EXPORT_PHASE

  override def genPhaseExecutionPlan: HashMap[String, String] = {
    val planBuilder = HashMap.newBuilder[String, String]
    planBuilder.sizeHint(3)

    if (Strings.isNullOrEmpty(index)) {
      logError("ElasticSearchExportPhase genPhaseExecutionPlan: index is empty")
    }
    if (Strings.isNullOrEmpty(`type`)) {
      logError("ElasticSearchExportPhase genPhaseExecutionPlan: type is empty")
    }
    if (Strings.isNullOrEmpty(fetchDSL)) {
      logError("ElasticSearchExportPhase genPhaseExecutionPlan: fetchDSL is empty")
    }

    planBuilder += (PhaseConstants.ELASTIC_SEARCH_INDEX -> index)
    planBuilder += (PhaseConstants.ELASTIC_SEARCH_TYPE -> `type`)
    planBuilder += (PhaseConstants.ELASTIC_SEARCH_DSL -> fetchDSL)

    planBuilder.result
  }

}

final class ElasticSearchExportPhaseBuilder(private val hostTask: TaskBuilder) extends ExportPhaseBuilder(hostTask) with Logging {

  private var index: String = _

  private var `type`: String = _

  /* 以下两者,用于构建elasticsearch的取数DSL */

  /**
    * NOTICE: 这里使用[[org.elasticsearch.index.query.BoolFilterBuilder]]作为
    * 用户自定义es取数逻辑的总控,是为了采纳elasticsearch在2.0版本之后提倡尽量使
    * 用BoolFilterBuilder的建议,同时这样做也确实能够更简洁地管理各种用户自定义
    * 取数过滤逻辑.
    */
  private val boolFilterBuilder = FilterBuilders.boolFilter()

  // 具体的filter与其在boolFilterBuilder中的对应逻辑操作的映射
  private val filterOperatorMap = HashMap.newBuilder[LogicOperatorType, EsFetchConditionBuilder]

  //容量预加载
  filterOperatorMap.sizeHint(5)

  /**
    * 生成elasticsearch的查询DSL
    */
  private def genDSL: String = {
    val filterMap = filterOperatorMap.result
    filterMap.foreach(tuple => {
      tuple._1 match {
        case LogicOperatorType.MUST => boolFilterBuilder.must(tuple._2.genDSLInternal)
        case LogicOperatorType.SHOULD => boolFilterBuilder.should(tuple._2.genDSLInternal)
        case LogicOperatorType.MUST_NOT => boolFilterBuilder.mustNot(tuple._2.genDSLInternal)
        // 对于用户自定义逻辑特殊处理:短路掉其他的逻辑条件
        case LogicOperatorType.CUSTOM => return tuple._2.genDSLInternal.toString
      }
    })
    QueryBuilders.filteredQuery(null, boolFilterBuilder).toString
  }

  /* 以下为ElasticSearchExportPhaseBuilder的对外api */

  def setIndex(index: String): this.type = {
    this.index = index
    this
  }

  def setType(`type`: String): this.type = {
    this.`type` = `type`
    this
  }

  /**
    * 返回一个依据某个字段值范围[[org.elasticsearch.index.query.RangeFilterBuilder]]的取数逻辑构造者
    *
    * @param logicGateType 表征此RangeFilterBuilder属于那种Boolean DSL的逻辑运算关系
    * @tparam T 目标字段值的类型
    */
  def rangeFetchBuilder[T <: AnyVal](logicGateType: LogicOperatorType): EsRangeFetchBuilder[T] = {
    Preconditions.checkNotNull(logicGateType)

    val fetchBuilder = new EsRangeFetchBuilder[T](this)
    logicGateType match {
      case LogicOperatorType.MUST => filterOperatorMap += (LogicOperatorType.MUST -> fetchBuilder)
      case LogicOperatorType.SHOULD => filterOperatorMap += (LogicOperatorType.SHOULD -> fetchBuilder)
      case LogicOperatorType.MUST_NOT => filterOperatorMap += (LogicOperatorType.MUST_NOT -> fetchBuilder)
      case LogicOperatorType.CUSTOM =>
        logError("LogicOperatorType CUSTOM does not confirm to method rangeFetchBuilder")
        throw new IllegalArgumentException("LogicOperatorType CUSTOM does not confirm to method rangeFetchBuilder")
    }

    fetchBuilder
  }

  /**
    * [[org.elasticsearch.index.query.TermFilterBuilder]]的取数逻辑构造者
    */
  def termFetchBuilder[T <: AnyVal](logicGateType: LogicOperatorType): EsTermFetchBuilder[T] = {
    Preconditions.checkNotNull(logicGateType)

    val fetchBuilder = new EsTermFetchBuilder[T](this)
    logicGateType match {
      case LogicOperatorType.MUST => filterOperatorMap += (LogicOperatorType.MUST -> fetchBuilder)
      case LogicOperatorType.SHOULD => filterOperatorMap += (LogicOperatorType.SHOULD -> fetchBuilder)
      case LogicOperatorType.MUST_NOT => filterOperatorMap += (LogicOperatorType.MUST_NOT -> fetchBuilder)
      case LogicOperatorType.CUSTOM =>
        logError("LogicOperatorType CUSTOM does not confirm to method termFetchBuilder")
        throw new IllegalArgumentException("LogicOperatorType CUSTOM does not confirm to method termFetchBuilder")
    }

    fetchBuilder
  }

  /**
    * [[org.elasticsearch.index.query.ExistsFilterBuilder]]的取数逻辑构造者
    */
  def existsFetchBuilder(logicGateType: LogicOperatorType): EsExistsFetchBuilder = {
    Preconditions.checkNotNull(logicGateType)

    val fetchBuilder = new EsExistsFetchBuilder(this)
    logicGateType match {
      case LogicOperatorType.MUST => filterOperatorMap += (LogicOperatorType.MUST -> fetchBuilder)
      case LogicOperatorType.SHOULD => filterOperatorMap += (LogicOperatorType.SHOULD -> fetchBuilder)
      case LogicOperatorType.MUST_NOT => filterOperatorMap += (LogicOperatorType.MUST_NOT -> fetchBuilder)
      case LogicOperatorType.CUSTOM =>
        logError("LogicOperatorType CUSTOM does not confirm to method existsFetchBuilder")
        throw new IllegalArgumentException("LogicOperatorType CUSTOM does not confirm to method termFetchBuilder")
    }

    fetchBuilder
  }

  /**
    * 返回一个用户自定义的取数逻辑构造者
    * NOTICE: 此方法构造的Query DSL将被不加其余处理地作为最终的查询语句
    */
  def customFetchBuilder: EsCustomFetchBuilder = {
    val fetchBuilder = new EsCustomFetchBuilder(this)
    filterOperatorMap += (LogicOperatorType.CUSTOM -> fetchBuilder)

    fetchBuilder
  }

  protected[transmit] override def buildPhase: ElasticSearchExportPhase = {
    new ElasticSearchExportPhase(index, `type`, genDSL)
  }

}

/**
  * [[org.elasticsearch.index.query.BoolFilterBuilder]]支持的几种逻辑运算关系
  */
object LogicOperatorType extends Enumeration {

  type LogicOperatorType = Value

  // 三种基本的BoolFilterBuilder逻辑运算关系
  val MUST, SHOULD, MUST_NOT = Value

  // 跳过常规逻辑的用户自定义类型
  val CUSTOM = Value

}
