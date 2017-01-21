package com.qunar.spark.transmit.task.exports.ext

import com.qunar.spark.transmit.base.{PropertiesLoader, SparkSessions}
import com.qunar.spark.transmit.task.exports.DataExportTask
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._

import scala.collection.mutable

/**
  * 将数据从ES中导出
  */
class ElasticSearchExport extends DataExportTask {

  private val session = SparkSessions.getSparkSession

  private val propertiesLoader = PropertiesLoader("elastic.export")

  private val defaultConfig = mutable.HashMap[String, String]()

  defaultConfig += ("es.nodes" -> propertiesLoader.getStr("es.nodes"))
  defaultConfig += ("es.nodes.wan.only" -> propertiesLoader.getStr("es.nodes.wan.only"))
  defaultConfig += ("es.nodes.discovery" -> propertiesLoader.getStr("es.nodes.discovery"))
  defaultConfig += ("es.scroll.size" -> propertiesLoader.getStr("es.scroll.size"))
  defaultConfig += ("es.net.http.auth.user" -> propertiesLoader.getStr("es.net.http.auth.user"))
  defaultConfig += ("es.net.http.auth.pass" -> propertiesLoader.getStr("es.net.http.auth.pass"))
  defaultConfig += ("es.output.json" -> propertiesLoader.getStr("es.output.json"))

  /**
    * 默认按照时间戳scan es中的数据
    *
    * @param index    索引名
    * @param `type`   表名
    * @param queryStr 查询DSL语句
    */
  def readDataFromEs(index: String, `type`: String, queryStr: String): RDD[String] = {
    readDataFromEs(index, `type`, queryStr, defaultConfig)
  }

  /**
    * @param cfg 配置元数据信息
    */
  def readDataFromEs(index: String, `type`: String, queryStr: String, cfg: scala.collection.Map[String, String]) = {
    session.sparkContext.esJsonRDD(index + "/" + `type`, queryStr, cfg).map(record => record._2)
  }

}
