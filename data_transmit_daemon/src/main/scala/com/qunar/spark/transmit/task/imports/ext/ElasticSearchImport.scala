package com.qunar.spark.transmit.task.imports.ext

import com.qunar.spark.transmit.base.PropertiesLoader
import com.qunar.spark.transmit.task.imports.DataImportTask
import org.apache.spark.rdd.RDD
import org.elasticsearch.spark._

import scala.collection.mutable

/**
  * 将数据导入ES
  */
class ElasticSearchImport extends DataImportTask {

  private val propertiesLoader = PropertiesLoader("elastic.import")

  private val defaultConfig = mutable.HashMap[String, String]()

  defaultConfig += ("es.nodes" -> propertiesLoader.getStr("es.nodes"))
  defaultConfig += ("es.port" -> propertiesLoader.getStr("es.port"))
  defaultConfig += ("es.nodes.wan.only" -> propertiesLoader.getStr("es.nodes.wan.only"))
  defaultConfig += ("es.nodes.discovery" -> propertiesLoader.getStr("es.nodes.discovery"))
  //  defaultConfig += ("es.scroll.size" -> propertiesLoader.getStr("es.scroll.size"))
  //  defaultConfig += ("es.net.http.auth.user" -> propertiesLoader.getStr("es.net.http.auth.user"))
  //  defaultConfig += ("es.net.http.auth.pass" -> propertiesLoader.getStr("es.net.http.auth.pass"))
  defaultConfig += ("es.output.json" -> propertiesLoader.getStr("es.output.json"))

  def writeDataToEs(rdd: RDD[String], index: String, `type`: String): Unit = {
    writeDataToEs(rdd, index, `type`, defaultConfig)
  }

  def writeDataToEs(rdd: RDD[String], index: String, `type`: String, cfg: scala.collection.Map[String, String]): Unit = {
    rdd.saveJsonToEs(index + "/" + `type`, cfg)
  }

}
