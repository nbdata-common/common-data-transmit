package com.qunar.spark.transmit.phase

import scala.language.implicitConversions

/**
  * 任务阶段的枚举类型
  */
object TaskPhaseType extends Enumeration {

  type TaskPhaseType = Value

  /**
    * 导出阶段类型
    */
  sealed class ExportPhaseType extends Val

  // 针对elasticsearch的导出阶段
  val ELASTIC_SEARCH_EXPORT_PHASE = new ExportPhaseType
  // 针对hdfs的导出阶段
  val HDFS_EXPORT_PHASE = new ExportPhaseType

  implicit def convertToExport(v: Value): ExportPhaseType = v.asInstanceOf[ExportPhaseType]

  /**
    * 导入阶段类型
    */
  sealed class ImportPhaseType extends Val

  // 针对elasticsearch的导入阶段
  val ELASTIC_SEARCH_IMPORT_PHASE = new ImportPhaseType
  // 针对hdfs的导入阶段
  val HDFS_IMPORT_PHASE = new ImportPhaseType

  implicit def convertToImport(v: Value): ImportPhaseType = v.asInstanceOf[ImportPhaseType]

}
