#### **common-data-transmit**
--------------------------------------------------------------------------------
#### **data-transmit-client**：
使用case如下:
```scala
    val taskBuilder = Task.builder

    val task = taskBuilder
      .setTaskName("tc_order_transaction")
      // export phase
      .elasticsearchExportPhaseBuilder
      .setIndex("tc_order_transaction_idx")
      .setType("tcOrderTransaction")
      .rangeFetchBuilder[Long](LogicOperatorType.MUST)
      .setRangeFieldName("updateTime").setStartValue(0L).setEndValue(System.currentTimeMillis)
      // import phase
      .hdfsImportPhaseBuilder.setPath("/esdata/lzo/tc_order_idx/")
      // build
      .buildTask

    task.transmitData()
```
--------------------------------------------------------------------------------
#### **data-transmit-daemon**:
more later...