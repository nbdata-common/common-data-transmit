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
      .rangeFetchBuilder[Long](LogicOperatorType.MUST).setStartValue(0L).setEndValue(100000L)
      // import phase
      .elasticsearchImportPhaseBuilder
      .setIndex("")
      .setType("")
      // build
      .buildTask

    task.transmitData()
```
--------------------------------------------------------------------------------
#### **data-transmit-daemon**:
more later...