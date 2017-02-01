import com.qunar.spark.transmit.Task
import com.qunar.spark.transmit.phase.elasticsearch.LogicOperatorType
import org.elasticsearch.index.query._
import org.junit.Test

class ScalaTest {

  @Test
  def test(): Unit = {
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
  }

  @Test
  def test2(): Unit = {
    val rangeFilterBuilder: RangeFilterBuilder = FilterBuilders.rangeFilter("sss").gte(0L).lt(10000L)
    val boolFilterBuilder: BoolFilterBuilder = FilterBuilders.boolFilter()

    val andFilterBuilder: AndFilterBuilder = FilterBuilders.andFilter(rangeFilterBuilder)
    val sss = QueryBuilders.filteredQuery(null, andFilterBuilder).toString
    Console.println(sss)
  }

}
