import com.qunar.spark.transmit.Task
import com.qunar.spark.transmit.phase.elasticsearch.LogicOperatorType
import org.elasticsearch.index.query._
import org.junit.Test

class ScalaTest {

  @Test
  def test(): Unit = {
    val taskBuilder = Task.builder

    val task = taskBuilder
      .elasticsearchExportPhaseBuilder
      .setIndex("tc_other_order_transaction_idx")
      .setType("tcOtherOrderTransaction")
      .customFetchBuilder.setQueryDSL("")
      //      .rangeFetchBuilder[Long](LogicOperatorType.MUST).setStartValue(0L).setEndValue(100000L)

      .elasticsearchImportPhaseBuilder
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
