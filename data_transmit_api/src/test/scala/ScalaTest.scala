import com.qunar.spark.transmit.Task
import org.junit.Test

class ScalaTest {

  @Test
  def test(): Unit = {
    val taskBuilder = Task.builder

    val task = taskBuilder
      .elasticsearchExportPhaseBuilder
      .setIndex("tc_other_order_transaction_idx")
      .setType("tcOtherOrderTransaction")

      .elasticsearchImportPhaseBuilder
      .buildTask

    task.transmitData()
  }

}
