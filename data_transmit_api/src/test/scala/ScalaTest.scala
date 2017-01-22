import com.qunar.spark.transmit.Task
import org.junit.Test

class ScalaTest {

  @Test
  def test(): Unit = {
    val taskBuilder = Task.builder

    val task = taskBuilder.setExportPhase(null)
      .setImportPhase(null)
      .build

    task.transmitData()
  }

}
