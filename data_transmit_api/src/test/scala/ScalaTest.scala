import com.qunar.spark.transmit.Task
import com.qunar.spark.transmit.phase.TaskPhaseType
import org.junit.Test

class ScalaTest {

  @Test
  def test(): Unit = {
    val taskBuilder = Task.builder

    val task = taskBuilder
      .exportPhaseBuilder(TaskPhaseType.ELASTIC_SEARCH_EXPORT_PHASE)
      .importPhaseBuilder(TaskPhaseType.HDFS_IMPORT_PHASE)
      .buildTask

    task.transmitData()
  }

}
