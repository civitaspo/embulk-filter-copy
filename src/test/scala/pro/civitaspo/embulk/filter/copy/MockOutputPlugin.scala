package pro.civitaspo.embulk.filter.copy

import java.util

import org.embulk.config.{
  Config,
  ConfigDiff,
  ConfigSource,
  Task,
  TaskReport,
  TaskSource
}
import org.embulk.spi.{
  Exec,
  OutputPlugin,
  Page,
  Schema,
  TransactionalPageOutput
}
import org.embulk.spi.TestPageBuilderReader.MockPageOutput

import scala.collection.concurrent.TrieMap

object MockOutputPlugin {
  private val map = TrieMap.empty[String, MockPageOutput]

  def getOrInitialize(name: String): MockPageOutput =
    map.getOrElseUpdate(name, new MockPageOutput())
  def cleanup(): Unit = map.clear()
}

class MockOutputPlugin extends OutputPlugin {
  trait PluginTask extends Task {
    @Config("name")
    def getName: String
  }

  override def transaction(
      config: ConfigSource,
      schema: Schema,
      taskCount: Int,
      control: OutputPlugin.Control
  ): ConfigDiff = {
    control.run(config.loadConfig(classOf[PluginTask]).dump())
    Exec.newConfigDiff()
  }

  override def resume(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      control: OutputPlugin.Control
  ): ConfigDiff = throw new UnsupportedOperationException

  override def cleanup(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      successTaskReports: util.List[TaskReport]
  ): Unit = {}

  override def open(
      taskSource: TaskSource,
      schema: Schema,
      taskIndex: Int
  ): TransactionalPageOutput = {
    val task = taskSource.loadTask(classOf[PluginTask])
    new TransactionalPageOutput {
      val output: MockPageOutput =
        MockOutputPlugin.getOrInitialize(task.getName)
      override def add(page: Page): Unit = output.add(page)
      override def finish(): Unit = output.finish()
      override def close(): Unit = output.close()
      override def abort(): Unit = {}
      override def commit(): TaskReport = Exec.newTaskReport()
    }
  }
}
