package pro.civitaspo.embulk.filter.copy.plugin

import java.util

import org.embulk.config.{
  ConfigDiff,
  ConfigSource,
  Task,
  TaskReport,
  TaskSource
}
import org.embulk.spi.{Exec, InputPlugin, Page, PageOutput, Schema}

case class PipeInputPlugin(
    schema: Schema,
    taskCount: Int,
    pageProducer: (Page => Unit) => Unit
) extends InputPlugin {

  trait PluginTask extends Task

  override def transaction(
      config: ConfigSource,
      control: InputPlugin.Control
  ): ConfigDiff = {
    val task = config.loadConfig(classOf[PluginTask])
    control.run(task.dump(), schema, taskCount)
    Exec.newConfigDiff()
  }

  override def resume(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      control: InputPlugin.Control
  ): ConfigDiff = throw new UnsupportedOperationException

  override def cleanup(
      taskSource: TaskSource,
      schema: Schema,
      taskCount: Int,
      successTaskReports: util.List[TaskReport]
  ): Unit = {}

  override def run(
      taskSource: TaskSource,
      schema: Schema,
      taskIndex: Int,
      output: PageOutput
  ): TaskReport = {
    try pageProducer(output.add)
    finally output.finish()
    Exec.newTaskReport()
  }

  override def guess(config: ConfigSource): ConfigDiff =
    throw new UnsupportedOperationException
}
