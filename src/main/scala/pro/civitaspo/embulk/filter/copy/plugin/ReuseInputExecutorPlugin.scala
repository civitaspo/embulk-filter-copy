package pro.civitaspo.embulk.filter.copy.plugin

import java.util.{List => JList}
import java.util.concurrent.Future

import org.embulk.config.{Config, ConfigSource, Task, TaskReport}
import org.embulk.exec.LocalExecutorPlugin.DirectExecutor
import org.embulk.spi.{
  Exec,
  ExecSession,
  ExecutorPlugin,
  FilterPlugin,
  InputPlugin,
  OutputPlugin,
  ProcessState,
  ProcessTask,
  Schema
}
import org.embulk.spi.util.{Executors, Filters}
import org.embulk.spi.util.Executors.ProcessStateCallback
import org.slf4j.{Logger, LoggerFactory}
import pro.civitaspo.embulk.filter.copy.ThreadNameContext

import scala.util.Using

case class ReuseInputExecutorPlugin(inputPlugin: InputPlugin)
    extends ExecutorPlugin {

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[ReuseInputExecutorPlugin])

  trait PluginTask extends Task {
    @Config("max_threads")
    def getMaxThreads: Int
  }

  override def transaction(
      config: ConfigSource,
      outputSchema: Schema,
      inputTaskCount: Int,
      control: ExecutorPlugin.Control
  ): Unit = {
    val task = config.loadConfig(classOf[PluginTask])
    val maxThreads: Int = task.getMaxThreads
    logger.info(
      s"Using local thread executor with max_threads=$maxThreads / tasks=$inputTaskCount"
    )
    Using.resource(ReuseInputDirectExecutor(maxThreads, inputTaskCount)) {
      exec => control.transaction(outputSchema, exec.getOutputTaskCount, exec)
    }
  }

  case class ReuseInputDirectExecutor(maxThreads: Int, taskCount: Int)
      extends DirectExecutor(maxThreads, taskCount) {

    override def startInputTask(
        task: ProcessTask,
        state: ProcessState,
        taskIndex: Int
    ): Future[Throwable] = {
      if (state.getOutputTaskState(taskIndex).isCommitted) {
        logger.warn("Skipped resumed task {}", taskIndex)
        return null // resumed
      }

      executor.submit(() => {
        try {
          ThreadNameContext.switch(String.format("task-%04d", taskIndex)) {
            _ =>
              val callback = new ProcessStateCallback {
                override def started(): Unit = {
                  state.getInputTaskState(taskIndex).start()
                  state.getOutputTaskState(taskIndex).start()
                }

                override def inputCommitted(report: TaskReport): Unit =
                  state.getInputTaskState(taskIndex).setTaskReport(report)

                override def outputCommitted(report: TaskReport): Unit =
                  state.getOutputTaskState(taskIndex).setTaskReport(report)
              }
              process(Exec.session(), task, taskIndex, callback)
          }
          null
        }
        finally {
          state.getInputTaskState(taskIndex).finish()
          state.getOutputTaskState(taskIndex).finish()
        }
      })
    }

    private def process(
        execSession: ExecSession,
        task: ProcessTask,
        taskIndex: Int,
        callback: ProcessStateCallback
    ): Unit = {
      val filterPlugins: JList[FilterPlugin] =
        Filters.newFilterPlugins(execSession, task.getFilterPluginTypes)
      val outputPlugin: OutputPlugin =
        execSession.newPlugin(classOf[OutputPlugin], task.getOutputPluginType)
      Executors.process(
        execSession,
        taskIndex,
        inputPlugin,
        task.getInputSchema,
        task.getInputTaskSource,
        filterPlugins,
        task.getFilterSchemas,
        task.getFilterTaskSources,
        outputPlugin,
        task.getOutputSchema,
        task.getOutputTaskSource,
        callback
      )
    }
  }
}
