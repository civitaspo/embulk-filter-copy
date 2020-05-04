package pro.civitaspo.embulk.filter.copy

import java.util.{Optional, List => JList}

import org.embulk.config.{
  Config,
  ConfigDefault,
  ConfigDiff,
  ConfigException,
  ConfigSource,
  TaskSource,
  Task => EmbulkTask
}
import org.embulk.exec.TransactionStage.{
  EXECUTOR_BEGIN,
  EXECUTOR_COMMIT,
  FILTER_BEGIN,
  FILTER_COMMIT,
  INPUT_BEGIN,
  INPUT_COMMIT,
  OUTPUT_BEGIN,
  OUTPUT_COMMIT,
  RUN
}
import org.embulk.plugin.PluginType
import org.embulk.spi.{
  Exec,
  ExecutorPlugin,
  FileOutputRunner,
  FilterPlugin,
  InputPlugin,
  OutputPlugin,
  Page,
  ProcessState,
  ProcessTask,
  Schema
}
import org.embulk.spi.util.Filters
import org.slf4j.{Logger, LoggerFactory}
import pro.civitaspo.embulk.filter.copy.plugin.{
  ReuseInputExecutorPlugin,
  PipeInputPlugin
}

import scala.util.chaining._

object BreakinBulkLoader {
  trait Task extends EmbulkTask {
    @Config("name")
    @ConfigDefault("null")
    def getName: Optional[String]

    @Config("exec")
    @ConfigDefault("{}")
    def getExec: ConfigSource

    @Config("filters")
    @ConfigDefault("[]")
    def getFilters: JList[ConfigSource]

    @Config("out")
    def getOut: ConfigSource

    // NOTE: When embulk is run as a server or using an union plugin inside
    //       another union plugin, the bulk loads that have the same
    //       loaderName cannot run twice or more because LoaderState is shared.
    //       So, the transaction id is used to distinguish the bulk loads.
    def setTransactionId(execId: String): Unit
    def getTransactionId: String
  }

  case class Result(
      configDiff: ConfigDiff,
      ignoredExceptions: Seq[Throwable]
  )
}

case class BreakinBulkLoader(task: BreakinBulkLoader.Task, idx: Int) {
  import implicits._

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[BreakinBulkLoader])

  private lazy val state: LoaderState = LoaderState.getOrInitialize(loaderName)
  private lazy val loaderName: String =
    s"transaction[${task.getTransactionId}]:copy[$idx]:" + task.getName
      .getOrElse {
        s"filters[${filterPluginTypes.map(_.getName).mkString(",")}]" +
          s".out[${outputPluginType.getName}]"
      }

  private lazy val inputTask: ConfigSource =
    Exec.newConfigSource().set("name", loaderName)
  private lazy val inputTaskCount: Int =
    executorTask.get(classOf[Int], "max_threads")

  private lazy val executorTask: ConfigSource = task.getExec.tap(c =>
    c.set(
      "max_threads",
      c.get(classOf[Int], "max_threads", Utils.getNumberOfCores * 2)
    )
  )

  private lazy val filterTasks: Seq[ConfigSource] = task.getFilters
  private lazy val filterPluginTypes: Seq[PluginType] =
    Filters.getPluginTypes(filterTasks)
  private lazy val filterPlugins: Seq[FilterPlugin] =
    Filters.newFilterPlugins(Exec.session(), filterPluginTypes)

  private lazy val outputTask: ConfigSource = task.getOut
  private lazy val outputPluginType: PluginType =
    outputTask.get(classOf[PluginType], "type")
  private lazy val outputPlugin: OutputPlugin =
    Exec.newPlugin(classOf[OutputPlugin], outputPluginType)

  def run(schema: Schema): Unit = {
    ThreadNameContext.switch(loaderName) { _ =>
      val inputPlugin: InputPlugin =
        PipeInputPlugin(schema, inputTaskCount, state.consumePages)
      val executorPlugin: ExecutorPlugin = ReuseInputExecutorPlugin(inputPlugin)
      try {
        runInput(inputPlugin) {
          runFilters {
            runExecutor(executorPlugin) { executor =>
              runOutput {
                execute(executor)
              }
            }
          }
        }
      }
      catch {
        case ex: Throwable
            if state.isAllTasksCommitted && state.isAllTransactionsCommitted =>
          logger.warn(
            s"Threw exception on the stage: ${state.getTransactionStage.getOrElse("None")}," +
              s" but all tasks and transactions are committed.",
            ex
          )
      }
    }
  }

  def sendPage(page: Page): Unit = ThreadNameContext.switch(loaderName) { _ =>
    state.sendPage(page)
  }

  def cleanup(): Unit = {
    ThreadNameContext.switch(loaderName) { _ =>
      state.sendSentinels()

      val outputTaskSource: TaskSource = outputPlugin match {
        case _: FileOutputRunner =>
          FileOutputRunner.getFileOutputTaskSource(
            state.getOutputTaskSource.get
          )
        case _ => state.getOutputTaskSource.get
      }
      outputPlugin.cleanup(
        outputTaskSource,
        state.getExecutorSchema.get,
        state.getOutputTaskCount.get,
        state.getOutputTaskReports.flatten
      )

      state.cleanup()
    }
  }

  def getResult: BreakinBulkLoader.Result =
    ThreadNameContext.switch(loaderName) { _ => buildResult() }

  private def lastFilterSchema: Schema =
    state.getFilterSchemas.map(_.last).getOrElse {
      throw new ConfigException(
        "'filterSchemas' must be set. Call #runFilters before."
      )
    }

  private def buildResult(): BreakinBulkLoader.Result = {
    BreakinBulkLoader.Result(
      configDiff = Exec.newConfigDiff().tap { configDiff: ConfigDiff =>
        state.getInputConfigDiff.foreach(configDiff.setNested("in", _))
      // NOTE: BreakinBulkLoader does not support PipeOutputPlugin configuration.
      // state.getOutputConfigDiff.foreach(configDiff.setNested("out", _))
      },
      ignoredExceptions = state.getExceptions
    )
  }

  private def newProcessTask: ProcessTask = {
    new ProcessTask(
      null,
      outputPluginType,
      filterPluginTypes,
      state.getInputTaskSource.get,
      state.getOutputTaskSource.get,
      state.getFilterTaskSources.get,
      state.getFilterSchemas.get,
      state.getExecutorSchema.get,
      Exec.newTaskSource()
    )
  }

  // scalafmt: { maxColumn = 130 }
  private def runInput(inputPlugin: InputPlugin)(f: => Unit): Unit = {
    state.setTransactionStage(INPUT_BEGIN)
    val inputControl: InputPlugin.Control =
      (inputTaskSource: TaskSource, inputSchema: Schema, inputTaskCount: Int) => {
        state.setInputSchema(inputSchema)
        state.setInputTaskSource(inputTaskSource)
        state.setInputTaskCount(inputTaskCount)
        f
        state.setTransactionStage(INPUT_COMMIT)
        state.getAllInputTaskReports
      }
    val inputConfigDiff: ConfigDiff = inputPlugin.transaction(inputTask, inputControl)
    state.setInputConfigDiff(inputConfigDiff)
  }

  private def runFilters(f: => Unit): Unit = {
    val inputSchema: Schema = state.getInputSchema.getOrElse {
      throw new ConfigException("'inputSchema' must be set. Call #runInput before.")
    }
    state.setTransactionStage(FILTER_BEGIN)
    val filtersControl: Filters.Control =
      (filterTaskSources: JList[TaskSource], filterSchemas: JList[Schema]) => {
        state.setFilterTaskSources(filterTaskSources)
        state.setFilterSchemas(filterSchemas)
        f
        state.setTransactionStage(FILTER_COMMIT)
      }
    Filters.transaction(filterPlugins, filterTasks, inputSchema, filtersControl)
  }

  private def runExecutor(executorPlugin: ExecutorPlugin)(f: ExecutorPlugin.Executor => Unit): Unit = {
    val inputTaskCount: Int = state.getInputTaskCount.getOrElse {
      throw new ConfigException("'inputTaskCount' must be set. Call #runInput before.")
    }
    state.setTransactionStage(EXECUTOR_BEGIN)
    val executorControl: ExecutorPlugin.Control =
      (executorSchema: Schema, outputTaskCount: Int, executor: ExecutorPlugin.Executor) => {
        state.setExecutorSchema(executorSchema)
        state.setOutputTaskCount(outputTaskCount)
        f(executor)
        state.setTransactionStage(EXECUTOR_COMMIT)
      }
    executorPlugin.transaction(executorTask, lastFilterSchema, inputTaskCount, executorControl)
  }

  private def runOutput(f: => Unit): Unit = {
    val executorSchema: Schema = state.getExecutorSchema.getOrElse {
      throw new ConfigException("'executorSchema' must be set. Call #runExecutor before.")
    }
    val outputTaskCount: Int = state.getOutputTaskCount.getOrElse {
      throw new ConfigException("'outputTaskCount' must be set. Call #runExecutor before.")
    }
    state.setTransactionStage(OUTPUT_BEGIN)
    val outputControl: OutputPlugin.Control =
      (outputTaskSource: TaskSource) => {
        state.setOutputTaskSource(outputTaskSource)
        f
        state.setTransactionStage(OUTPUT_COMMIT)
        state.getAllOutputTaskReports
      }
    val outputConfigDiff: ConfigDiff =
      outputPlugin.transaction(outputTask, executorSchema, outputTaskCount, outputControl)
    state.setOutputConfigDiff(outputConfigDiff)
  }

  private def execute(executor: ExecutorPlugin.Executor): Unit = {
    val processState: ProcessState = state.newProcessState
    processState.initialize(state.getInputTaskCount.get, state.getOutputTaskCount.get)
    state.setTransactionStage(RUN)
    if (!state.isAllTasksCommitted) {
      executor.execute(newProcessTask, processState)
      if (!state.isAllTasksCommitted) throw state.buildRepresentativeException
    }
    if (!state.isAllTasksCommitted) {
      throw new RuntimeException(
        s"${state.countUncommittedInputTasks} input tasks" +
          s" and ${state.countUncommittedOutputTasks} output tasks failed."
      )
    }
  }
  // scalafmt: { maxColumn = 80 }
}
