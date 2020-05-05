package pro.civitaspo.embulk.filter.copy

import java.util.{Optional, List => JList}

import org.embulk.config.{
  Config,
  ConfigDefault,
  ConfigException,
  ConfigSource,
  Task,
  TaskSource
}
import org.embulk.spi.{
  DataException,
  Exec,
  FilterPlugin,
  Page,
  PageBuilder,
  PageOutput,
  Schema
}
import org.slf4j.{Logger, LoggerFactory}

import scala.concurrent.{Await, ExecutionContextExecutor, Future}
import scala.concurrent.duration.Duration
import scala.util.{Failure, Success}
import scala.util.chaining._

class CopyFilterPlugin extends FilterPlugin {
  import implicits._

  private val logger: Logger =
    LoggerFactory.getLogger(classOf[CopyFilterPlugin])

  trait PluginTask extends Task {
    @Config("config")
    @ConfigDefault("null")
    def getConfig: Optional[BreakinBulkLoader.Task]

    @Config("copy")
    @ConfigDefault("[]")
    def getCopy: JList[BreakinBulkLoader.Task]
  }

  override def transaction(
      config: ConfigSource,
      inputSchema: Schema,
      control: FilterPlugin.Control
  ): Unit = {
    val task = config.loadConfig(classOf[PluginTask])
    task.getConfig.foreach { cfg =>
      logger.warn("[DEPRECATED] Use \"copy\" option instead.")
      task.getCopy.add(cfg)
    }
    if (task.getCopy.isEmpty)
      throw new ConfigException("Either 'copy' or 'config' option is required.")
    val transactionId: String = Utils.genTransactionId()
    task.getCopy.foreach(_.setTransactionId(transactionId))

    withRunningBulkLoaders(task, inputSchema, transactionId) {
      control.run(task.dump(), inputSchema)
    }
  }

  override def open(
      taskSource: TaskSource,
      inputSchema: Schema,
      outputSchema: Schema,
      output: PageOutput
  ): PageOutput = {
    val task = taskSource.loadTask(classOf[PluginTask])

    val copyOutputs: Seq[PageOutput] = task.getCopy.zipWithIndex
      .map(x => BreakinBulkLoader(x._1, x._2))
      .map(l => Utils.buildCopyPageOutput(l.sendPage))

    new PageOutput {
      private val pageBuilders: Seq[PageBuilder] = (copyOutputs :+ output).map(
        new PageBuilder(Exec.getBufferAllocator, inputSchema, _)
      )
      private val visitor = CopyColumnVisitor(inputSchema, pageBuilders)
      override def add(page: Page): Unit = visitor.visit(page)
      override def finish(): Unit = pageBuilders.foreach(_.finish())
      override def close(): Unit = pageBuilders.foreach(_.close())
    }
  }

  private def withRunningBulkLoaders(
      task: PluginTask,
      schema: Schema,
      transactionId: String
  )(
      runControl: => Unit
  ): Unit = {
    val loaders: Seq[BreakinBulkLoader] = task.getCopy.zipWithIndex
      .map(x => BreakinBulkLoader(x._1, x._2))
    val runFutures: Seq[Future[Unit]] = {
      implicit val ec: ExecutionContextExecutor =
        Utils.newDefaultExecutionContext()
      loaders
        .map(l => Future(l.run(schema)))
        .tap { futures =>
          futures.foreach(_.onComplete {
            case Failure(ex) => throw new DataException(ex)
            case Success(_) =>
              ThreadNameContext.switch(s"transaction[$transactionId]") { _ =>
                logger.info(
                  s"Copy in progress." +
                    s" {done: ${futures.count(_.isCompleted)} / ${futures.size}}"
                )
              }
          })
        }
    }

    runControl

    val cleanupFutures: Seq[Future[Unit]] = {
      implicit val ec: ExecutionContextExecutor =
        Utils.newDefaultExecutionContext()
      loaders.map(l => Future(l.cleanup())).tap { futures =>
        futures.foreach(_.onComplete {
          case Failure(ex) => throw new DataException(ex)
          case _           => // do nothing
        })
      }
    }

    {
      implicit val ec: ExecutionContextExecutor =
        Utils.newDefaultExecutionContext()
      Await.result(Future.sequence(runFutures), Duration.Inf)
      Await.result(Future.sequence(cleanupFutures), Duration("1h"))
    }

    logger.info(s"All of the copies are completed. {done: ${runFutures
      .count(_.isCompleted)} / ${runFutures.size}}")
  }
}
