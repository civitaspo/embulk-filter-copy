package pro.civitaspo.embulk.filter.copy

import java.util.concurrent.ExecutionException

import com.fasterxml.jackson.databind.{JsonNode, ObjectMapper}
import com.google.inject.{Binder, Guice, Module, Stage}
import org.embulk.{TestPluginSourceModule, TestUtilityModule}
import org.embulk.config.{
  ConfigLoader,
  ConfigSource,
  DataSourceImpl,
  ModelManager,
  TaskSource
}
import org.embulk.exec.{
  ExecModule,
  ExtensionServiceLoaderModule,
  SystemConfigModule
}
import org.embulk.jruby.JRubyScriptingModule
import org.embulk.plugin.{
  BuiltinPluginSourceModule,
  InjectedPluginSource,
  PluginClassLoaderModule
}
import org.embulk.spi.{
  Exec,
  ExecSession,
  FilterPlugin,
  OutputPlugin,
  Page,
  PageTestUtils,
  Schema
}
import org.embulk.spi.util.Pages
import org.embulk.spi.TestPageBuilderReader.MockPageOutput
import org.msgpack.value.{Value, ValueFactory}
import org.scalatest.funsuite.AnyFunSuite
import org.scalatest.BeforeAndAfter
import org.scalatest.diagrams.Diagrams

import scala.util.Using

object EmbulkTestHelper {
  case class TestRuntimeModule() extends Module {
    override def configure(binder: Binder): Unit = {
      val systemConfig = new DataSourceImpl(null)
      new SystemConfigModule(systemConfig).configure(binder)
      new ExecModule(systemConfig).configure(binder)
      new ExtensionServiceLoaderModule(systemConfig).configure(binder)
      new BuiltinPluginSourceModule().configure(binder)
      new JRubyScriptingModule(systemConfig).configure(binder)
      new PluginClassLoaderModule().configure(binder)
      new TestUtilityModule().configure(binder)
      new TestPluginSourceModule().configure(binder)
      InjectedPluginSource.registerPluginTo(
        binder,
        classOf[FilterPlugin],
        "copy",
        classOf[CopyFilterPlugin]
      )
      InjectedPluginSource.registerPluginTo(
        binder,
        classOf[OutputPlugin],
        "mock",
        classOf[MockOutputPlugin]
      )
    }
  }

  case class MockOutput(name: String) {
    val pluginTypeName: String = "mock"
    def data(outputSchema: Schema): Seq[Seq[Any]] = {
      import implicits._
      val pages: Seq[Page] = MockOutputPlugin.getOrInitialize(name).pages
      Pages.toObjects(outputSchema, pages).map(_.toSeq)
    }
  }

  def getExecSession: ExecSession = {
    val injector =
      Guice.createInjector(Stage.PRODUCTION, TestRuntimeModule())
    val execConfig = new DataSourceImpl(
      injector.getInstance(classOf[ModelManager])
    )
    ExecSession.builder(injector).fromExecConfig(execConfig).build()
  }
}

abstract class EmbulkTestHelper
    extends AnyFunSuite
    with BeforeAndAfter
    with Diagrams {

  import implicits._

  var exec: ExecSession = _

  before {
    exec = EmbulkTestHelper.getExecSession
    MockOutputPlugin.cleanup()
  }
  after {
    exec.cleanup()
    exec = null
    MockOutputPlugin.cleanup()
  }

  def runFilter(
      filterConfig: ConfigSource,
      inputSchema: Schema,
      data: Seq[Seq[Any]],
      test: Seq[Seq[Any]] => Unit = { _ => }
  ): Unit = {
    try {
      Exec.doWith(
        exec,
        () => {
          val plugin = exec.getInjector
            .getInstance(classOf[CopyFilterPlugin])
          val control: FilterPlugin.Control =
            (taskSource: TaskSource, outputSchema: Schema) => {
              val output = new MockPageOutput()
              Using.resource(
                plugin.open(taskSource, inputSchema, outputSchema, output)
              ) { pageOutput =>
                try PageTestUtils
                  .buildPage(
                    Exec.getBufferAllocator,
                    outputSchema,
                    data.flatten: _*
                  )
                  .foreach(pageOutput.add)
                finally pageOutput.finish()
              }
              test(Pages.toObjects(outputSchema, output.pages).map(_.toSeq))
            }
          plugin.transaction(filterConfig, inputSchema, control)
        }
      )
    }
    catch {
      case ex: ExecutionException => throw ex.getCause
    }
  }

  def loadYaml(yaml: String): ConfigSource = {
    new ConfigLoader(exec.getModelManager).fromYamlString(yaml)
  }

  def newMockOutput(prefix: String = ""): EmbulkTestHelper.MockOutput = {
    val random = Utils.genTransactionId()
    EmbulkTestHelper.MockOutput(prefix + random)
  }

  def newJson(json: String): Value = {
    newJson(new ObjectMapper().readTree(json))
  }

  def newJson(node: JsonNode): Value = {
    node match {
      case j if j.isNull || j.isMissingNode => ValueFactory.newNil()
      case j if j.isArray =>
        ValueFactory.newArray(j.elements().map(newJson).toSeq)
      case j if j.isBigInteger => ValueFactory.newInteger(j.bigIntegerValue())
      case j if j.isInt || j.isIntegralNumber =>
        ValueFactory.newInteger(j.intValue())
      case j if j.isLong    => ValueFactory.newInteger(j.longValue())
      case j if j.isShort   => ValueFactory.newInteger(j.shortValue())
      case j if j.isBoolean => ValueFactory.newBoolean(j.booleanValue())
      case j if j.isDouble  => ValueFactory.newFloat(j.doubleValue())
      case j if j.isFloat || j.isFloatingPointNumber =>
        ValueFactory.newFloat(j.floatValue())
      case j if j.isBigDecimal =>
        ValueFactory.newFloat(j.decimalValue().doubleValue())
      case j if j.isTextual => ValueFactory.newString(j.textValue())
      case j if j.isBinary  => ValueFactory.newBinary(j.binaryValue())
      case j if j.isObject =>
        ValueFactory
          .newMapBuilder()
          .putAll(
            j.fields()
              .map(x => ValueFactory.newString(x.getKey) -> newJson(x.getValue))
              .toMap
          )
          .build()
      case _ =>
        throw new UnsupportedOperationException
    }
  }
}
