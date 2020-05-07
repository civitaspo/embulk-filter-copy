package pro.civitaspo.embulk.filter.copy

import java.nio.charset.StandardCharsets.UTF_8
import java.security.MessageDigest
import java.util.UUID

import org.embulk.spi.{Page, PageOutput}

import scala.concurrent.{ExecutionContext, ExecutionContextExecutor}

object Utils {
  def genTransactionId(): String = {
    val uuid: UUID = UUID.randomUUID()
    val md: MessageDigest = MessageDigest.getInstance("SHA1")
    val sha1: String =
      md.digest(uuid.toString.getBytes(UTF_8)).map("%02x".format(_)).mkString
    sha1.take(7)
  }

  def buildCopyPageOutput(f: Page => Unit): PageOutput = new PageOutput {
    override def add(page: Page): Unit = f(page)
    override def finish(): Unit = {}
    override def close(): Unit = {}
  }

  def getNumberOfCores: Int = Runtime.getRuntime.availableProcessors()

  def newDefaultExecutionContext(): ExecutionContextExecutor =
    ExecutionContext.fromExecutor(null)
}
