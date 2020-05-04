package pro.civitaspo.embulk.filter.copy

import org.embulk.spi.{
  Column,
  ColumnVisitor,
  Page,
  PageBuilder,
  PageReader,
  Schema
}

case class CopyColumnVisitor(schema: Schema, pageBuilders: Seq[PageBuilder])
    extends ColumnVisitor {

  private val pageReader: PageReader = new PageReader(schema)

  def visit(page: Page): Unit = {
    try {
      pageReader.setPage(page)
      while (pageReader.nextRecord()) {
        schema.visitColumns(this)
        pageBuilders.foreach(_.addRecord())
      }
    }
    finally pageReader.close()
  }

  private def nullOr(column: Column)(f: => Unit): Unit =
    if (pageReader.isNull(column))
      pageBuilders.foreach(_.setNull(column))
    else f
  override def booleanColumn(column: Column): Unit = nullOr(column) {
    val bool = pageReader.getBoolean(column)
    pageBuilders.foreach(_.setBoolean(column, bool))
  }
  override def longColumn(column: Column): Unit = nullOr(column) {
    val long = pageReader.getLong(column)
    pageBuilders.foreach(_.setLong(column, long))
  }
  override def doubleColumn(column: Column): Unit = nullOr(column) {
    val double = pageReader.getDouble(column)
    pageBuilders.foreach(_.setDouble(column, double))
  }
  override def stringColumn(column: Column): Unit = nullOr(column) {
    val string = pageReader.getString(column)
    pageBuilders.foreach(_.setString(column, string))
  }
  override def timestampColumn(column: Column): Unit =
    nullOr(column) {
      val timestamp = pageReader.getTimestamp(column)
      pageBuilders.foreach(_.setTimestamp(column, timestamp))
    }
  override def jsonColumn(column: Column): Unit = nullOr(column) {
    val json = pageReader.getJson(column)
    pageBuilders.foreach(_.setJson(column, json))
  }

}
