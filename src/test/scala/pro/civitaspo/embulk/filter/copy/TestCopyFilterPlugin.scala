package pro.civitaspo.embulk.filter.copy

import org.embulk.spi.Schema
import org.embulk.spi.`type`.Types
import org.embulk.spi.time.Timestamp

class TestCopyFilterPlugin extends EmbulkTestHelper {

  private def assertData(
      expectedData: Seq[Seq[Any]],
      actualData: Seq[Seq[Any]],
      clue: String = ""
  ): Unit = {
    expectedData.indices.foreach { lineIdx: Int =>
      expectedData(lineIdx).indices.foreach { columnIdx: Int =>
        assert(
          expectedData(lineIdx)(columnIdx) == actualData(lineIdx)(columnIdx),
          s"Asserted at line: $lineIdx, column: $columnIdx." +
            s"${if (clue.nonEmpty) s" clue = $clue."}"
        )
      }
    }
  }

  test("The data is copied") {
    println("The data is copied 1")
    val inSchema: Schema = Schema
      .builder()
      .add("c0", Types.BOOLEAN)
      .add("c1", Types.STRING)
      .add("c2", Types.LONG)
      .add("c3", Types.DOUBLE)
      .add("c4", Types.TIMESTAMP)
      .add("c5", Types.JSON)
      .build()
    val inData: Seq[Seq[Any]] = Seq(
      Seq(
        true,
        "l1",
        1L,
        1.11d,
        Timestamp.ofEpochMilli(5),
        newJson("""{"a": 5, "b": "x", "c": {"y": "z"}}""")
      )
    )
    val mock1 = newMockOutput()
    val mock2 = newMockOutput()
    val config: String =
      s"""
         |copy:
         |  - out:
         |      type: ${mock1.pluginTypeName}
         |      name: ${mock1.name}
         |  - out:
         |      type: ${mock2.pluginTypeName}
         |      name: ${mock2.name}
         |""".stripMargin
    println("The data is copied 2")

    runFilter(loadYaml(config), inSchema, inData, { actualData: Seq[Seq[Any]] =>
      println("The data is copied 3")
      assertData(inData, actualData)
    })
    println("The data is copied 4")
    assertData(inData, mock1.data(inSchema), "copy[0]")
    assertData(inData, mock2.data(inSchema), "copy[1]")
  }

  test("Can define \"config\" option.") {
    val inSchema: Schema = Schema
      .builder()
      .add("c0", Types.BOOLEAN)
      .add("c1", Types.STRING)
      .add("c2", Types.LONG)
      .add("c3", Types.DOUBLE)
      .add("c4", Types.TIMESTAMP)
      .add("c5", Types.JSON)
      .build()
    val inData: Seq[Seq[Any]] = Seq(
      Seq(
        true,
        "l1",
        1L,
        1.11d,
        Timestamp.ofEpochMilli(5),
        newJson("""{"a": 5, "b": "x", "c": {"y": "z"}}""")
      )
    )
    val mock1 = newMockOutput()
    val config: String =
      s"""
         |config:
         |  out:
         |    type: ${mock1.pluginTypeName}
         |    name: ${mock1.name}
         |""".stripMargin

    runFilter(loadYaml(config), inSchema, inData, { actualData: Seq[Seq[Any]] =>
      assertData(inData, actualData)
    })
    assertData(inData, mock1.data(inSchema), "copy[0]")
  }

  test("The each copied data is modified") {
    val inSchema: Schema = Schema
      .builder()
      .add("c0", Types.BOOLEAN)
      .add("c1", Types.STRING)
      .add("c2", Types.LONG)
      .add("c3", Types.DOUBLE)
      .add("c4", Types.TIMESTAMP)
      .add("c5", Types.JSON)
      .build()
    val inData: Seq[Seq[Any]] = Seq(
      Seq(
        true,
        "l1",
        1L,
        1.11d,
        Timestamp.ofEpochMilli(5),
        newJson("""{"a": 5, "b": "x", "c": {"y": "z"}}""")
      )
    )
    val mock1 = newMockOutput("copy[0]")
    val mock2 = newMockOutput("copy[1]")
    val config: String =
      s"""
         |copy:
         |  - filters:
         |      - type: remove_columns
         |        remove:
         |          - c2
         |          - c3
         |    out:
         |      type: ${mock1.pluginTypeName}
         |      name: ${mock1.name}
         |  - filters:
         |      - type: remove_columns
         |        remove:
         |          - c4
         |          - c5
         |    out:
         |      type: ${mock2.pluginTypeName}
         |      name: ${mock2.name}
         |""".stripMargin

    runFilter(loadYaml(config), inSchema, inData, { actualData: Seq[Seq[Any]] =>
      assertData(inData, actualData)
    })
    assertData(
      expectedData = Seq(
        Seq(
          true,
          "l1",
          Timestamp.ofEpochMilli(5),
          newJson("""{"a": 5, "b": "x", "c": {"y": "z"}}""")
        )
      ),
      actualData = mock1.data(
        Schema
          .builder()
          .add("c0", Types.BOOLEAN)
          .add("c1", Types.STRING)
          .add("c4", Types.TIMESTAMP)
          .add("c5", Types.JSON)
          .build()
      ),
      clue = mock1.name
    )
    assertData(
      expectedData = Seq(
        Seq(
          true,
          "l1",
          1L,
          1.11d
        )
      ),
      actualData = mock2.data(
        Schema
          .builder()
          .add("c0", Types.BOOLEAN)
          .add("c1", Types.STRING)
          .add("c2", Types.LONG)
          .add("c3", Types.DOUBLE)
          .build()
      ),
      clue = mock2.name
    )
  }

  test("Can define nested copy") {
    val inSchema: Schema = Schema
      .builder()
      .add("c0", Types.BOOLEAN)
      .add("c1", Types.STRING)
      .add("c2", Types.LONG)
      .add("c3", Types.DOUBLE)
      .add("c4", Types.TIMESTAMP)
      .add("c5", Types.JSON)
      .build()
    val inData: Seq[Seq[Any]] = Seq(
      Seq(
        true,
        "l1",
        1L,
        1.11d,
        Timestamp.ofEpochMilli(5),
        newJson("""{"a": 5, "b": "x", "c": {"y": "z"}}""")
      )
    )
    val mock1 = newMockOutput("copy[0]")
    val mock2 = newMockOutput("copy[1]")
    val mock3 = newMockOutput("copy[0].copy[0]")
    val mock4 = newMockOutput("copy[0].copy[1].copy[0]")
    val mock5 = newMockOutput("copy[0].copy[1]")

    val config: String =
      s"""
         |copy:
         |  - filters:
         |      - type: copy
         |        copy:
         |          - out:
         |              type: ${mock3.pluginTypeName}
         |              name: ${mock3.name}
         |      - type: copy
         |        copy:
         |          - filters:
         |              - type: copy
         |                copy:
         |                  - out:
         |                      type: ${mock4.pluginTypeName}
         |                      name: ${mock4.name}
         |            out:
         |              type: ${mock5.pluginTypeName}
         |              name: ${mock5.name}
         |    out:
         |      type: ${mock1.pluginTypeName}
         |      name: ${mock1.name}
         |  - out:
         |      type: ${mock2.pluginTypeName}
         |      name: ${mock2.name}
         |""".stripMargin

    runFilter(loadYaml(config), inSchema, inData, { actualData: Seq[Seq[Any]] =>
      assertData(inData, actualData)
    })
    Seq(mock1, mock2, mock3, mock4, mock5).foreach { m =>
      assertData(inData, m.data(inSchema), m.name)
    }
  }
}
