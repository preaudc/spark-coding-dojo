package com.kelkoogroup.dojo.spark.solution

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.junit.runner.RunWith
import org.scalatest.{FunSuite, Matchers}
import org.scalatest.junit.JUnitRunner

@RunWith(classOf[JUnitRunner])
class TestSparkWindowAggregationFunctions extends FunSuite with Matchers with DataFrameSuiteBase {
  implicit lazy val sparkSession = spark
  import spark.implicits._

  override def beforeAll() = {
    super.beforeAll()
    sc.setLogLevel("WARN")
  }

  override def afterAll() = {
    super.afterAll()
  }

  lazy val df = Seq(
    ("id01", "a"),
    ("id02", "b"),
    ("id03", "c"),
    ("id04", "a"),
    ("id05", "b"),
    ("id06", "a"),
    ("id07", "c"),
    ("id08", "a"),
    ("id09", "c"),
    ("id10", "a")
  ).toDF("id", "desc")

  lazy val ds = Seq(
    Data(1, "a"),
    Data(2, "b"),
    Data(3, "c"),
    Data(4, "a"),
    Data(5, "b"),
    Data(6, "a"),
    Data(7, "c"),
    Data(8, "a"),
    Data(9, "c"),
    Data(10, "a")
  ).toDS

  lazy val expectedGroupByDF = Seq(
    ("id01", "a", 5),
    ("id02", "b", 2),
    ("id03", "c", 3),
    ("id04", "a", 5),
    ("id05", "b", 2),
    ("id06", "a", 5),
    ("id07", "c", 3),
    ("id08", "a", 5),
    ("id09", "c", 3),
    ("id10", "a", 5)
  ).toDF("id", "desc", "count")

  lazy val expectedDS = Seq(
    Data(1, "a", 1, 5),
    Data(4, "a", 2, 5),
    Data(6, "a", 3, 5),
    Data(8, "a", 4, 5),
    Data(10, "a", 5, 5),
    Data(2, "b", 1, 2),
    Data(5, "b", 2, 2),
    Data(3, "c", 1, 3),
    Data(7, "c", 2, 3),
    Data(9, "c", 3, 3)
  ).toDS()

  // standard implementation with groupBy
  test("add count column - with groupBy") {
    val dfCountDesc = df.groupBy("desc").count

    val actualGroupByDF = df
      .join(
        dfCountDesc,
        Seq("desc"),
        "left_outer"
      )
      .select("id", "desc", "count")

    actualGroupByDF.collect should contain theSameElementsAs expectedGroupByDF.collect
  }

  // WINDOW: BASIC USAGE

  // with Window aggregation function
  test("add count column - with Window aggregation function") {
    val byDesc = Window.partitionBy("desc")

    val actualGroupByDF = df.select(
      $"id",
      $"desc",
      count("*").over(byDesc).as("count")
    )

    actualGroupByDF.collect should contain theSameElementsAs expectedGroupByDF.collect
  }

  // add index column, with rowsBetween
  test("add index column, ordered by id") {
    val orderedById = Window.orderBy("id").rowsBetween(Window.unboundedPreceding, Window.currentRow)

    val actualDF = df.select(
      count("*").over(orderedById).as("num_row"),
      $"id",
      $"desc"
    )

    val expectedDF = Seq(
      (1L, "id01", "a"),
      (2L, "id02", "b"),
      (3L, "id03", "c"),
      (4L, "id04", "a"),
      (5L, "id05", "b"),
      (6L, "id06", "a"),
      (7L, "id07", "c"),
      (8L, "id08", "a"),
      (9L, "id09", "c"),
      (10L, "id10", "a")
    ).toDF("num_row", "id", "desc")
    actualDF.collect should contain theSameElementsAs expectedDF.collect
  }

  // not a test - monotonically_increasing_id example usage
  test("not a test --> use monotonically_increasing_id for unique id, not row_number!") {
    df.select(
      monotonically_increasing_id().as("uid"),
      $"id",
      $"desc"
    ).show()
  }

  // demonstrate difference between rowsBetween and rangeBetween
  test("rowsBetween or rangeBetween?") {
    val orderedByDesc = Window.orderBy("desc")

    val actualRowsDF = df
      .orderBy("id")
      .select(
        count("*")
          .over(orderedByDesc.rowsBetween(Window.unboundedPreceding, Window.currentRow))
          .as("num_row"),
        $"id",
        $"desc"
      )
    val expectedRowsDF = Seq(
      (1L, "id01", "a"),
      (2L, "id04", "a"),
      (3L, "id06", "a"),
      (4L, "id08", "a"),
      (5L, "id10", "a"),
      (6L, "id02", "b"),
      (7L, "id05", "b"),
      (8L, "id03", "c"),
      (9L, "id07", "c"),
      (10L, "id09", "c")
    ).toDF("num_row", "id", "desc")
    actualRowsDF.collect should contain theSameElementsAs expectedRowsDF.collect

    val actualRangeDF = df
      .orderBy("id")
      .select(
        count("*")
          .over(orderedByDesc.rangeBetween(Window.unboundedPreceding, Window.currentRow))
          .as("num_row"),
        $"id",
        $"desc"
      )
    val expectedRangeDF = Seq(
      (5L, "id01", "a"),
      (5L, "id04", "a"),
      (5L, "id06", "a"),
      (5L, "id08", "a"),
      (5L, "id10", "a"),
      (7L, "id02", "b"),
      (7L, "id05", "b"),
      (10L, "id03", "c"),
      (10L, "id07", "c"),
      (10L, "id09", "c")
    ).toDF("num_row", "id", "desc")
    actualRangeDF.collect should contain theSameElementsAs expectedRangeDF.collect
  }

  test(
    "test add range_one column containing values from the rows just before and after the current one"
  ) {
    val orderedById = Window.orderBy("id")

    val actualDF = df.select(
      $"id",
      $"desc",
      collect_list("id").over(orderedById.rowsBetween(-1L, 1L)).as("range_one")
    )

    val expectedDF = Seq(
      ("id01", "a", Seq("id01", "id02")),
      ("id02", "b", Seq("id01", "id02", "id03")),
      ("id03", "c", Seq("id02", "id03", "id04")),
      ("id04", "a", Seq("id03", "id04", "id05")),
      ("id05", "b", Seq("id04", "id05", "id06")),
      ("id06", "a", Seq("id05", "id06", "id07")),
      ("id07", "c", Seq("id06", "id07", "id08")),
      ("id08", "a", Seq("id07", "id08", "id09")),
      ("id09", "c", Seq("id08", "id09", "id10")),
      ("id10", "a", Seq("id09", "id10"))
    ).toDF("id", "desc", "range_one")
    actualDF.collect should contain theSameElementsAs expectedDF.collect
  }

  // WINDOW FUNCTIONS
  // http://spark.apache.org/docs/2.4.6/api/scala/index.html#org.apache.spark.sql.functions$

  // add index column, with Window function row_number
  test("add index column, ordered by id - with row_number") {
    val orderedById = Window.orderBy("id")

    val actualDF = df.select(
      row_number.over(orderedById).as("num_row"),
      $"id",
      $"desc"
    )

    val expectedDF = Seq(
      (1L, "id01", "a"),
      (2L, "id02", "b"),
      (3L, "id03", "c"),
      (4L, "id04", "a"),
      (5L, "id05", "b"),
      (6L, "id06", "a"),
      (7L, "id07", "c"),
      (8L, "id08", "a"),
      (9L, "id09", "c"),
      (10L, "id10", "a")
    ).toDF("num_row", "id", "desc")
    actualDF.collect should contain theSameElementsAs expectedDF.collect
  }

  // other Window functions to test:
  // cume_dist, dense_rank, lag, lead, nth_value (N/A in Spark-2.4), ntile, percent_rank, rank

  // ADVANCED
  // Yet another solution to the problem investigated in a previous coding dojo:
  // "Coding Dojo/Dataset - advanced"

  // compact implementation with performance issue
  test("add indexes and length by id") {
    val actualDS = ds
      .groupByKey(_.desc)
      .flatMapGroups({ (_, iter) =>
        val sortedList = iter.toList.sortBy(_.id)
        sortedList
          .scanLeft(Data(0, ""))({ (prevData, curData) =>
            curData.copy(
              index = prevData.index + 1,
              length = sortedList.length.toLong
            )
          })
          .drop(1)
      })
      .as[Data]

    actualDS.collect should contain theSameElementsAs expectedDS.collect
  }

  test("add indexes and length by id - with Windows") {
    val windowIndexRange = Window.partitionBy("desc").orderBy("id")
    val windowLengthRange = Window.partitionBy("desc")

    val actualDS = ds
      .withColumn("index", row_number.over(windowIndexRange))
      .withColumn("length", count("*").over(windowLengthRange))
      .as[Data]

    actualDS.collect should contain theSameElementsAs expectedDS.collect
  }
}

case class Data(id: Int, desc: String, index: Long = 0, length: Long = 0)
