package com.kelkoogroup.dojo.spark

import com.holdenkarau.spark.testing.DataFrameSuiteBase
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions.count
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

  test("add count column - with Window aggregation function") {
    val wByDesc = Window.partitionBy("desc")

    val actualGroupByDF = df.select(
      $"id",
      $"desc",
      count("*").over(wByDesc).as("count")
    )

    actualGroupByDF.collect should contain theSameElementsAs expectedGroupByDF.collect
  }

  test("add index column") {
    val wIndex = Window.orderBy("id")

    val actualDF = df.select(
      count("*")
        .over(wIndex.rowsBetween(Window.unboundedPreceding, Window.currentRow))
        .as("num_row"),
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
}
