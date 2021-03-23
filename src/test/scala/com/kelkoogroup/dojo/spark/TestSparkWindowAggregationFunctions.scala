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
    (1, "a", 5),
    (2, "b", 2),
    (3, "c", 3),
    (4, "a", 5),
    (5, "b", 2),
    (6, "a", 5),
    (7, "c", 3),
    (8, "a", 5),
    (9, "c", 3),
    (10, "a", 5)
  ).toDF("id", "desc", "count")

  lazy val df = Seq(
    (1, "a"),
    (2, "b"),
    (3, "c"),
    (4, "a"),
    (5, "b"),
    (6, "a"),
    (7, "c"),
    (8, "a"),
    (9, "c"),
    (10, "a")
  ).toDF("id", "desc")

  test("add count column - with groupBy") {
    val dfCountDesc = df.groupBy("desc").count

    val actualGroupByDF = df.join(
      dfCountDesc, Seq("desc"), "left_outer"
    ).select("id", "desc", "count")

    actualGroupByDF.collect should contain theSameElementsAs expectedGroupByDF.collect
  }

  test("add count column - with Window aggregation function") {
    val windowIndexRange = Window.partitionBy("desc")

    val actualGroupByDF = df.select(
      $"id",
      $"desc",
      count("*").over(windowIndexRange).as("count")
    )

    actualGroupByDF.collect should contain theSameElementsAs expectedGroupByDF.collect
  }
}
