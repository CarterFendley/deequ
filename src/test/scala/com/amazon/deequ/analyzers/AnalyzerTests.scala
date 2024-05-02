/**
  * Copyright 2018 Amazon.com, Inc. or its affiliates. All Rights Reserved.
  *
  * Licensed under the Apache License, Version 2.0 (the "License"). You may not
  * use this file except in compliance with the License. A copy of the License
  * is located at
  *
  *     http://aws.amazon.com/apache2.0/
  *
  * or in the "license" file accompanying this file. This file is distributed on
  * an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
  * express or implied. See the License for the specific language governing
  * permissions and limitations under the License.
  *
  */

package com.amazon.deequ
package analyzers

import com.amazon.deequ.analyzers.runners.NoSuchColumnException
import com.amazon.deequ.metrics.Distribution
import com.amazon.deequ.metrics.DistributionValue
import com.amazon.deequ.metrics.DoubleMetric
import com.amazon.deequ.metrics.Entity
import com.amazon.deequ.utils.AssertionUtils.TryUtils
import com.amazon.deequ.utils.FixtureSupport
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions.col
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.types._
import org.scalatest.Inside.inside
import org.scalatest.wordspec.AnyWordSpec
import org.scalatest.matchers.should.Matchers
import org.scalatest.wordspec.AnyWordSpec

import scala.util.Failure
import scala.util.Success

class AnalyzerTests extends AnyWordSpec with Matchers with SparkContextSpec with FixtureSupport {

  "Size analyzer" should {
    "compute correct metrics" in withSparkSession { sparkSession =>
      val dfMissing = getDfMissing(sparkSession)
      val dfFull = getDfFull(sparkSession)

      assert(Size().calculate(dfMissing) == DoubleMetric(Entity.Dataset, "Size", "*",
        Success(dfMissing.count())))
      assert(Size().calculate(dfFull) == DoubleMetric(Entity.Dataset, "Size", "*",
        Success(dfFull.count())))
    }
  }

  "Completeness analyzer" should {

    "compute correct metrics" in withSparkSession { sparkSession =>
      val dfMissing = getDfMissing(sparkSession)

      assert(Completeness("someMissingColumn").preconditions.size == 2,
        "should check column name availability")
      val result1 = Completeness("att1").calculate(dfMissing)
      assert(result1 == DoubleMetric(Entity.Column,
        "Completeness", "att1", Success(0.5), result1.fullColumn))
      val result2 = Completeness("att2").calculate(dfMissing)
      assert(result2 == DoubleMetric(Entity.Column,
        "Completeness", "att2", Success(0.75), result2.fullColumn))
      val result3 = Completeness("att2", Option("att1 is NOT NULL")).calculate(dfMissing)
      assert(result3 == DoubleMetric(Entity.Column,
      "Completeness", "att2", Success(4.0/6.0), result3.fullColumn))
    }

    "fail on wrong column input" in withSparkSession { sparkSession =>
      val dfMissing = getDfMissing(sparkSession)

      Completeness("someMissingColumn").calculate(dfMissing) match {
        case metric =>
          assert(metric.entity == Entity.Column)
          assert(metric.name == "Completeness")
          assert(metric.instance == "someMissingColumn")
          assert(metric.value.isFailure)
      }
    }

    "fail on nested column input" in withSparkSession { sparkSession =>

      val df = getDfWithNestedColumn(sparkSession)

      val result: DoubleMetric = Completeness("source").calculate(df)

      assert(result.value.isFailure)
    }

    "work with filtering" in withSparkSession { sparkSession =>
      val dfMissing = getDfMissing(sparkSession)

      val result = Completeness("att1", Some("item IN ('1', '2')")).calculate(dfMissing)
      assert(result == DoubleMetric(Entity.Column, "Completeness", "att1", Success(1.0), result.fullColumn))
    }

  }

  "Uniqueness analyzer" should {
    "compute correct metrics" in withSparkSession { sparkSession =>
      val dfMissing = getDfMissing(sparkSession)
      val dfFull = getDfFull(sparkSession)

      val uniquenessDfMissingAtt1 = Uniqueness("att1").calculate(dfMissing)
      assert(uniquenessDfMissingAtt1 == DoubleMetric(Entity.Column, "Uniqueness",
        "att1", Success(0.0), uniquenessDfMissingAtt1.fullColumn))
      val uniquenessDfMissingAtt2 = Uniqueness("att2").calculate(dfMissing)
      assert(uniquenessDfMissingAtt2 == DoubleMetric(Entity.Column, "Uniqueness",
        "att2", Success(0.0), uniquenessDfMissingAtt2.fullColumn))

      val uniquenessDfFullAtt1 = Uniqueness("att1").calculate(dfFull)
      assert(uniquenessDfFullAtt1 == DoubleMetric(Entity.Column, "Uniqueness",
        "att1", Success(0.25), uniquenessDfFullAtt1.fullColumn))
      val uniquenessDfFullAtt2 = Uniqueness("att2").calculate(dfFull)
      assert(uniquenessDfFullAtt2 == DoubleMetric(Entity.Column, "Uniqueness",
        "att2", Success(0.25), uniquenessDfFullAtt2.fullColumn))

    }
    "compute correct metrics on multiple columns" in withSparkSession { sparkSession =>
      val dfFull = getDfWithUniqueColumns(sparkSession)

      val unique = Uniqueness("unique").calculate(dfFull)
      assert(unique ==
        DoubleMetric(Entity.Column, "Uniqueness", "unique", Success(1.0), unique.fullColumn))
      val uniqueWithNulls = Uniqueness("uniqueWithNulls").calculate(dfFull)
      assert(uniqueWithNulls ==
        DoubleMetric(Entity.Column, "Uniqueness", "uniqueWithNulls", Success(1.0), uniqueWithNulls.fullColumn))
      val multiColUnique = Uniqueness(Seq("unique", "nonUnique")).calculate(dfFull)
      assert(multiColUnique ==
        DoubleMetric(Entity.Multicolumn, "Uniqueness", "unique,nonUnique", Success(1.0), multiColUnique.fullColumn))
      val multiColUniqueWithNull = Uniqueness(Seq("unique", "nonUniqueWithNulls")).calculate(dfFull)
      assert(multiColUniqueWithNull ==
        DoubleMetric(Entity.Multicolumn, "Uniqueness", "unique,nonUniqueWithNulls",
          Success(1.0), multiColUniqueWithNull.fullColumn))
      val multiColUniqueComb = Uniqueness(Seq("nonUnique", "onlyUniqueWithOtherNonUnique")).calculate(dfFull)
      assert(multiColUniqueComb ==
        DoubleMetric(Entity.Multicolumn, "Uniqueness", "nonUnique,onlyUniqueWithOtherNonUnique",
          Success(1.0), multiColUniqueComb.fullColumn))

    }
    "fail on wrong column input" in withSparkSession { sparkSession =>
      val dfFull = getDfWithUniqueColumns(sparkSession)

      Uniqueness("nonExistingColumn").calculate(dfFull) match {
        case metric =>
          assert(metric.entity == Entity.Column)
          assert(metric.name == "Uniqueness")
          assert(metric.instance == "nonExistingColumn")
          assert(metric.value.compareFailureTypes(Failure(new NoSuchColumnException(""))))
      }

      Uniqueness(Seq("nonExistingColumn", "unique")).calculate(dfFull) match {
        case metric =>
          assert(metric.entity == Entity.Multicolumn)
          assert(metric.name == "Uniqueness")
          assert(metric.instance == "nonExistingColumn,unique")
          assert(metric.value.compareFailureTypes(Failure(new NoSuchColumnException(""))))
      }
    }
  }

  "Entropy analyzer" should {
    "compute correct metrics" in withSparkSession { sparkSession =>
      val dfFull = getDfFull(sparkSession)

      assert(Entropy("att1").calculate(dfFull) ==
        DoubleMetric(Entity.Column, "Entropy", "att1",
          Success(-(0.75 * math.log(0.75) + 0.25 * math.log(0.25)))))
      assert(Entropy("att2").calculate(dfFull) ==
        DoubleMetric(Entity.Column, "Entropy", "att2",
          Success(-(0.75 * math.log(0.75) + 0.25 * math.log(0.25)))))

    }
  }

  "MutualInformation analyzer" should {
    "compute correct metrics " in withSparkSession { sparkSession =>
      val dfFull = getDfFull(sparkSession)
      assert(MutualInformation("att1", "att2").calculate(dfFull) ==
        DoubleMetric(Entity.Multicolumn, "MutualInformation", "att1,att2",
          Success(-(0.75 * math.log(0.75) + 0.25 * math.log(0.25)))))
    }
    "yields 0 for conditionally uninformative columns" in withSparkSession { sparkSession =>
      val df = getDfWithConditionallyUninformativeColumns(sparkSession)
      assert(MutualInformation("att1", "att2").calculate(df).value == Success(0.0))
    }
    "compute entropy for same column" in withSparkSession { session =>
      val data = getDfFull(session)

      val entropyViaMI = MutualInformation("att1", "att1").calculate(data)
      val entropy = Entropy("att1").calculate(data)

      assert(entropyViaMI.value.isSuccess)
      assert(entropy.value.isSuccess)

      assert(entropyViaMI.value.get == entropy.value.get)
    }
  }

  "Compliance analyzer" should {
    "compute correct metrics " in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      val result1 = Compliance("rule1", "att1 > 3", columns = List("att1")).calculate(df)
      inside(result1) { case DoubleMetric(entity, name, instance, value, fullColumn) =>
        entity shouldBe Entity.Column
        name shouldBe "Compliance"
        instance shouldBe "rule1"
        value shouldBe Success(3.0/6)
        fullColumn.isDefined shouldBe true
      }

      val result2 = Compliance("rule2", "att1 > 2", columns = List("att1")).calculate(df)
      inside(result2) { case DoubleMetric(entity, name, instance, value, fullColumn) =>
        entity shouldBe Entity.Column
        name shouldBe "Compliance"
        instance shouldBe "rule2"
        value shouldBe Success(4.0 / 6)
        fullColumn.isDefined shouldBe true
      }
    }

    "compute correct metrics with filtering" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      val result = Compliance("rule1", "att2 = 0", Some("att1 < 4"), columns = List("att1")).calculate(df)
      inside(result) { case DoubleMetric(entity, name, instance, value, fullColumn) =>
        entity shouldBe Entity.Column
        name shouldBe "Compliance"
        instance shouldBe "rule1"
        value shouldBe Success(1.0)
        fullColumn.isDefined shouldBe true
      }
    }

    "fail on wrong column input" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      Compliance("rule1", "attNoSuchColumn > 3", columns = List("attNoSuchColumn")).calculate(df) match {
        case metric =>
          assert(metric.entity == Entity.Column)
          assert(metric.name == "Compliance")
          assert(metric.instance == "rule1")
          assert(metric.value.isFailure)
      }

    }
  }


  "Histogram analyzer" should {
    "compute correct metrics " in withSparkSession { sparkSession =>
      val dfFull = getDfMissing(sparkSession)
      val histogram = Histogram("att1").calculate(dfFull)
      assert(histogram.value.isSuccess)

      histogram.value.get match {
        case hv =>
          assert(hv.numberOfBins == 3)
          assert(hv.values.size == 3)
          assert(hv.values.keys == Set("a", "b", Histogram.NullFieldReplacement))

      }
    }

    "compute correct sum metrics " in withSparkSession { sparkSession =>
      val dfFull = getDateDf(sparkSession)
      val histogram = Histogram("product", aggregateFunction = Histogram.Sum("units")).calculate(dfFull)
      assert(histogram.value.isSuccess)

      histogram.value.get match {
        case hv =>
          assert(hv.numberOfBins == 3)
          assert(hv.values.size == 3)
          assert(hv.values.keys == Set("Furniture", "Cosmetics", "Electronics"))
          assert(hv("Furniture").absolute == 55)
          assert(hv("Furniture").ratio == 55.0 / (55 + 20 + 60))
          assert(hv("Cosmetics").absolute == 20)
          assert(hv("Cosmetics").ratio == 20.0 / (55 + 20 + 60))
          assert(hv("Electronics").absolute == 60)
          assert(hv("Electronics").ratio == 60.0 / (55 + 20 + 60))

      }
    }

    "compute correct metrics on numeric values" in withSparkSession { sparkSession =>
      val dfFull = getDfWithNumericValues(sparkSession)
      val histogram = Histogram("att2").calculate(dfFull)
      assert(histogram.value.isSuccess)

      histogram.value.get match {
        case hv =>
          assert(hv.numberOfBins == 4)
          assert(hv.values.size == 4)
      }
    }

    "compute correct metrics after binning if provided" in withSparkSession { sparkSession =>
      val customBinner = udf {
        (cnt: String) =>
          cnt match {
            case "a" | "b" => "Value1"
            case _ => "Value2"
          }
      }
      val dfFull = getDfMissing(sparkSession)
      val histogram = Histogram("att1", Some(customBinner)).calculate(dfFull)

      assert(histogram.value.isSuccess)

      histogram.value.get match {
        case hv =>
          assert(hv.numberOfBins == 2)
          assert(hv.values.keys == Set("Value1", "Value2"))

      }
    }

    "compute correct metrics after splits if provided" in withSparkSession { sparkSession =>
      val dfFull = getDfWithNumericFractionalValues(sparkSession)
      val splits = Seq(
        Double.NegativeInfinity,
        3.0,
        5.0,
        Double.PositiveInfinity
      )
      val histogram = Histogram(
        "att1",
        splits = Some(splits)
      ).calculate(dfFull)

      assert(histogram.value.isSuccess)

      histogram.value.get match {
          case hv =>
            assert(hv.numberOfBins == 3)
            assert(hv.splits == Some(splits))
            assert(hv.values.keys == Set(
              "-Infinity <= x < 3.0",
              "3.0 <= x < 5.0",
              "5.0 <= x <= Infinity"
            ))
            assert(hv("-Infinity <= x < 3.0").absolute == 2)
            assert(hv("3.0 <= x < 5.0").absolute == 2)
            assert(hv("5.0 <= x <= Infinity").absolute == 2)
      }
    }

    "compute correct metrics should only get top N bins" in withSparkSession { sparkSession =>
      val dfFull = getDfMissing(sparkSession)
      val histogram = Histogram("att1", None, 2).calculate(dfFull)

      assert(histogram.value.isSuccess)

      histogram.value.get match {
        case hv =>
          assert(hv.numberOfBins == 3)
          assert(hv.values.size == 2)
          assert(hv.values.keys == Set("a", Histogram.NullFieldReplacement))

      }
    }

    "fail for max detail bins > 1000" in withSparkSession { sparkSession =>
      val df = getDfFull(sparkSession)
      Histogram("att1", binningUdf = None, maxDetailBins = 1001).calculate(df).value match {
        case Failure(t) => t.getMessage shouldBe "Cannot return " +
          "histogram values for more than 1000 values"
        case _ => fail("test was expected to fail due to parameter precondition")
      }
    }
  }

  "Data type analyzer" should {

    def distributionFrom(
        nonZeroValues: (DataTypeInstances.Value, DistributionValue)*)
      : Distribution = {

      val nonZeroValuesWithStringKeys = nonZeroValues.toSeq
        .map { case (instance, distValue) => instance.toString -> distValue }

      val dataTypes = DataTypeInstances.values.map { _.toString }

      val zeros = dataTypes
        .diff { nonZeroValuesWithStringKeys.map { case (distKey, _) => distKey }.toSet }
        .map(dataType => dataType -> DistributionValue(0, 0.0))
        .toSeq

      val distributionValues = Map(zeros ++ nonZeroValuesWithStringKeys: _*)

      Distribution(distributionValues, numberOfBins = dataTypes.size)
    }

    "fail for non-atomic columns" in withSparkSession { sparkSession =>
      val df = getDfWithNestedColumn(sparkSession)

      assert(DataType("source").calculate(df).value.isFailure)
    }

    "fall back to String in case no known data type matched" in withSparkSession { sparkSession =>
      val df = getDfFull(sparkSession)

      DataType("att1").calculate(df).value shouldBe
        Success(distributionFrom(DataTypeInstances.String -> DistributionValue(4, 1.0)))
    }

    "detect integral type correctly" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      val expectedResult = distributionFrom(DataTypeInstances.Integral -> DistributionValue(6, 1.0))
      DataType("att1").calculate(df).value shouldBe Success(expectedResult)
    }

    "detect integral type correctly for negative numbers" in withSparkSession { sparkSession =>
      val df = getDfWithNegativeNumbers(sparkSession)
      val expectedResult = distributionFrom(DataTypeInstances.Integral -> DistributionValue(4, 1.0))
      DataType("att1").calculate(df).value shouldBe Success(expectedResult)
    }

    "detect fractional type correctly for negative numbers" in withSparkSession { sparkSession =>
      val df = getDfWithNegativeNumbers(sparkSession)
      val expectedResult =
        distributionFrom(DataTypeInstances.Fractional -> DistributionValue(4, 1.0))
      DataType("att2").calculate(df).value shouldBe Success(expectedResult)
    }


    "detect fractional type correctly" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
        .withColumn("att1_float", col("att1").cast(FloatType))
      val expectedResult =
        distributionFrom(DataTypeInstances.Fractional -> DistributionValue(6, 1.0))
        DataType("att1_float").calculate(df).value shouldBe Success(expectedResult)
    }

    "detect integral type in string column" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
        .withColumn("att1_str", col("att1").cast(StringType))
      val expectedResult = distributionFrom(DataTypeInstances.Integral -> DistributionValue(6, 1.0))
      DataType("att1_str").calculate(df).value shouldBe Success(expectedResult)
    }

    "detect fractional type in string column" in withSparkSession { sparkSession =>
      val df = getDfWithNumericFractionalValues(sparkSession)
        .withColumn("att1_str", col("att1").cast(StringType))

      val expectedResult =
        distributionFrom(DataTypeInstances.Fractional -> DistributionValue(6, 1.0))
      DataType("att1_str").calculate(df).value shouldBe Success(expectedResult)
    }

    "fall back to string in case the string column didn't match " +
      " any known other data type" in withSparkSession { sparkSession =>
      val df = getDfFull(sparkSession)
      val expectedResult = distributionFrom(DataTypeInstances.String -> DistributionValue(4, 1.0))
      DataType("att1").calculate(df).value shouldBe Success(expectedResult)
    }

    "detect fractional for mixed fractional and integral" in withSparkSession { sparkSession =>
      val df = getDfFractionalIntegralTypes(sparkSession)
      DataType("att1").calculate(df).value shouldBe Success(
        distributionFrom(
          DataTypeInstances.Fractional -> DistributionValue(1, 0.5),
          DataTypeInstances.Integral -> DistributionValue(1, 0.5)
        )
      )
    }

    "fall back to string for mixed fractional and string" in withSparkSession { sparkSession =>
      val df = getDfFractionalStringTypes(sparkSession)
      DataType("att1").calculate(df).value shouldBe Success(
        distributionFrom(
          DataTypeInstances.Fractional -> DistributionValue(1, 0.5),
          DataTypeInstances.String -> DistributionValue(1, 0.5)
        )
      )
    }

    "fall back to string for mixed integral and string" in withSparkSession { sparkSession =>
      val df = getDfIntegralStringTypes(sparkSession)
      DataType("att1").calculate(df).value shouldBe Success(
        distributionFrom(
          DataTypeInstances.Integral -> DistributionValue(1, 0.5),
          DataTypeInstances.String -> DistributionValue(1, 0.5)
        )
      )
    }

    "integral for numeric and null" in withSparkSession { sparkSession =>
      val df = getDfWithUniqueColumns(sparkSession)
      DataType("uniqueWithNulls").calculate(df).value shouldBe Success(
        distributionFrom(
          DataTypeInstances.Unknown -> DistributionValue(1, 1.0/6.0),
          DataTypeInstances.Integral -> DistributionValue(5, 5.0/6.0)
        )
      )
    }

    "detect boolean type" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val df = Seq(
        ("1", "true"),
        ("2", "false"))
        .toDF("item", "att1")

      val expectedResult = distributionFrom(DataTypeInstances.Boolean -> DistributionValue(2, 1.0))

      DataType("att1").calculate(df).value shouldBe Success(expectedResult)
    }

    "fall back to string for boolean and null" in withSparkSession { sparkSession =>
      import sparkSession.implicits._
      val df = Seq(
        ("1", "true"),
        ("2", "false"),
        ("3", null),
        ("4", "2.0")
      ).toDF("item", "att1")

      DataType("att1").calculate(df).value shouldBe Success(
        distributionFrom(
          DataTypeInstances.Fractional -> DistributionValue(1, 0.25),
          DataTypeInstances.Unknown -> DistributionValue(1, 0.25),
          DataTypeInstances.Boolean -> DistributionValue(2, 0.5)
        )
      )
    }
  }

  "Basic statistics" should {
    "compute mean correctly for numeric data" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      val result = Mean("att1").calculate(df).value
      result shouldBe Success(3.5)
    }
    "fail to compute mean for non numeric type" in withSparkSession { sparkSession =>
      val df = getDfFull(sparkSession)
      assert(Mean("att1").calculate(df).value.isFailure)
    }
    "compute mean correctly for numeric data with where predicate" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)
        val result = Mean("att1", where = Some("item != '6'")).calculate(df).value
        result shouldBe Success(3.0)
      }

    "compute standard deviation correctly for numeric data" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      val result = StandardDeviation("att1").calculate(df).value
      result shouldBe Success(1.707825127659933)
    }
    "fail to compute standard deviaton for non numeric type" in withSparkSession { sparkSession =>
      val df = getDfFull(sparkSession)
      assert(StandardDeviation("att1").calculate(df).value.isFailure)
    }

    "compute minimum correctly for numeric data" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      val result = Minimum("att1").calculate(df)
      result.value shouldBe Success(1.0)
      assert(result.fullColumn.isDefined)
    }
    "fail to compute minimum for non numeric type" in withSparkSession { sparkSession =>
      val df = getDfFull(sparkSession)
      assert(Minimum("att1").calculate(df).value.isFailure)
    }

    "compute maximum correctly for numeric data" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      val result = Maximum("att1").calculate(df)
      result.value shouldBe Success(6.0)
      assert(result.fullColumn.isDefined)
    }

    "compute maximum correctly for numeric data with filtering" in
      withSparkSession { sparkSession =>
        val df = getDfWithNumericValues(sparkSession)
        val result = Maximum("att1", where = Some("item != '6'")).calculate(df)
        result.value shouldBe Success(5.0)
        assert(result.fullColumn.isDefined)
      }

    "fail to compute maximum for non numeric type" in withSparkSession { sparkSession =>
      val df = getDfFull(sparkSession)
      assert(Maximum("att1").calculate(df).value.isFailure)
    }

    "compute sum correctly for numeric data" in withSparkSession { session =>
      val df = getDfWithNumericValues(session)
      Sum("att1").calculate(df).value shouldBe Success(21)
    }

    "fail to compute sum for non numeric type" in withSparkSession { sparkSession =>
      val df = getDfFull(sparkSession)
      assert(Sum("att1").calculate(df).value.isFailure)
    }

    "should work correctly on decimal columns" in withSparkSession { session =>

      val schema =
        StructType(StructField(name = "num", dataType = DecimalType.SYSTEM_DEFAULT) :: Nil)

      val rows = session.sparkContext.parallelize(Seq(
        Row(BigDecimal(123.45)),
        Row(BigDecimal(99)),
        Row(BigDecimal(678))))

      val data = session.createDataFrame(rows, schema)

      val result = Minimum("num").calculate(data)

      assert(result.value.isSuccess)
      assert(result.value.get == 99.0)
    }

    "compute min length correctly for string data" in withSparkSession { sparkSession =>
      val df = getDfWithVariableStringLengthValues(sparkSession)
      val result = MinLength("att1").calculate(df).value
      result shouldBe Success(0.0)
    }

    "compute min length correctly for string data with filtering" in
      withSparkSession { sparkSession =>
        val df = getDfWithVariableStringLengthValues(sparkSession)
        val result = MinLength("att1", where = Some("att1 != ''")).calculate(df).value
        result shouldBe Success(1.0)
    }

    "fail to compute min length for non string type" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      assert(MinLength("att1").calculate(df).value.isFailure)
    }

    "compute max length correctly for string data" in withSparkSession { sparkSession =>
      val df = getDfWithVariableStringLengthValues(sparkSession)
      val result = MaxLength("att1").calculate(df).value
      result shouldBe Success(4.0)
    }

    "compute max length correctly for string data with filtering" in
      withSparkSession { sparkSession =>
        val df = getDfWithVariableStringLengthValues(sparkSession)
        val result = MaxLength("att1", where = Some("att1 != 'dddd'")).calculate(df).value
        result shouldBe Success(3.0)
    }

    "fail to compute max length for non string type" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      assert(MaxLength("att1").calculate(df).value.isFailure)
    }
  }

  "Count distinct analyzers" should {
    "compute approximate distinct count for numeric data" in withSparkSession { sparkSession =>
      val df = getDfWithUniqueColumns(sparkSession)
      val result = ApproxCountDistinct("uniqueWithNulls").calculate(df).value

      result shouldBe Success(5.0)
    }

    "compute approximate distinct count for numeric data with filtering" in
      withSparkSession { sparkSession =>

        val df = getDfWithUniqueColumns(sparkSession)
        val result = ApproxCountDistinct("uniqueWithNulls", where = Some("unique < 4"))
          .calculate(df).value
        result shouldBe Success(2.0)
      }

    "compute exact distinct count of elements for numeric data" in withSparkSession {
      sparkSession =>
        val df = getDfWithUniqueColumns(sparkSession)
        val result = CountDistinct("uniqueWithNulls").calculate(df).value
        result shouldBe Success(5.0)
      }
  }

  "Approximate quantile analyzer" should {

    "approximate quantile 0.5 within acceptable error bound" in
      withSparkSession { sparkSession =>

        import sparkSession.implicits._
        val df = sparkSession.sparkContext.range(-1000L, 1000L).toDF("att1")

        val result = ApproxQuantile("att1", 0.5).calculate(df).value.get

        assert(result > -20 && result < 20)
      }

    "approximate quantile 0.25 within acceptable error bound" in
      withSparkSession { sparkSession =>

        import sparkSession.implicits._
        val df = sparkSession.sparkContext.range(-1000L, 1000L).toDF("att1")

        val result = ApproxQuantile("att1", 0.25).calculate(df).value.get

        assert(result > -520 && result < -480)
      }

    "approximate quantile 0.75 within acceptable error bound" in
      withSparkSession { sparkSession =>

        import sparkSession.implicits._
        val df = sparkSession.sparkContext.range(-1000L, 1000L).toDF("att1")

        val result = ApproxQuantile("att1", 0.75).calculate(df).value.get

        assert(result > 480 && result < 520)
      }

    "fail for relative error > 1.0" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      ApproxQuantile("att1", quantile = 0.5, relativeError = 1.1).calculate(df).value match {
        case Failure(t) => t.getMessage shouldBe "Relative error parameter must " +
          "be in the closed interval [0, 1]. Currently, the value is: 1.1!"
        case _ => fail(AnalyzerTests.expectedPreconditionViolation)
      }
    }
    "fail for relative error < 0.0" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      ApproxQuantile("att1", quantile = 0.5, relativeError = -0.1).calculate(df).value match {
        case Failure(t) => t.getMessage shouldBe "Relative error parameter must " +
          "be in the closed interval [0, 1]. Currently, the value is: -0.1!"
        case _ => fail(AnalyzerTests.expectedPreconditionViolation)
      }
    }
    "fail for quantile < 0.0" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      ApproxQuantile("att1", quantile = -0.1).calculate(df).value match {
        case Failure(t) => t.getMessage shouldBe "Quantile parameter must " +
          "be in the closed interval [0, 1]. Currently, the value is: -0.1!"
        case _ => fail(AnalyzerTests.expectedPreconditionViolation)

      }
    }
    "fail for quantile > 1.0" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      ApproxQuantile("att1", quantile = 1.1).calculate(df).value match {
        case Failure(t) => t.getMessage shouldBe "Quantile parameter must be " +
          "in the closed interval [0, 1]. Currently, the value is: 1.1!"
        case _ => fail(AnalyzerTests.expectedPreconditionViolation)
      }
    }
  }

  "Pearson correlation" should {
    "yield NaN for conditionally uninformative columns" in withSparkSession { sparkSession =>
      val df = getDfWithConditionallyUninformativeColumns(sparkSession)
      val corr = Correlation("att1", "att2").calculate(df).value.get
      assert(java.lang.Double.isNaN(corr))
    }
    "yield 1.0 for maximal conditionally informative columns" in withSparkSession { sparkSession =>
      val df = getDfWithConditionallyInformativeColumns(sparkSession)
      Correlation("att1", "att2").calculate(df) shouldBe DoubleMetric(
        Entity.Multicolumn,
        "Correlation",
        "att1,att2",
        Success(1.0)
      )
    }
    "is commutative" in withSparkSession { sparkSession =>
      val df = getDfWithConditionallyInformativeColumns(sparkSession)
      Correlation("att1", "att2").calculate(df).value shouldBe
        Correlation("att2", "att1").calculate(df).value
    }
  }


  "Pattern compliance analyzer" should {
    val someColumnName = "some"

    "PatternMatch hashCode should equal for the same pattern" in {
      val p1 = PatternMatch("col1", "[a-z]".r)
      val p2 = PatternMatch("col1", "[a-z]".r)
      p1.hashCode() should equal(p2.hashCode())
      p1 should equal(p2)
    }

    "not match doubles in nullable column" in withSparkSession { sparkSession =>

       val df = dataFrameWithColumn(someColumnName, DoubleType, sparkSession, Row(1.1),
          Row(null), Row(3.2), Row(4.4))

      val result: DoubleMetric = PatternMatch(someColumnName, """\d\.\d""".r).calculate(df)

      assert(result.value.isFailure)
    }

    "match integers in a String column" in withSparkSession { sparkSession =>
      val df = dataFrameWithColumn(someColumnName, StringType, sparkSession, Row("1"), Row("a"))
      val result: DoubleMetric = PatternMatch(someColumnName, """\d""".r).calculate(df)
      result.value shouldBe Success(0.5)
      assert(result.fullColumn.isDefined)
    }

    "match email addresses" in withSparkSession { sparkSession =>
      val df = dataFrameWithColumn(someColumnName, StringType, sparkSession,
        Row("someone@somewhere.org"), Row("someone@else"))
      val result: DoubleMetric = PatternMatch(someColumnName, Patterns.EMAIL).calculate(df)
      result.value shouldBe Success(0.5)
      assert(result.fullColumn.isDefined)
    }

    "match credit card numbers" in withSparkSession { sparkSession =>
      // https://www.paypalobjects.com/en_AU/vhelp/paypalmanager_help/credit_card_numbers.htm
      val maybeCreditCardNumbers = Seq(
        "378282246310005",// AMEX

        "6011111111111117", // Discover
        "6011 1111 1111 1117", // Discover spaced
        "6011-1111-1111-1117", // Discover dashed

        "5555555555554444", // MasterCard
        "5555 5555 5555 4444", // MasterCard spaced
        "5555-5555-5555-4444", // MasterCard dashed

        "4111111111111111", // Visa
        "4111 1111 1111 1111", // Visa spaced
        "4111-1111-1111-1111", // Visa dashed

        "0000111122223333", // not really a CC number
        "000011112222333",  // not really a CC number
        "00001111222233"    // not really a CC number
      )
      val df = dataFrameWithColumn(someColumnName, StringType, sparkSession,
        maybeCreditCardNumbers.map(Row(_)): _*)
      val analyzer = PatternMatch(someColumnName, Patterns.CREDITCARD)

      analyzer.calculate(df).value shouldBe Success(10.0/13.0)
      assert(analyzer.calculate(df).fullColumn.isDefined)
    }

    "match URLs" in withSparkSession { sparkSession =>
      // URLs taken from https://mathiasbynens.be/demo/url-regex
      val maybeURLs = Seq(
        "http://foo.com/blah_blah",
        "http://foo.com/blah_blah_(wikipedia)",
        "http://foo.bar/?q=Test%20URL-encoded%20stuff",

        // scalastyle:off
        "http://➡.ws/䨹",
        "http://⌘.ws/",
        "http://☺.damowmow.com/",
        "http://例子.测试",
        // scalastyle:on

        "https://foo_bar.example.com/",
        "http://userid@example.com:8080",
        "http://foo.com/blah_(wikipedia)#cite-1",

        "http://../", // not really a valid URL
        "h://test",  // not really a valid URL
        "http://.www.foo.bar/"    // not really a valid URL
      )
      val df = dataFrameWithColumn(someColumnName, StringType, sparkSession,
        maybeURLs.map(Row(_)): _*)
      val analyzer = PatternMatch(someColumnName, Patterns.URL)
      analyzer.calculate(df).value shouldBe Success(10.0/13.0)
      assert(analyzer.calculate(df).fullColumn.isDefined)
    }

    "match US social security numbers" in withSparkSession { sparkSession =>
      // https://en.wikipedia.org/wiki/Social_Security_number#Valid_SSNs
      val maybeSSN = Seq(
        "111-05-1130",
        "111051130", // without dashes
        "111-05-000", // no all-zeros allowed in any group
        "111-00-000", // no all-zeros allowed in any group
        "000-05-1130", // no all-zeros allowed in any group
        "666-05-1130", // 666 and 900-999 forbidden in first digit group
        "900-05-1130", // 666 and 900-999 forbidden in first digit group
        "999-05-1130" // 666 and 900-999 forbidden in first digit group
      )
      val df = dataFrameWithColumn(someColumnName, StringType, sparkSession,
        maybeSSN.map(Row(_)): _*)
      val analyzer = PatternMatch(someColumnName, Patterns.SOCIAL_SECURITY_NUMBER_US)
      analyzer.calculate(df).value shouldBe Success(2.0 / 8.0)
      assert(analyzer.calculate(df).fullColumn.isDefined)
    }

    "compute ratio of sums correctly for numeric data" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      RatioOfSums("att1", "att2").calculate(df).value shouldBe Success(21.0 / 18.0)
    }

    "fail to compute ratio of sums for non numeric type" in withSparkSession { sparkSession =>
      val df = getDfFull(sparkSession)
      assert(RatioOfSums("att1", "att2").calculate(df).value.isFailure)
    }

    "divide by zero" in withSparkSession { sparkSession =>
      val df = getDfWithNumericValues(sparkSession)
      val testVal = RatioOfSums("att1", "att2", Some("item IN ('1', '2')")).calculate(df)
      assert(testVal.value.isSuccess)
      assert(testVal.value.toOption.get.isInfinite)
    }
  }
}

object AnalyzerTests {
  val expectedPreconditionViolation: String =
    "computation was unexpectedly successful, should have failed due to violated precondition"
}
