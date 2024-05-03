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

package com.amazon.deequ.metrics

import com.amazon.deequ.analyzers.DataTypeInstances
import org.scalatest.{Matchers, WordSpec}

import scala.util.{Failure, Success}


class MetricsTests extends WordSpec with Matchers {
  val sampleException = new IllegalArgumentException()
  "Double metric" should {
    "flatten and return itself" in {
      val metric = DoubleMetric(Entity.Column, "metric-name", "instance-name", Success(50))
      assert(metric.flatten() == List(metric))
    }

    "flatten in case of an error" in {
      val metric = DoubleMetric(Entity.Column, "metric-name", "instance-name",
        Failure(sampleException))
      assert(metric.flatten() == List(metric))
    }
  }

  "Histogram metric" should {
    "flatten matched and unmatched" in {
      val distribution = Distribution(
        Map("a" -> DistributionValue(6, 0.6), "b" -> DistributionValue(4, 0.4)), 2)

      val metric = HistogramMetric("instance-name", Success(distribution))

      val expected = Seq(
        DoubleMetric(Entity.Column, "Histogram.bins", "instance-name", Success(2)),
        DoubleMetric(Entity.Column, "Histogram.abs.a", "instance-name", Success(6)),
        DoubleMetric(Entity.Column, "Histogram.abs.b", "instance-name", Success(4)),
        DoubleMetric(Entity.Column, "Histogram.ratio.a", "instance-name", Success(0.6)),
        DoubleMetric(Entity.Column, "Histogram.ratio.b", "instance-name", Success(0.4))
      ).toSet
      assert(metric.flatten().toSet == expected)
    }

    "flatten matched and unmatched in case of an error" in {
      val metric = HistogramMetric("instance-name", Failure(sampleException))

      val expected = Seq(DoubleMetric(Entity.Column, "Histogram.bins", "instance-name",
        Failure(sampleException))).toSet
      assert(metric.flatten().toSet == expected)
    }

    "distribution correctly converts to bucket values" in {
        val distribution = Distribution(
          Map(
            "-Infinity <= x < 3.0" -> DistributionValue(1, 0.1),
            "3.0 <= x < 5.0" -> DistributionValue(5, 0.5),
            "5.0 <= x <= Infinity" -> DistributionValue(4, 0.4)
          ),
          3,
          splits = Some(Seq(
            Double.NegativeInfinity,
            3.0,
            5.0,
            Double.PositiveInfinity
          ))
        )

        val expectedBucketValues = Seq(
          BucketValue(lowValue = Double.NegativeInfinity, highValue = 3.0, count = 1),
          BucketValue(lowValue = 3.0, highValue = 5.0, count = 5),
          BucketValue(lowValue = 5.0, highValue = Double.PositiveInfinity, count = 4)
        )

        assert(distribution.toBucketValues == expectedBucketValues)
    }
  }

  "KLL metric" should {
      "bucket distribution correctly produces splits" in {
          val bucketDist = BucketDistribution(
            buckets = List(
              BucketValue(lowValue = 0.0, highValue = 3.0, count = 1),
              BucketValue(lowValue = 3.0, highValue = 5.0, count = 5),
              BucketValue(lowValue = 5.0, highValue = 9.0, count = 4)
            ),
            parameters = List(), // Not important for this test
            data = Array(Array()) // Not important for this test
          )

          val expectedSplitsBounded = Seq(
            0.0,
            3.0,
            5.0,
            9.0
          )
          val expectedSplitsUnbounded = Seq(
            Double.NegativeInfinity,
            3.0,
            5.0,
            Double.PositiveInfinity
          )

          assert(bucketDist.getSplits() == expectedSplitsBounded)
          assert(bucketDist.getSplits(unboundedEdges = true) == expectedSplitsUnbounded)
      }
  }

}
