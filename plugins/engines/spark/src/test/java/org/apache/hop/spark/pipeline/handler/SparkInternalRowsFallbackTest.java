/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.spark.pipeline.handler;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.List;
import org.apache.hop.core.HopEnvironment;
import org.apache.hop.core.logging.HopLogStore;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.RowFactory;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.types.DataTypes;
import org.apache.spark.sql.types.StructType;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * When Spark's internal row API fails on this Spark build, generic transforms fall back to Spark's
 * regular rows instead of failing the run.
 */
class SparkInternalRowsFallbackTest {

  private static SparkSession spark;

  @BeforeAll
  static void startSpark() throws Exception {
    HopEnvironment.init();
    HopLogStore.init();
    spark =
        SparkSession.builder()
            .appName("hop-spark-internal-rows-fallback-test")
            .master("local[1]")
            .config("spark.ui.enabled", "false")
            .config("spark.ui.showConsoleProgress", "false")
            .config("spark.driver.host", "localhost")
            .getOrCreate();
  }

  @AfterAll
  static void stopSpark() {
    if (spark != null) {
      spark.stop();
    }
  }

  @AfterEach
  void tryAgainNextTime() {
    SparkGenericTransformHandler.resetInternalRowsAvailability();
  }

  @Test
  void aFailingInternalRowApiGivesNoDatasetAndIsNotTriedAgain() {
    StructType schema = new StructType().add("n", DataTypes.LongType);
    Dataset<Row> source = spark.createDataFrame(List.of(RowFactory.create(1L)), schema);

    // No map function: setting up the internal row path fails, as a missing Spark method would
    assertNull(
        SparkGenericTransformHandler.withInternalRows(
            LogChannel.GENERAL, spark, source, null, schema));
    // Remembered: the next transform doesn't try again (and would not reach the null function)
    assertNull(
        SparkGenericTransformHandler.withInternalRows(
            LogChannel.GENERAL, spark, source, null, schema));
  }

  @Test
  void theInternalRowConversionIsOnByDefaultAndCanBeCleared() {
    SparkPipelineRunConfiguration runConfiguration = new SparkPipelineRunConfiguration();
    assertTrue(runConfiguration.isInternalRowConversion());
    runConfiguration.setInternalRowConversion(false);
    assertFalse(new SparkPipelineRunConfiguration(runConfiguration).isInternalRowConversion());
  }
}
