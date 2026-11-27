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

package org.apache.hop.spark.engines;

import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.spark.util.SparkConst;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * In the driver that MainSpark runs, the engine runs the pipeline on the driver's session: a run
 * configuration of the Databricks type, for example, is not submitted to Databricks once more.
 */
class SparkPipelineEngineDriverMarkerTest {

  @AfterEach
  void clearMarker() {
    System.clearProperty(SparkConst.MAIN_SPARK_DRIVER_PROPERTY);
  }

  @Test
  void theMainSparkMarkerMeansTheDriverRunsThePipeline() {
    System.setProperty(SparkConst.MAIN_SPARK_DRIVER_PROPERTY, "true");
    assertTrue(SparkPipelineEngine.isRunningUnderSparkSubmit());
  }
}
