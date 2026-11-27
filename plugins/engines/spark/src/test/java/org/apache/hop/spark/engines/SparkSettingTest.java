/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.spark.engines;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import org.junit.jupiter.api.Test;

/** A row of the Spark settings next to what Hop sets itself. */
class SparkSettingTest {

  @Test
  void aListRowAddsToWhatHopSets() {
    assertEquals(
        "s3a://staging/hop.jar,s3a://libs/pg.jar",
        SparkSetting.merge("spark.jars", "s3a://staging/hop.jar", "s3a://libs/pg.jar"));
    assertEquals(
        "/mnt/hop/hop.jar:/opt/drivers/pg.jar",
        SparkSetting.merge(
            "spark.executor.extraClassPath", "/mnt/hop/hop.jar", "/opt/drivers/pg.jar"));
    // Already in the row: once
    assertEquals(
        "s3a://staging/hop.jar,s3a://libs/pg.jar",
        SparkSetting.merge(
            "spark.jars", "s3a://staging/hop.jar", "s3a://libs/pg.jar, s3a://staging/hop.jar"));
  }

  @Test
  void anyOtherRowReplacesWhatHopSets() {
    assertEquals("4g", SparkSetting.merge("spark.driver.memory", "1g", "4g"));
    assertEquals("x.jar", SparkSetting.merge("spark.jars", null, "x.jar"));
    assertEquals("hop.jar", SparkSetting.merge("spark.jars", "hop.jar", ""));
  }

  @Test
  void onlyTheGivenEntryLeavesAList() {
    assertEquals(
        "s3a://libs/pg.jar",
        SparkSetting.without(
            "spark.jars", "s3a://staging/hop.jar,s3a://libs/pg.jar", "s3a://staging/hop.jar"));
    assertNull(
        SparkSetting.without("spark.jars", "s3a://staging/hop.jar", "s3a://staging/hop.jar"));
    assertNull(SparkSetting.without("spark.jars", null, "s3a://staging/hop.jar"));
  }
}
