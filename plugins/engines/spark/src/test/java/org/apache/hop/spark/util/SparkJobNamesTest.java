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

package org.apache.hop.spark.util;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.CopyOnWriteArrayList;
import org.apache.spark.scheduler.SparkListener;
import org.apache.spark.scheduler.SparkListenerJobStart;
import org.apache.spark.sql.SparkSession;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The jobs of a pipeline carry its name in the Spark UI instead of a line of Hop code. */
class SparkJobNamesTest {

  private static SparkSession spark;
  private static final List<Properties> JOBS = new CopyOnWriteArrayList<>();

  @BeforeAll
  static void startSpark() {
    spark =
        SparkSession.builder()
            .master("local[1]")
            .appName("SparkJobNamesTest")
            .config("spark.ui.enabled", "false")
            .getOrCreate();
    spark
        .sparkContext()
        .addSparkListener(
            new SparkListener() {
              @Override
              public void onJobStart(SparkListenerJobStart jobStart) {
                JOBS.add(jobStart.properties());
              }
            });
  }

  @AfterAll
  static void stopSpark() {
    spark.stop();
  }

  private static Properties runJob() throws Exception {
    JOBS.clear();
    spark.range(10).count();
    // The listener bus delivers asynchronously
    for (int i = 0; i < 100 && JOBS.isEmpty(); i++) {
      Thread.sleep(50);
    }
    assertFalse(JOBS.isEmpty(), "a job ran");
    return JOBS.get(0);
  }

  @Test
  void theJobsOfAPipelineCarryItsNameAndTheTransform() throws Exception {
    SparkJobNames.forPipeline(spark, SparkJobNames.groupId("1a2b"), "0002-group-by");
    SparkJobNames.forTransform(spark, "0002-group-by", "Spark File Output", "SparkFileOutput");
    Properties job = runJob();
    assertEquals("hop-1a2b", job.getProperty("spark.jobGroup.id"));
    assertEquals(
        "Hop pipeline '0002-group-by', transform 'Spark File Output' (SparkFileOutput)",
        job.getProperty("spark.job.description"));
    assertEquals("Hop 0002-group-by → Spark File Output", job.getProperty("callSite.short"));

    SparkJobNames.clear(spark);
    Properties after = runJob();
    assertNull(after.getProperty("spark.jobGroup.id"), "later jobs of the thread are not named");
    assertNull(after.getProperty("spark.job.description"));
  }
}
