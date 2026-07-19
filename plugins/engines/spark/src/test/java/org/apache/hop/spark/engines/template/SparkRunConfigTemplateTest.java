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

package org.apache.hop.spark.engines.template;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.junit.jupiter.api.Test;

class SparkRunConfigTemplateTest {

  @Test
  void localExecutionSetsMasterAndUiOff() {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    SparkRunConfigTemplate.LOCAL_EXECUTION.applyTo(config);
    assertEquals("local[*]", config.getSparkMaster());
    assertTrue(config.getSparkConfigs().contains("spark.ui.enabled=false"));
    assertEquals("", config.getFatJar());
  }

  @Test
  void sparkSubmitLeavesMasterBlank() {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    SparkRunConfigTemplate.SPARK_SUBMIT.applyTo(config);
    assertEquals("", config.getSparkMaster());
  }

  @Test
  void standaloneSetsServerMasterAndMemory() {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    SparkRunConfigTemplate.SPARK_STANDALONE.applyTo(config);
    assertEquals("spark://host:7077", config.getSparkMaster());
    assertEquals("2g", config.getDriverMemory());
    assertEquals("2g", config.getExecutorMemory());
    assertEquals("2", config.getExecutorCores());
  }

  @Test
  void yarnClientSetsMasterAndDeployMode() {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    SparkRunConfigTemplate.YARN_CLIENT.applyTo(config);
    assertEquals("yarn", config.getSparkMaster());
    assertTrue(config.getSparkConfigs().contains("spark.submit.deployMode=client"));
  }

  @Test
  void fromDisplayNameRoundTrip() {
    for (SparkRunConfigTemplate t : SparkRunConfigTemplate.values()) {
      assertEquals(t, SparkRunConfigTemplate.fromDisplayName(t.getDisplayName()));
    }
    assertEquals(
        SparkRunConfigTemplate.values().length, SparkRunConfigTemplate.displayNames().length);
  }

  @Test
  void looksCustomizedDetectsEdits() {
    SparkPipelineRunConfiguration fresh = new SparkPipelineRunConfiguration();
    assertFalse(SparkRunConfigTemplate.looksCustomized(fresh));

    SparkPipelineRunConfiguration edited = new SparkPipelineRunConfiguration();
    edited.setSparkMaster("yarn");
    assertTrue(SparkRunConfigTemplate.looksCustomized(edited));

    SparkPipelineRunConfiguration withConfigs = new SparkPipelineRunConfiguration();
    withConfigs.setSparkConfigs("spark.ui.enabled=false");
    assertTrue(SparkRunConfigTemplate.looksCustomized(withConfigs));
  }

  @Test
  void everyTemplateAppliesWithoutNpe() {
    for (SparkRunConfigTemplate t : SparkRunConfigTemplate.values()) {
      SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
      t.applyTo(config);
      assertNotNull(config.getSparkMaster());
      assertNotNull(t.getDescription());
    }
  }
}
