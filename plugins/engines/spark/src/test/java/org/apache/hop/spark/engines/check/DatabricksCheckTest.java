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

package org.apache.hop.spark.engines.check;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.apache.hop.spark.engines.SparkClusterType;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The settings of the Databricks cluster type, without a workspace to reach. */
class DatabricksCheckTest {

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  private static SparkPipelineRunConfiguration databricks(String compute, String folder) {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    config.setClusterType(SparkClusterType.DATABRICKS);
    config.setSparkMaster("");
    config.setDatabricksConnection("workspace");
    config.setDatabricksCompute(compute);
    config.setStagingFolder(folder);
    return config;
  }

  private static ICheckResult run(SparkPipelineRunConfiguration config, boolean withMetadata) {
    return new DatabricksCheck()
        .run(
            new SparkCheckContext(
                config,
                new Variables(),
                withMetadata ? new MemoryMetadataProvider() : null,
                null,
                false));
  }

  @Test
  void itOnlyAppliesToDatabricksAndAnEmptyMasterIsNoMismatch() {
    SparkPipelineRunConfiguration config = databricks("1126-0825-x0e8", "/Volumes/c/s/v/hop");
    SparkCheckContext context = new SparkCheckContext(config, new Variables());
    assertTrue(new DatabricksCheck().appliesTo(context));
    assertFalse(context.isClusterMode(), "no Hop VFS staging checks on a volume path");
    assertTrue(context.isOnACluster());
    assertEquals(
        ICheckResult.TYPE_RESULT_OK, new ClusterTypeCheck().run(context).getType(), "cluster type");

    config.setClusterType(SparkClusterType.YARN);
    config.setSparkMaster("yarn");
    assertFalse(new DatabricksCheck().appliesTo(new SparkCheckContext(config, new Variables())));
  }

  @Test
  void missingSettingsAreNamed() {
    ICheckResult result = run(databricks("", ""), false);
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, result.getType());
    assertTrue(result.getText().contains("the compute"), result.getText());
    assertTrue(result.getText().contains("the staging folder"), result.getText());
  }

  @Test
  void serverlessAndANonVolumeFolderAreFlagged() {
    ICheckResult serverless = run(databricks("serverless", "/Volumes/c/s/v/hop"), false);
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, serverless.getType());
    assertTrue(serverless.getText().contains("SparkContext"), serverless.getText());

    ICheckResult notAVolume = run(databricks("1126-0825-x0e8", "/tmp/hop"), false);
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, notAVolume.getType());
  }

  @Test
  void withoutTheDatabricksPluginTheCheckSaysSo() {
    // No plugin answers the extension point in this test: as when hop-tech-databricks is missing
    ICheckResult result = run(databricks("1126-0825-x0e8", "/Volumes/c/s/v/hop"), true);
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, result.getType());
    assertTrue(result.getText().contains("Databricks plugin"), result.getText());
  }
}
