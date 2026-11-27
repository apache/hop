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

package org.apache.hop.databricks.deploy;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.cluster.SparkClusterTarget;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databricks.metadata.DatabricksConnection;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DatabricksSparkClusterTargetExtensionPointTest {

  private MemoryMetadataProvider metadataProvider;
  private final DatabricksSparkClusterTargetExtensionPoint extensionPoint =
      new DatabricksSparkClusterTargetExtensionPoint();

  @BeforeAll
  static void init() throws Exception {
    // The access token of the connection is decoded with Hop's password encoder
    HopClientEnvironment.init();
  }

  @BeforeEach
  void setUp() throws Exception {
    metadataProvider = new MemoryMetadataProvider();
    DatabricksConnection connection = new DatabricksConnection();
    connection.setName("workspace");
    connection.setHost("https://adb-1.azuredatabricks.net");
    connection.setToken("dapi-test");
    metadataProvider.getSerializer(DatabricksConnection.class).save(connection);
  }

  private SparkClusterTarget target(String clusterType, Map<String, String> settings) {
    return new SparkClusterTarget(
        LogChannel.GENERAL, new Variables(), metadataProvider, clusterType, settings);
  }

  private static Map<String, String> settings(String compute) {
    Map<String, String> settings = new HashMap<>();
    settings.put(SparkClusterTarget.SETTING_CONNECTION, "workspace");
    settings.put(SparkClusterTarget.SETTING_COMPUTE, compute);
    settings.put(SparkClusterTarget.SETTING_STAGING_FOLDER, "/Volumes/main/default/hop/staging/");
    return settings;
  }

  @Test
  void otherClusterTypesAreLeftAlone() throws Exception {
    SparkClusterTarget target = target("YARN", settings("1126-082508-x0e8qaoz"));
    extensionPoint.callExtensionPoint(LogChannel.GENERAL, new Variables(), target);
    assertNull(target.getSubmitter());
    assertNull(target.getStagingStore());
  }

  @Test
  void aDatabricksTargetGetsTheJobRunsSubmitterAndTheVolume() throws Exception {
    SparkClusterTarget target = target("DATABRICKS", settings("1126-082508-x0e8qaoz"));
    extensionPoint.callExtensionPoint(LogChannel.GENERAL, new Variables(), target);

    DatabricksRunSubmitter submitter =
        assertInstanceOf(DatabricksRunSubmitter.class, target.getSubmitter());
    assertTrue(submitter.describe().contains("cluster 1126-082508-x0e8qaoz"), submitter.describe());
    assertEquals(
        "/Volumes/main/default/hop/staging/hop-native-1.jar",
        target.getStagingStore().clusterUrl("hop-native-1.jar"));
    submitter.close();
  }

  @Test
  void incompleteOrUnsupportedSettingsSayWhatIsWrong() {
    Map<String, String> noConnection = settings("1126-082508-x0e8qaoz");
    noConnection.remove(SparkClusterTarget.SETTING_CONNECTION);
    HopException missing =
        assertThrows(
            HopException.class,
            () ->
                extensionPoint.callExtensionPoint(
                    LogChannel.GENERAL, new Variables(), target("DATABRICKS", noConnection)));
    assertTrue(missing.getMessage().contains("Databricks connection"), missing.getMessage());

    Map<String, String> unknown = settings("1126-082508-x0e8qaoz");
    unknown.put(SparkClusterTarget.SETTING_CONNECTION, "nope");
    HopException notFound =
        assertThrows(
            HopException.class,
            () ->
                extensionPoint.callExtensionPoint(
                    LogChannel.GENERAL, new Variables(), target("DATABRICKS", unknown)));
    assertTrue(notFound.getMessage().contains("'nope' does not exist"), notFound.getMessage());

    HopException serverless =
        assertThrows(
            HopException.class,
            () ->
                extensionPoint.callExtensionPoint(
                    LogChannel.GENERAL,
                    new Variables(),
                    target("DATABRICKS", settings("serverless"))));
    assertTrue(serverless.getMessage().contains("SparkContext"), serverless.getMessage());
  }
}
