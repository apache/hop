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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import java.util.Set;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.json.JsonMetadataProvider;
import org.apache.hop.pipeline.config.PipelineRunConfiguration;
import org.apache.hop.pipeline.engine.PipelineEnginePlugin;
import org.apache.hop.pipeline.engine.PipelineEnginePluginType;
import org.apache.hop.spark.engines.template.SparkRunConfigTemplate;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/** The cluster type of a native Spark run configuration and the options it hides. */
class SparkClusterTypeTest {

  @BeforeAll
  static void registerTheEngine() throws Exception {
    HopClientEnvironment.init();
    PluginRegistry.getInstance()
        .registerPluginClass(
            SparkPipelineEngine.class.getName(),
            PipelineEnginePluginType.class,
            PipelineEnginePlugin.class);
  }

  @Test
  void theMasterSaysWhereItRuns() {
    assertEquals(SparkClusterType.LOCAL, SparkClusterType.fromMaster("local[*]"));
    assertEquals(SparkClusterType.LOCAL, SparkClusterType.fromMaster("local[4]"));
    assertEquals(SparkClusterType.STANDALONE, SparkClusterType.fromMaster("spark://spark:7077"));
    assertEquals(SparkClusterType.YARN, SparkClusterType.fromMaster("yarn"));
    assertEquals(SparkClusterType.KUBERNETES, SparkClusterType.fromMaster("k8s://https://k:6443"));
    assertEquals(SparkClusterType.SUBMITTED, SparkClusterType.fromMaster(""));
    assertEquals(SparkClusterType.SUBMITTED, SparkClusterType.fromMaster(null));
    assertNull(SparkClusterType.fromMaster("${SPARK_MASTER}"), "a variable can't be told");
    for (SparkClusterType type : SparkClusterType.values()) {
      assertTrue(type.matchesMaster(type.getMasterTemplate()), type + " template");
    }
  }

  /**
   * Local and spark-submit have no deploy mode: the editor hides it, but keeps a cluster deploy
   * mode chosen for another type. It must not send the run to cluster submission.
   */
  @Test
  void aHiddenDeployModeDoesNotSubmitTheDriver() {
    assertFalse(SparkClusterType.submitsTheDriver(SparkClusterType.LOCAL, SparkDeployMode.CLUSTER));
    assertFalse(
        SparkClusterType.submitsTheDriver(SparkClusterType.SUBMITTED, SparkDeployMode.CLUSTER));
    assertTrue(
        SparkClusterType.submitsTheDriver(SparkClusterType.STANDALONE, SparkDeployMode.CLUSTER));
    assertTrue(SparkClusterType.submitsTheDriver(SparkClusterType.YARN, SparkDeployMode.CLUSTER));
    assertTrue(
        SparkClusterType.submitsTheDriver(SparkClusterType.KUBERNETES, SparkDeployMode.CLUSTER));
    assertFalse(SparkClusterType.submitsTheDriver(SparkClusterType.YARN, SparkDeployMode.CLIENT));
    assertFalse(SparkClusterType.submitsTheDriver(SparkClusterType.YARN, null), "client mode");
    assertTrue(SparkClusterType.submitsTheDriver(SparkClusterType.DATABRICKS, null));
    assertTrue(SparkClusterType.submitsTheDriver(null, SparkDeployMode.CLUSTER), "can't tell");

    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    config.setDeployMode(SparkDeployMode.CLUSTER);
    config.setClusterType(SparkClusterType.LOCAL);
    config.setSparkMaster("local[*]");
    assertFalse(SparkPipelineEngine.submitsTheDriver(config, new Variables()));
    config.setClusterType(SparkClusterType.STANDALONE);
    config.setSparkMaster("spark://spark:7077");
    assertTrue(SparkPipelineEngine.submitsTheDriver(config, new Variables()));

    // No type and the master in a variable: the resolved master says
    config.setClusterType(null);
    config.setSparkMaster("${SPARK_MASTER}");
    Variables variables = new Variables();
    variables.setVariable("SPARK_MASTER", "local[4]");
    assertFalse(SparkPipelineEngine.submitsTheDriver(config, variables));
    variables.setVariable("SPARK_MASTER", "spark://spark:7077");
    assertTrue(SparkPipelineEngine.submitsTheDriver(config, variables));
  }

  /**
   * An older configuration with an empty master ran locally from Hop. Opening it in the editor must
   * not make it the type spark-submit, which refuses to run from Hop once it is saved.
   */
  @Test
  void anEmptyMasterLeavesTheTypeOfAnOlderConfigurationUnset() {
    assertNull(SparkRunConfigurationWidgetsListener.typeOfAnOlderConfiguration(""));
    assertNull(SparkRunConfigurationWidgetsListener.typeOfAnOlderConfiguration("  "));
    assertNull(SparkRunConfigurationWidgetsListener.typeOfAnOlderConfiguration(null));
    assertEquals(
        SparkClusterType.YARN,
        SparkRunConfigurationWidgetsListener.typeOfAnOlderConfiguration("yarn"));
    assertEquals(
        SparkClusterType.LOCAL,
        SparkRunConfigurationWidgetsListener.typeOfAnOlderConfiguration("local[*]"));
    // Unset, it hides nothing
    assertTrue(SparkRunConfigurationWidgetsListener.hiddenOptions(null, false).isEmpty());
  }

  /** A run configuration saved before the option loads as it was, its type from the master. */
  @Test
  void aConfigurationSavedWithoutATypeTakesItFromTheMaster() throws Exception {
    PipelineRunConfiguration old =
        load(
            "{\"engineRunConfiguration\":{\"SparkPipelineEngine\":{\"sparkMaster\":\"yarn\"}},"
                + "\"name\":\"old\"}");
    SparkPipelineRunConfiguration spark =
        (SparkPipelineRunConfiguration) old.getEngineRunConfiguration();
    assertEquals("yarn", spark.getSparkMaster());
    assertNull(spark.getClusterType());
    assertEquals(SparkClusterType.YARN, spark.getEffectiveClusterType());

    PipelineRunConfiguration saved =
        load(
            "{\"engineRunConfiguration\":{\"SparkPipelineEngine\":{\"sparkMaster\":\"\","
                + "\"cluster_type\":\"SUBMITTED\"}},\"name\":\"new\"}");
    assertEquals(
        SparkClusterType.SUBMITTED,
        ((SparkPipelineRunConfiguration) saved.getEngineRunConfiguration()).getClusterType());
    assertTrue(
        parser().getJsonObject(saved).toJSONString().contains("\"cluster_type\":\"SUBMITTED\""),
        "stored by its code");
  }

  /**
   * Plugins to stage and the temp location did nothing and are gone: a configuration that still has
   * them loads.
   */
  @Test
  void aConfigurationWithARemovedOptionStillLoads() throws Exception {
    PipelineRunConfiguration old =
        load(
            "{\"engineRunConfiguration\":{\"SparkPipelineEngine\":{\"sparkMaster\":\"yarn\","
                + "\"pluginsToStage\":\"tech/neo4j\",\"tempLocation\":\"/tmp\"}},\"name\":\"old\"}");
    assertEquals(
        "yarn", ((SparkPipelineRunConfiguration) old.getEngineRunConfiguration()).getSparkMaster());
  }

  /**
   * The Spark settings and the path scheme map of a configuration saved as text lines load as rows
   * of their tables, and are saved as rows; the engine reads the same lines as before.
   */
  @Test
  void textSettingsOfAnOlderConfigurationBecomeRows() throws Exception {
    PipelineRunConfiguration old =
        load(
            "{\"engineRunConfiguration\":{\"SparkPipelineEngine\":{\"sparkMaster\":\"yarn\","
                + "\"sparkConfigs\":\"# tuned\\nspark.yarn.queue=etl\\nspark.sql.shuffle.partitions = 64\","
                + "\"pathSchemeMap\":\"minio=s3a\"}},\"name\":\"old\"}");
    SparkPipelineRunConfiguration spark =
        (SparkPipelineRunConfiguration) old.getEngineRunConfiguration();
    assertEquals(2, spark.getSparkSettings().size(), "the comment line is not a setting");
    assertEquals("spark.sql.shuffle.partitions", spark.getSparkSettings().get(1).getKey());
    assertEquals("64", spark.getSparkSettings().get(1).getValue());
    assertEquals("spark.yarn.queue=etl\nspark.sql.shuffle.partitions=64", spark.getSparkConfigs());
    assertEquals("minio=s3a", spark.getPathSchemeMap());

    String saved = parser().getJsonObject(old).toJSONString();
    assertTrue(saved.contains("\"spark_settings\""), saved);
    assertTrue(saved.contains("\"path_schemes\""), saved);
    assertTrue(!saved.contains("spark.yarn.queue=etl"), "the old text is gone: " + saved);

    SparkPipelineRunConfiguration again =
        (SparkPipelineRunConfiguration)
            load(saved.replace("\"name\":\"old\"", "\"name\":\"again\""))
                .getEngineRunConfiguration();
    assertEquals(spark.getSparkConfigs(), again.getSparkConfigs(), "no rows twice");
    assertEquals("minio=s3a", again.getPathSchemeMap());
    assertEquals(2, again.clone().getSparkSettings().size());
  }

  @Test
  void aTemplateSetsTheTypeOfItsMaster() {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    SparkRunConfigTemplate.YARN_CLIENT.applyTo(config);
    assertEquals(SparkClusterType.YARN, config.getClusterType());
    SparkRunConfigTemplate.DATABRICKS.applyTo(config);
    assertEquals(SparkClusterType.DATABRICKS, config.getClusterType(), "no master of its own");
    SparkRunConfigTemplate.SPARK_SUBMIT.applyTo(config);
    assertEquals(SparkClusterType.SUBMITTED, config.getClusterType());
  }

  @Test
  void onlyTheOptionsThatApplyAreShown() {
    Set<String> local =
        SparkRunConfigurationWidgetsListener.hiddenOptions(SparkClusterType.LOCAL, false);
    assertTrue(
        local.containsAll(Set.of("deployMode", "fatJar", "executorMemory", "stagingFolder")));

    Set<String> yarnClient =
        SparkRunConfigurationWidgetsListener.hiddenOptions(SparkClusterType.YARN, false);
    assertFalse(yarnClient.contains("hadoopConfDir"));
    assertTrue(yarnClient.contains("submitUrl"));
    assertFalse(
        yarnClient.contains("stagingFolder"),
        "client mode can stage the fat jar for the executors");
    Set<String> yarnCluster =
        SparkRunConfigurationWidgetsListener.hiddenOptions(SparkClusterType.YARN, true);
    assertFalse(yarnCluster.contains("stagingFolder"), "cluster mode stages the jar");

    Set<String> standaloneCluster =
        SparkRunConfigurationWidgetsListener.hiddenOptions(SparkClusterType.STANDALONE, true);
    assertFalse(standaloneCluster.contains("submitUrl"));
    assertTrue(standaloneCluster.contains("hadoopConfDir"));

    Set<String> kubernetes =
        SparkRunConfigurationWidgetsListener.hiddenOptions(SparkClusterType.KUBERNETES, true);
    assertTrue(kubernetes.containsAll(Set.of("hadoopConfDir", "submitUrl")));
    assertFalse(kubernetes.contains("fatJar"));

    Set<String> submitted =
        SparkRunConfigurationWidgetsListener.hiddenOptions(SparkClusterType.SUBMITTED, false);
    assertTrue(submitted.containsAll(Set.of("deployMode", "sparkMaster")));
    assertTrue(
        submitted.containsAll(SparkRunConfigurationWidgetsListener.DATABRICKS_OPTIONS),
        "the Databricks options belong to the Databricks cluster type only");

    Set<String> databricks =
        SparkRunConfigurationWidgetsListener.hiddenOptions(SparkClusterType.DATABRICKS, false);
    assertTrue(
        databricks.containsAll(
            Set.of("sparkMaster", "deployMode", "hadoopConfDir", "stagingClusterUrl")),
        "Databricks sets the master, and the cluster reads the volume under the same path");
    assertTrue(databricks.containsAll(Set.of("driverMemory", "executorMemory", "executorCores")));
    assertFalse(databricks.contains("stagingFolder"), "the volume folder Hop stages in");
    assertFalse(databricks.contains("fatJar"));
    SparkRunConfigurationWidgetsListener.DATABRICKS_OPTIONS.forEach(
        option -> assertFalse(databricks.contains(option), option));

    assertTrue(
        SparkRunConfigurationWidgetsListener.hiddenOptions(null, false).isEmpty(),
        "a master in a variable hides nothing");
  }

  @Test
  void databricksHasNoSparkMasterOfItsOwn() {
    assertTrue(SparkClusterType.DATABRICKS.matchesMaster(""));
    assertTrue(SparkClusterType.DATABRICKS.matchesMaster(null));
    assertFalse(SparkClusterType.DATABRICKS.matchesMaster("local[*]"));
    assertEquals(
        SparkClusterType.SUBMITTED,
        SparkClusterType.fromMaster(""),
        "an older run configuration without a master stays spark-submit");
  }

  private static JsonMetadataParser<PipelineRunConfiguration> parser() {
    JsonMetadataProvider provider =
        new JsonMetadataProvider(
            new HopTwoWayPasswordEncoder(), "/tmp/hop-spark-cluster-type-test", new Variables());
    return new JsonMetadataParser<>(PipelineRunConfiguration.class, provider);
  }

  private static PipelineRunConfiguration load(String json) throws Exception {
    try (JsonParser jsonParser = new JsonFactory().createParser(json)) {
      jsonParser.nextToken();
      return parser().loadJsonObject(PipelineRunConfiguration.class, jsonParser);
    }
  }
}
