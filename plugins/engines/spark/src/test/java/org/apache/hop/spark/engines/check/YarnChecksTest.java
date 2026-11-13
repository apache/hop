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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.spark.engines.SparkClusterType;
import org.apache.hop.spark.engines.SparkDeployMode;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** The YARN checks that need no cluster; the others ran by hand against a Hadoop 3 cluster. */
class YarnChecksTest {

  @TempDir Path conf;

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  private SparkCheckContext context(String sparkConfigs) {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    config.setClusterType(SparkClusterType.YARN);
    config.setSparkMaster("yarn");
    config.setHadoopConfDir(conf.toString());
    config.setSparkConfigs(sparkConfigs);
    return new SparkCheckContext(config, new Variables());
  }

  private void site(String file, String... properties) throws Exception {
    StringBuilder xml = new StringBuilder("<configuration>");
    for (int i = 0; i < properties.length; i += 2) {
      xml.append("<property><name>")
          .append(properties[i])
          .append("</name><value>")
          .append(properties[i + 1])
          .append("</value></property>");
    }
    Files.writeString(conf.resolve(file), xml.append("</configuration>").toString());
  }

  @Test
  void theConfigurationFolderMustHoldTheSiteFiles() throws Exception {
    site("core-site.xml", "fs.defaultFS", "hdfs://namenode:8020");
    ICheckResult missing = SparkConfigurationChecker.run(new YarnConfigurationCheck(), context(""));
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, missing.getType());
    assertTrue(missing.getText().contains("yarn-site.xml"), missing.getText());

    site("yarn-site.xml", "yarn.nodemanager.resource.memory-mb", "8192");
    assertEquals(
        ICheckResult.TYPE_RESULT_WARNING,
        SparkConfigurationChecker.run(new YarnConfigurationCheck(), context("")).getType(),
        "no ResourceManager named");

    site("yarn-site.xml", "yarn.resourcemanager.hostname", "rm.example");
    SparkCheckContext context = context("spark.hadoop.yarn.scheduler.maximum-allocation-mb=4096");
    ICheckResult read = SparkConfigurationChecker.run(new YarnConfigurationCheck(), context);
    assertEquals(ICheckResult.TYPE_RESULT_OK, read.getType(), read.getText());
    assertTrue(read.getText().contains("rm.example:8032"), read.getText());
    Map<String, String> properties = context.get(YarnConfigurationCheck.HADOOP_PROPERTIES);
    assertEquals(
        "4096",
        properties.get("spark.hadoop.yarn.scheduler.maximum-allocation-mb"),
        "the Spark config adds to the site files");
  }

  @Test
  void aResourceManagerThatIsDownFailsFast() throws Exception {
    site("core-site.xml");
    site("yarn-site.xml", "yarn.resourcemanager.address", "127.0.0.1:1");
    SparkPipelineRunConfiguration config = context("").getConfiguration();
    long start = System.currentTimeMillis();
    Map<String, ICheckResult> byName = new HashMap<>();
    for (ICheckResult result :
        SparkConfigurationChecker.standard().check(config, new Variables(), null)) {
      byName.put(result.getSourceInfo().getName(), result);
    }
    assertTrue(System.currentTimeMillis() - start < 15_000, "no minutes of retries");
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, byName.get("YARN ResourceManager").getType());
    assertTrue(byName.get("YARN queue").getText().startsWith("Skipped"));
    assertTrue(byName.get("YARN container size").getText().startsWith("Skipped"));
  }

  @Test
  void anExecutorMustFitInTheLargestContainer() {
    SparkCheckContext context = context("");
    context.put(
        YarnConfigurationCheck.HADOOP_PROPERTIES,
        Map.of("spark.hadoop.yarn.scheduler.maximum-allocation-mb", "8192"));
    context.getConfiguration().setExecutorMemory("7g");
    assertEquals(
        ICheckResult.TYPE_RESULT_OK,
        SparkConfigurationChecker.run(new YarnContainerSizeCheck(), context).getType());

    context.getConfiguration().setExecutorMemory("8g");
    ICheckResult tooBig = SparkConfigurationChecker.run(new YarnContainerSizeCheck(), context);
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, tooBig.getType());
    assertTrue(tooBig.getText().contains("at most 7448 MiB"), tooBig.getText());

    context.getConfiguration().setExecutorMemory("2g");
    context.put(YarnResourceManagerCheck.LARGEST_NODE_MIB, 2048L);
    assertEquals(
        ICheckResult.TYPE_RESULT_ERROR,
        SparkConfigurationChecker.run(new YarnContainerSizeCheck(), context).getType(),
        "no node is large enough");

    context.put(YarnResourceManagerCheck.LARGEST_NODE_MIB, 65536L);
    context.getConfiguration().setDeployMode(SparkDeployMode.CLUSTER);
    context.getConfiguration().setDriverMemory("16g");
    assertTrue(
        SparkConfigurationChecker.run(new YarnContainerSizeCheck(), context)
            .getText()
            .contains("driver"));

    for (long maximum : new long[] {1024, 4224, 8192, 16384}) {
      long fits = YarnContainerSizeCheck.largestFitting(maximum);
      assertTrue(YarnContainerSizeCheck.withOverhead(fits) <= maximum);
      assertTrue(
          YarnContainerSizeCheck.withOverhead(fits + 1) > maximum, "the largest: " + maximum);
    }
  }

  @Test
  void kerberosNeedsAKeytabThatExists() throws Exception {
    SparkCheckContext simple = context("");
    simple.put(YarnConfigurationCheck.HADOOP_PROPERTIES, Map.of());
    assertEquals(
        ICheckResult.TYPE_RESULT_OK,
        SparkConfigurationChecker.run(new YarnKerberosCheck(), simple).getType());

    Map<String, String> kerberos =
        Map.of("spark.hadoop.hadoop.security.authentication", "kerberos");
    SparkCheckContext ticket = context("");
    ticket.put(YarnConfigurationCheck.HADOOP_PROPERTIES, kerberos);
    assertEquals(
        ICheckResult.TYPE_RESULT_WARNING,
        SparkConfigurationChecker.run(new YarnKerberosCheck(), ticket).getType());

    SparkCheckContext missing =
        context(
            "spark.kerberos.principal=hop@EXAMPLE\nspark.kerberos.keytab="
                + conf.resolve("none.keytab"));
    missing.put(YarnConfigurationCheck.HADOOP_PROPERTIES, kerberos);
    assertEquals(
        ICheckResult.TYPE_RESULT_ERROR,
        SparkConfigurationChecker.run(new YarnKerberosCheck(), missing).getType());

    Path keytab = Files.writeString(conf.resolve("hop.keytab"), "x");
    SparkCheckContext present =
        context("spark.kerberos.principal=hop@EXAMPLE\nspark.kerberos.keytab=" + keytab);
    present.put(YarnConfigurationCheck.HADOOP_PROPERTIES, kerberos);
    assertEquals(
        ICheckResult.TYPE_RESULT_OK,
        SparkConfigurationChecker.run(new YarnKerberosCheck(), present).getType());
  }
}
