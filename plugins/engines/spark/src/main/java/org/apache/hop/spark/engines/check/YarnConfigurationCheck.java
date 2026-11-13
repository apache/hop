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

package org.apache.hop.spark.engines.check;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import org.apache.commons.vfs2.FileObject;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.spark.engines.SparkClusterType;
import org.apache.hop.spark.util.SparkHadoopConfiguration;

/**
 * YARN: the Hadoop configuration folder holds core-site.xml and yarn-site.xml and names a
 * ResourceManager. The configuration, as the engine reads it, is kept for the next checks.
 */
public class YarnConfigurationCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "YarnConfiguration";

  /** The spark.hadoop.* properties of the site files and the Spark config. */
  static final String HADOOP_PROPERTIES = "yarn.hadoopProperties";

  public YarnConfigurationCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.getClusterType() == SparkClusterType.YARN;
  }

  @Override
  public ICheckResult run(SparkCheckContext context) throws Exception {
    List<String> folders =
        SparkHadoopConfiguration.resolveFolders(
            context.resolve(context.getConfiguration().getHadoopConfDir()), true, System::getenv);
    if (folders.isEmpty()) {
      return error(
          "No Hadoop configuration folder is set, and HADOOP_CONF_DIR and YARN_CONF_DIR are empty.",
          "Set the Hadoop configuration folder on the Cluster tab: the folder with the cluster's"
              + " core-site.xml and yarn-site.xml.");
    }
    List<String> missing = new ArrayList<>();
    for (String file : List.of("core-site.xml", "yarn-site.xml")) {
      boolean present = false;
      for (String folder : folders) {
        FileObject dir = HopVfs.getFileObject(folder, context.getVariables());
        present |= dir.resolveFile(file).exists();
      }
      if (!present) {
        missing.add(file);
      }
    }
    if (!missing.isEmpty()) {
      return error(
          String.join(" and ", missing) + " is not in " + String.join(", ", folders) + ".",
          "Copy the client configuration of the cluster (core-site.xml, yarn-site.xml, hdfs-site.xml)"
              + " into the Hadoop configuration folder.");
    }
    Map<String, String> properties =
        new LinkedHashMap<>(SparkHadoopConfiguration.load(folders, LogChannel.GENERAL));
    // The Spark config can add or override Hadoop settings as spark.hadoop.<key>
    context
        .getSparkConfigs()
        .forEach(
            (key, value) -> {
              if (key.startsWith(SparkHadoopConfiguration.SPARK_HADOOP_PREFIX)) {
                properties.put(key, value);
              }
            });
    context.put(HADOOP_PROPERTIES, properties);
    String address = resourceManager(properties);
    if (address == null) {
      return warning(
          "The configuration in " + String.join(", ", folders) + " names no ResourceManager.",
          "Set yarn.resourcemanager.hostname (or yarn.resourcemanager.address) in yarn-site.xml.");
    }
    return ok("Read " + String.join(", ", folders) + ": the ResourceManager is " + address + ".");
  }

  static String resourceManager(Map<String, String> properties) {
    String address =
        properties.get(
            SparkHadoopConfiguration.SPARK_HADOOP_PREFIX + "yarn.resourcemanager.address");
    if (address != null && !address.contains("${")) {
      return address;
    }
    String host =
        properties.get(
            SparkHadoopConfiguration.SPARK_HADOOP_PREFIX + "yarn.resourcemanager.hostname");
    return host == null ? null : host + ":8032";
  }
}
