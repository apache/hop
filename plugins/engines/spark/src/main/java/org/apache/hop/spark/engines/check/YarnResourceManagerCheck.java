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

import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.yarn.api.records.NodeReport;
import org.apache.hadoop.yarn.api.records.NodeState;
import org.apache.hadoop.yarn.client.api.YarnClient;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.spark.engines.SparkClusterType;
import org.apache.hop.spark.util.SparkHadoopConfiguration;

/** YARN: the ResourceManager answers, and has NodeManagers running. */
public class YarnResourceManagerCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "YarnResourceManager";

  static final String CLIENT = "yarn.client";
  static final String LARGEST_NODE_MIB = "yarn.largestNodeMib";

  public YarnResourceManagerCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.getClusterType() == SparkClusterType.YARN;
  }

  @Override
  public List<String> getRequiredChecks() {
    return List.of(YarnConfigurationCheck.ID);
  }

  @Override
  public ICheckResult run(SparkCheckContext context) throws Exception {
    Map<String, String> properties = context.get(YarnConfigurationCheck.HADOOP_PROPERTIES);
    String address = YarnConfigurationCheck.resourceManager(properties);
    String unreachable = unreachable(address);
    if (unreachable != null) {
      // The YARN client retries for many seconds: a closed port says enough
      return error(
          "The ResourceManager " + address + " doesn't answer: " + unreachable + ".",
          "Check that the ResourceManager runs and that its port (8032 by default) is open to this"
              + " machine.");
    }
    Configuration configuration = new YarnConfiguration();
    configuration.setClassLoader(YarnResourceManagerCheck.class.getClassLoader());
    properties.forEach(
        (key, value) ->
            configuration.set(
                key.substring(SparkHadoopConfiguration.SPARK_HADOOP_PREFIX.length()), value));
    // A test answers within seconds: the client normally retries for minutes
    configuration.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_MAX_WAIT_MS, 10_000);
    configuration.setLong(YarnConfiguration.RESOURCEMANAGER_CONNECT_RETRY_INTERVAL_MS, 1_000);
    configuration.setInt("ipc.client.connect.max.retries", 1);
    configuration.setInt("ipc.client.connect.timeout", 5_000);
    YarnClient client = YarnClient.createYarnClient();
    client.init(configuration);
    client.start();
    context.put(CLIENT, client);
    int nodeManagers;
    List<NodeReport> nodes;
    try {
      nodeManagers = client.getYarnClusterMetrics().getNumNodeManagers();
      nodes = client.getNodeReports(NodeState.RUNNING);
    } catch (Exception e) {
      return error(
          "The ResourceManager " + address + " doesn't answer: " + e.getMessage(),
          "Check that the ResourceManager runs and that its port (8032 by default) is open to this"
              + " machine; on a secured cluster, that this user has a Kerberos ticket.");
    }
    if (nodeManagers == 0 || nodes.isEmpty()) {
      return error(
          "The ResourceManager " + address + " answers, but no NodeManager is running.",
          "Start the NodeManagers of the cluster: without them YARN has nowhere to run Spark.");
    }
    long largest = 0;
    for (NodeReport node : nodes) {
      largest = Math.max(largest, node.getCapability().getMemorySize());
    }
    context.put(LARGEST_NODE_MIB, largest);
    return ok(
        "The ResourceManager "
            + address
            + " answers with "
            + nodes.size()
            + " running NodeManager(s).");
  }

  /** Why {@code host:port} can't be reached, null when it can (or can't be told). */
  static String unreachable(String address) {
    if (address == null) {
      return null;
    }
    int colon = address.lastIndexOf(':');
    if (colon <= 0) {
      return null;
    }
    try (Socket socket = new Socket()) {
      socket.connect(
          new InetSocketAddress(
              address.substring(0, colon), Integer.parseInt(address.substring(colon + 1).trim())),
          StandaloneMasterCheck.TIMEOUT_MS);
      return null;
    } catch (NumberFormatException e) {
      return null;
    } catch (Exception e) {
      return e.getMessage();
    }
  }
}
