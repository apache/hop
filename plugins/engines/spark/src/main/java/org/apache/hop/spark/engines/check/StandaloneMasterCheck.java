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
import java.util.ArrayList;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.spark.engines.SparkClusterType;

/** The standalone master answers on its port. */
public class StandaloneMasterCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "StandaloneMaster";

  static final int TIMEOUT_MS = 5000;

  public StandaloneMasterCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.getClusterType() == SparkClusterType.STANDALONE;
  }

  @Override
  public List<String> getRequiredChecks() {
    return List.of(ClusterTypeCheck.ID);
  }

  @Override
  public ICheckResult run(SparkCheckContext context) {
    List<String> reached = new ArrayList<>();
    List<String> failed = new ArrayList<>();
    for (String[] master : masters(context.getMaster())) {
      String address = master[0] + ":" + master[1];
      try (Socket socket = new Socket()) {
        socket.connect(new InetSocketAddress(master[0], Integer.parseInt(master[1])), TIMEOUT_MS);
        reached.add(address);
      } catch (Exception e) {
        failed.add(address + " (" + e.getMessage() + ")");
      }
    }
    if (reached.isEmpty()) {
      return error(
          "The master can't be reached: " + String.join(", ", failed) + ".",
          "Check the host name and port of the Spark master (7077 by default), that the master"
              + " runs, and that no firewall blocks the port from this machine.");
    }
    if (!failed.isEmpty()) {
      return warning(
          "Reached " + String.join(", ", reached) + ", not " + String.join(", ", failed) + ".",
          "A standby master that is down is fine; otherwise check that one.");
    }
    return ok("The master answers at " + String.join(", ", reached) + ".");
  }

  /** The host and port of every master of {@code spark://a:7077,b:7077}. */
  static List<String[]> masters(String master) {
    List<String[]> masters = new ArrayList<>();
    String hosts = master.trim().substring("spark://".length());
    for (String hostPort : hosts.split(",")) {
      String trimmed = hostPort.trim();
      int colon = trimmed.lastIndexOf(':');
      masters.add(
          colon > 0
              ? new String[] {trimmed.substring(0, colon), trimmed.substring(colon + 1)}
              : new String[] {trimmed, "7077"});
    }
    return masters;
  }
}
