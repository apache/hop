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

package org.apache.hop.server.loadbalance;

import java.util.List;
import org.apache.hop.server.IRemoteCapableRunConfiguration;

/** Engine-specific options for the load-balancing pipeline and workflow run configurations. */
public interface ILoadBalancingRunConfiguration extends IRemoteCapableRunConfiguration {

  List<LoadBalancingServerEntry> getServers();

  void setServers(List<LoadBalancingServerEntry> servers);

  String getAlgorithm();

  void setAlgorithm(String algorithm);

  String getMaxRetries();

  void setMaxRetries(String maxRetries);

  String getRetryWindowMs();

  void setRetryWindowMs(String retryWindowMs);

  boolean isRetryOnExecutionFailure();

  void setRetryOnExecutionFailure(boolean retryOnExecutionFailure);

  String getProbeTimeoutMs();

  void setProbeTimeoutMs(String probeTimeoutMs);

  String getConfigRefreshIntervalMs();

  void setConfigRefreshIntervalMs(String configRefreshIntervalMs);

  String getStateFolder();

  void setStateFolder(String stateFolder);
}
