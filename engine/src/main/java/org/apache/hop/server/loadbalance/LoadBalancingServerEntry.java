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

import lombok.Getter;
import lombok.Setter;
import org.apache.hop.metadata.api.HopMetadataProperty;

/** One Hop Server in a load-balancing run configuration. */
@Getter
@Setter
public class LoadBalancingServerEntry {

  @HopMetadataProperty(key = "hop_server")
  private String hopServerName;

  @HopMetadataProperty(key = "enabled")
  private boolean enabled = true;

  /** Variable-capable maximum of occupying pipelines plus workflows on this server. */
  @HopMetadataProperty(key = "max_concurrent")
  private String maxConcurrent;

  public LoadBalancingServerEntry() {}

  public LoadBalancingServerEntry(String hopServerName, boolean enabled, String maxConcurrent) {
    this.hopServerName = hopServerName;
    this.enabled = enabled;
    this.maxConcurrent = maxConcurrent;
  }

  public LoadBalancingServerEntry(LoadBalancingServerEntry entry) {
    this.hopServerName = entry.hopServerName;
    this.enabled = entry.enabled;
    this.maxConcurrent = entry.maxConcurrent;
  }
}
