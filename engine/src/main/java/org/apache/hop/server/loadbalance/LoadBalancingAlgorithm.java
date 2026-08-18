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

import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/** How a load-balancing engine picks an eligible Hop Server. */
public enum LoadBalancingAlgorithm implements IEnumHasCodeAndDescription {
  EVEN_LOAD("even-load", "Even load"),
  PACK("pack", "Keep as few servers busy as possible");

  private final String code;
  private final String description;

  LoadBalancingAlgorithm(String code, String description) {
    this.code = code;
    this.description = description;
  }

  @Override
  public String getCode() {
    return code;
  }

  @Override
  public String getDescription() {
    return description;
  }

  public static LoadBalancingAlgorithm fromCodeOrDescription(String value) {
    if (value == null || value.isBlank()) {
      return EVEN_LOAD;
    }
    LoadBalancingAlgorithm byCode =
        org.apache.hop.metadata.api.IEnumHasCode.lookupCode(
            LoadBalancingAlgorithm.class, value, null);
    if (byCode != null) {
      return byCode;
    }
    return IEnumHasCodeAndDescription.lookupDescription(
        LoadBalancingAlgorithm.class, value, EVEN_LOAD);
  }
}
