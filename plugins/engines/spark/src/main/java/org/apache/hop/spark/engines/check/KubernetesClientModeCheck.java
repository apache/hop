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

import org.apache.hop.core.ICheckResult;
import org.apache.hop.spark.engines.SparkClusterType;

/** Kubernetes, client mode: the executor pods connect back to the driver in this Hop process. */
public class KubernetesClientModeCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "KubernetesClientMode";

  public KubernetesClientModeCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.getClusterType() == SparkClusterType.KUBERNETES && !context.isClusterMode();
  }

  @Override
  public ICheckResult run(SparkCheckContext context) {
    String host = context.getSparkConfigs().get("spark.driver.host");
    if (host == null || host.isEmpty()) {
      return warning(
          "In client mode the executor pods connect back to this machine, at the address Spark"
              + " guesses for it, which pods often can't reach.",
          "Set spark.driver.host to an address of this machine the pods reach, and open"
              + " spark.driver.port and spark.blockManager.port; or run in cluster mode.");
    }
    return ok(
        "The executor pods connect back to "
            + host
            + ": spark.driver.port and"
            + " spark.blockManager.port must be open to them.");
  }
}
