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

import java.util.Map;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.spark.engines.SparkClusterType;

/** Kubernetes: a container image is set for the Spark pods. */
public class KubernetesImageCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "KubernetesImage";

  public KubernetesImageCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.getClusterType() == SparkClusterType.KUBERNETES;
  }

  @Override
  public ICheckResult run(SparkCheckContext context) {
    Map<String, String> configs = context.getSparkConfigs();
    String image = configs.get("spark.kubernetes.container.image");
    String executor = configs.getOrDefault("spark.kubernetes.executor.container.image", image);
    String driver = configs.getOrDefault("spark.kubernetes.driver.container.image", image);
    boolean needsDriver = context.isClusterMode();
    if (executor == null || needsDriver && driver == null) {
      return error(
          "No container image is set for the Spark "
              + (executor == null ? "executor" : "driver")
              + " pods.",
          "Set spark.kubernetes.container.image=<image> in the Spark config: an image with Spark "
              + VersionsCheck.majorMinor(VersionsCheck.sparkVersion())
              + " and Java 21.");
    }
    return ok(
        "The pods run "
            + (needsDriver && !driver.equals(executor) ? driver + " (driver) and " : "")
            + executor
            + ". Whether the nodes can pull it shows when the first pod starts.");
  }
}
