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
import org.apache.hop.spark.engines.SparkDeployMode;

/** The cluster type and the Spark master agree, and say where the pipeline runs. */
public class ClusterTypeCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "ClusterType";

  public ClusterTypeCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return true;
  }

  @Override
  public ICheckResult run(SparkCheckContext context) {
    String master = context.getMaster();
    SparkClusterType chosen = context.getConfiguration().getClusterType();
    SparkClusterType ofMaster = SparkClusterType.fromMaster(master);
    if (chosen != null && ofMaster != null && chosen != ofMaster && !chosen.matchesMaster(master)) {
      return error(
          "The cluster type is '"
              + chosen.getDescription()
              + "' but the Spark master '"
              + master
              + "' is one of '"
              + ofMaster.getDescription()
              + "'.",
          "Choose the cluster type '"
              + ofMaster.getDescription()
              + "', or set a Spark master such as '"
              + chosen.getMasterTemplate()
              + "'.");
    }
    SparkClusterType type = context.getClusterType();
    if (type == null) {
      return warning(
          "The Spark master '" + master + "' is not one Hop can tell the cluster type of.",
          "Choose the cluster type, so the test can run the checks of that cluster.");
    }
    String where = type.getDescription();
    if (type != SparkClusterType.LOCAL
        && type != SparkClusterType.SUBMITTED
        && type != SparkClusterType.DATABRICKS) {
      where +=
          ", "
              + SparkDeployMode.orDefault(context.getConfiguration().getDeployMode())
                  .getDescription();
    }
    return ok("Runs on " + where + (master.isEmpty() ? "." : ", master '" + master + "'."));
  }
}
