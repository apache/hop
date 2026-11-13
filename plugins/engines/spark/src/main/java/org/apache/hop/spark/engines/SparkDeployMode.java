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

package org.apache.hop.spark.engines;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/** Where the Spark driver of a native Spark pipeline runs. */
public enum SparkDeployMode implements IEnumHasCodeAndDescription {

  /**
   * The Hop process (Hop GUI, hop-run, Hop Server) is the driver. Executors on a cluster get the
   * fat jar and the project package from it.
   */
  CLIENT,

  /**
   * Hop stages the fat jar and the project package, submits {@code MainSpark} to the cluster and
   * waits for the driver there. The Hop process does not need to stay reachable for the executors.
   */
  CLUSTER;

  @Override
  public String getCode() {
    return name();
  }

  @Override
  public String getDescription() {
    return BaseMessages.getString(SparkDeployMode.class, "SparkDeployMode." + name());
  }

  /** {@link #CLIENT} for null, as stored by run configurations from before this option. */
  public static SparkDeployMode orDefault(SparkDeployMode mode) {
    return mode == null ? CLIENT : mode;
  }
}
