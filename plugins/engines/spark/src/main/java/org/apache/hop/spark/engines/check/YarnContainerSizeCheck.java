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

import java.util.List;
import java.util.Map;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.spark.engines.SparkClusterType;
import org.apache.hop.spark.util.SparkHadoopConfiguration;

/**
 * YARN: the largest container YARN hands out holds an executor's memory and its overhead, and in
 * cluster mode the driver's.
 */
public class YarnContainerSizeCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "YarnContainerSize";

  /** YARN's default maximum container size. */
  static final long DEFAULT_MAXIMUM_MIB = 8192;

  public YarnContainerSizeCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.getClusterType() == SparkClusterType.YARN;
  }

  @Override
  public List<String> getRequiredChecks() {
    return List.of(YarnResourceManagerCheck.ID);
  }

  @Override
  public ICheckResult run(SparkCheckContext context) {
    Map<String, String> properties = context.get(YarnConfigurationCheck.HADOOP_PROPERTIES);
    long maximum = DEFAULT_MAXIMUM_MIB;
    String configured =
        properties.get(
            SparkHadoopConfiguration.SPARK_HADOOP_PREFIX + "yarn.scheduler.maximum-allocation-mb");
    if (configured != null) {
      maximum = Long.parseLong(configured.trim());
    }
    Long largestNode = context.get(YarnResourceManagerCheck.LARGEST_NODE_MIB);
    if (largestNode != null && largestNode > 0) {
      maximum = Math.min(maximum, largestNode);
    }
    long executor =
        withOverhead(
            StandaloneStatusCheck.sizeInMib(
                context.resolve(context.getConfiguration().getExecutorMemory())));
    if (executor > maximum) {
      return error(
          "An executor needs a container of "
              + executor
              + " MiB (memory and overhead), but YARN hands out at most "
              + maximum
              + " MiB.",
          "Lower the executor memory to at most "
              + largestFitting(maximum)
              + " MiB, or ask for a"
              + " larger yarn.scheduler.maximum-allocation-mb.");
    }
    if (context.isClusterMode()) {
      long driver =
          withOverhead(
              StandaloneStatusCheck.sizeInMib(
                  context.resolve(context.getConfiguration().getDriverMemory())));
      if (driver > maximum) {
        return error(
            "The driver needs a container of "
                + driver
                + " MiB, but YARN hands out at most "
                + maximum
                + " MiB.",
            "Lower the driver memory, or ask for a larger yarn.scheduler.maximum-allocation-mb.");
      }
    }
    return ok(
        "An executor container of "
            + executor
            + " MiB fits in YARN's maximum of "
            + maximum
            + " MiB.");
  }

  /** The largest memory whose container (with its overhead) fits in {@code maximum} MiB. */
  static long largestFitting(long maximum) {
    // The overhead is at least 384 MiB: count down from there, a few hundred steps at most
    long memory = Math.max(0, maximum - 384);
    while (memory > 0 && withOverhead(memory) > maximum) {
      memory--;
    }
    return memory;
  }

  /** Spark asks YARN for the memory plus 10% overhead, at least 384 MiB. */
  static long withOverhead(long mib) {
    return mib + Math.max(384, mib / 10);
  }
}
