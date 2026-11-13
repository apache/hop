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
import org.apache.spark.package$;

/** The Spark and Java versions on the Hop side, which the cluster has to match. */
public class VersionsCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "Versions";

  public VersionsCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return true;
  }

  @Override
  public ICheckResult run(SparkCheckContext context) {
    String spark = sparkVersion();
    String found =
        "Hop runs Spark " + spark + " on Java " + System.getProperty("java.version") + ".";
    if (context.isOnACluster()) {
      found +=
          " The cluster needs Spark "
              + majorMinor(spark)
              + " and Java 21 or later for the Hop classes.";
    }
    return info(found);
  }

  public static String sparkVersion() {
    return package$.MODULE$.SPARK_VERSION();
  }

  static String majorMinor(String version) {
    String[] parts = version.split("\\.");
    return parts.length >= 2 ? parts[0] + "." + parts[1] : version;
  }
}
