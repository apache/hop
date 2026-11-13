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
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.spark.engines.SparkClusterType;
import org.apache.hop.spark.util.SparkHadoopConfiguration;

/** YARN with Kerberos: a principal with a keytab, or the Kerberos ticket of this user. */
public class YarnKerberosCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "YarnKerberos";

  public YarnKerberosCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.getClusterType() == SparkClusterType.YARN;
  }

  @Override
  public List<String> getRequiredChecks() {
    return List.of(YarnConfigurationCheck.ID);
  }

  @Override
  public ICheckResult run(SparkCheckContext context) throws Exception {
    Map<String, String> properties = context.get(YarnConfigurationCheck.HADOOP_PROPERTIES);
    String authentication =
        properties.getOrDefault(
            SparkHadoopConfiguration.SPARK_HADOOP_PREFIX + "hadoop.security.authentication",
            "simple");
    if (!"kerberos".equalsIgnoreCase(authentication.trim())) {
      return ok("The cluster doesn't use Kerberos.");
    }
    Map<String, String> configs = context.getSparkConfigs();
    String principal = configs.get("spark.kerberos.principal");
    String keytab = configs.get("spark.kerberos.keytab");
    if (principal != null && keytab != null) {
      if (!HopVfs.fileExists(keytab, context.getVariables())) {
        return error(
            "The keytab '" + keytab + "' of " + principal + " does not exist.",
            "Correct spark.kerberos.keytab in the Spark config.");
      }
      return ok(
          "The cluster uses Kerberos: Spark logs in as " + principal + " with " + keytab + ".");
    }
    return warning(
        "The cluster uses Kerberos and no principal and keytab are set: Spark uses the Kerberos"
            + " ticket of the user that runs Hop, which can't be checked from here.",
        "Run kinit before the pipeline, or set spark.kerberos.principal and spark.kerberos.keytab in"
            + " the Spark config.");
  }
}
