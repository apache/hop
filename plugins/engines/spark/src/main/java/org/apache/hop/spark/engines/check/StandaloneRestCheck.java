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

import com.fasterxml.jackson.databind.JsonNode;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.spark.engines.SparkClusterType;
import org.apache.hop.spark.submit.SparkStandaloneRestSubmitter;

/**
 * Cluster mode on a standalone master: its REST submission server answers, with the Spark version
 * of Hop.
 */
public class StandaloneRestCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "StandaloneRest";

  public StandaloneRestCheck() {
    super(ID);
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.getClusterType() == SparkClusterType.STANDALONE && context.isClusterMode();
  }

  @Override
  public List<String> getRequiredChecks() {
    return List.of(ClusterTypeCheck.ID);
  }

  @Override
  public ICheckResult run(SparkCheckContext context) throws Exception {
    String submitUrl = context.resolve(context.getConfiguration().getSubmitUrl());
    if (submitUrl.isEmpty()) {
      submitUrl = SparkStandaloneRestSubmitter.deriveSubmitUrl(context.getMaster());
    }
    String url = submitUrl.replaceAll("/+$", "") + "/v1/submissions/status/hop-configuration-test";
    JsonNode status;
    try {
      status = new StandaloneStatusCheck().getJson(url);
    } catch (Exception e) {
      // An unknown submission is answered with 404 by some versions: still a REST server
      if (e.getMessage() != null && e.getMessage().contains("HTTP 404")) {
        return ok("The REST submission server answers at " + submitUrl + ".");
      }
      return error(
          "The REST submission server at "
              + submitUrl
              + " doesn't answer ("
              + e.getMessage()
              + ").",
          "Start the master with spark.master.rest.enabled=true (port 6066), open that port to this"
              + " machine, or set the submit URL on the Cluster tab.");
    }
    String server = status.path("serverSparkVersion").asText("");
    String hop = VersionsCheck.sparkVersion();
    if (!server.isEmpty()
        && !VersionsCheck.majorMinor(server).equals(VersionsCheck.majorMinor(hop))) {
      return error(
          "The cluster runs Spark " + server + ", Hop runs Spark " + hop + ".",
          "Run the pipeline on a cluster with Spark " + VersionsCheck.majorMinor(hop) + ".");
    }
    return ok(
        "The REST submission server answers at "
            + submitUrl
            + (server.isEmpty() ? "." : " with Spark " + server + "."));
  }
}
