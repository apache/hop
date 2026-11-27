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
import com.fasterxml.jackson.databind.ObjectMapper;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.util.List;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.spark.engines.SparkClusterType;

/**
 * The status page of the standalone master: it is ALIVE, and a worker has the cores and memory an
 * executor asks for.
 */
public class StandaloneStatusCheck extends BaseSparkConfigurationCheck {

  public static final String ID = "StandaloneStatus";

  /** The port of the master's web UI and its JSON status, unless the cluster moved it. */
  static final int UI_PORT = 8080;

  /** What Spark gives an executor when no executor memory is set. */
  static final long DEFAULT_EXECUTOR_MIB = 1024;

  private final int uiPort;

  public StandaloneStatusCheck() {
    this(UI_PORT);
  }

  /** For tests: the status page on another port. */
  StandaloneStatusCheck(int uiPort) {
    super(ID);
    this.uiPort = uiPort;
  }

  @Override
  public boolean appliesTo(SparkCheckContext context) {
    return context.getClusterType() == SparkClusterType.STANDALONE;
  }

  @Override
  public List<String> getRequiredChecks() {
    return List.of(StandaloneMasterCheck.ID);
  }

  @Override
  public ICheckResult run(SparkCheckContext context) throws Exception {
    String host = StandaloneMasterCheck.masters(context.getMaster()).get(0)[0];
    String url = "http://" + host + ":" + uiPort + "/json/";
    JsonNode status;
    try {
      status = getJson(url);
    } catch (Exception e) {
      return warning(
          "The status page "
              + url
              + " can't be read ("
              + e.getMessage()
              + "), so the workers"
              + " can't be checked.",
          "Open port "
              + UI_PORT
              + " of the master to this machine, or ignore this when the"
              + " cluster moved its web UI.");
    }
    String state = status.path("status").asText("");
    if (!"ALIVE".equals(state)) {
      return error(
          "The master is " + state + ", not ALIVE: it doesn't take applications.",
          "A STANDBY master hands over when the active one fails: point the Spark master at both"
              + " (spark://a:7077,b:7077), or at the active one.");
    }
    long needMib = sizeInMib(context.resolve(context.getConfiguration().getExecutorMemory()));
    int needCores = cores(context.resolve(context.getConfiguration().getExecutorCores()));
    int alive = 0;
    int fit = 0;
    for (JsonNode worker : status.path("workers")) {
      if (!"ALIVE".equals(worker.path("state").asText())) {
        continue;
      }
      alive++;
      if (worker.path("memoryfree").asLong() >= needMib
          && worker.path("coresfree").asInt() >= needCores) {
        fit++;
      }
    }
    String version = status.path("version").asText("");
    String found =
        "The master is ALIVE"
            + (version.isEmpty() ? "" : " (Spark " + version + ")")
            + " with "
            + alive
            + " worker(s).";
    if (alive == 0) {
      return error(found, "Start a worker and point it at this master.");
    }
    if (fit == 0) {
      return warning(
          found
              + " None has "
              + needMib
              + " MiB and "
              + Math.max(needCores, 1)
              + " core(s) free for an"
              + " executor right now.",
          "Lower the executor memory or cores, or wait until other applications free the workers.");
    }
    return ok(found + " " + fit + " can take an executor of " + needMib + " MiB.");
  }

  JsonNode getJson(String url) throws Exception {
    HttpClient client =
        HttpClient.newBuilder()
            .connectTimeout(Duration.ofMillis(StandaloneMasterCheck.TIMEOUT_MS))
            .build();
    HttpResponse<String> response =
        client.send(
            HttpRequest.newBuilder(URI.create(url))
                .timeout(Duration.ofMillis(StandaloneMasterCheck.TIMEOUT_MS))
                .GET()
                .build(),
            HttpResponse.BodyHandlers.ofString());
    if (response.statusCode() != 200) {
      throw new IllegalStateException("HTTP " + response.statusCode());
    }
    return new ObjectMapper().readTree(response.body());
  }

  /** A Spark size (512m, 2g, a number of MiB) in MiB; the default executor memory when empty. */
  static long sizeInMib(String size) {
    if (size == null || size.isBlank()) {
      return DEFAULT_EXECUTOR_MIB;
    }
    String value = size.trim().toLowerCase().replaceAll("b$", "");
    char unit = value.charAt(value.length() - 1);
    if (Character.isDigit(unit)) {
      return Long.parseLong(value);
    }
    long number = Long.parseLong(value.substring(0, value.length() - 1));
    return switch (unit) {
      case 'k' -> Math.max(1, number / 1024);
      case 'g' -> number * 1024;
      case 't' -> number * 1024 * 1024;
      case 'p' -> number * 1024 * 1024 * 1024;
      default -> number;
    };
  }

  private static int cores(String cores) {
    try {
      return cores.isEmpty() ? 1 : Integer.parseInt(cores);
    } catch (NumberFormatException e) {
      return 1;
    }
  }
}
