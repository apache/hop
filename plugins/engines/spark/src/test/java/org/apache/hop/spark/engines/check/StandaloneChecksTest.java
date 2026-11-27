/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.spark.engines.check;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.sun.net.httpserver.HttpServer;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.HopClientEnvironment;
import org.apache.hop.core.ICheckResult;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.spark.engines.SparkClusterType;
import org.apache.hop.spark.engines.SparkDeployMode;
import org.apache.hop.spark.engines.SparkPipelineRunConfiguration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 * The checks of a standalone master, against a stand-in for its port, status page and REST server.
 */
class StandaloneChecksTest {

  private HttpServer server;
  private String statusJson;
  private int statusCode;
  private String restJson;

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void startServer() throws Exception {
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/json/", exchange -> respond(exchange, statusCode, statusJson));
    server.createContext("/v1/submissions/status/", exchange -> respond(exchange, 200, restJson));
    server.start();
    statusCode = 200;
  }

  @AfterEach
  void stopServer() {
    server.stop(0);
  }

  private static void respond(com.sun.net.httpserver.HttpExchange exchange, int code, String body)
      throws java.io.IOException {
    byte[] bytes = (body == null ? "" : body).getBytes(StandardCharsets.UTF_8);
    exchange.sendResponseHeaders(code, bytes.length == 0 ? -1 : bytes.length);
    if (bytes.length > 0) {
      exchange.getResponseBody().write(bytes);
    }
    exchange.close();
  }

  private int port() {
    return server.getAddress().getPort();
  }

  private SparkCheckContext context(String master, String executorMemory) {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    config.setClusterType(SparkClusterType.STANDALONE);
    config.setSparkMaster(master);
    config.setExecutorMemory(executorMemory);
    return new SparkCheckContext(config, new Variables());
  }

  private static String worker(String state, int coresFree, int memoryFree) {
    return "{\"state\":\""
        + state
        + "\",\"coresfree\":"
        + coresFree
        + ",\"memoryfree\":"
        + memoryFree
        + "}";
  }

  @Test
  void theMasterMustAnswerOnItsPort() throws Exception {
    ICheckResult reached =
        SparkConfigurationChecker.run(
            new StandaloneMasterCheck(), context("spark://127.0.0.1:" + port(), ""));
    assertEquals(ICheckResult.TYPE_RESULT_OK, reached.getType(), reached.getText());

    int closed;
    try (ServerSocket socket = new ServerSocket(0)) {
      closed = socket.getLocalPort();
    }
    ICheckResult down =
        SparkConfigurationChecker.run(
            new StandaloneMasterCheck(), context("spark://127.0.0.1:" + closed, ""));
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, down.getType());
    assertTrue(down.getText().contains("Fix:"), down.getText());

    ICheckResult standby =
        SparkConfigurationChecker.run(
            new StandaloneMasterCheck(),
            context("spark://127.0.0.1:" + port() + ",127.0.0.1:" + closed, ""));
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, standby.getType(), "one of two masters is down");
  }

  @Test
  void aWorkerMustHaveRoomForAnExecutor() {
    statusJson =
        "{\"status\":\"ALIVE\",\"workers\":["
            + worker("ALIVE", 2, 4096)
            + ","
            + worker("DEAD", 8, 65536)
            + "]}";
    StandaloneStatusCheck check = new StandaloneStatusCheck(port());

    ICheckResult fits =
        SparkConfigurationChecker.run(check, context("spark://127.0.0.1:7077", "2g"));
    assertEquals(ICheckResult.TYPE_RESULT_OK, fits.getType(), fits.getText());
    assertTrue(
        fits.getText().contains("1 worker(s)"), "the dead one doesn't count: " + fits.getText());

    ICheckResult tooBig =
        SparkConfigurationChecker.run(check, context("spark://127.0.0.1:7077", "8g"));
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, tooBig.getType(), tooBig.getText());

    statusJson = "{\"status\":\"STANDBY\",\"workers\":[]}";
    assertEquals(
        ICheckResult.TYPE_RESULT_ERROR,
        SparkConfigurationChecker.run(check, context("spark://127.0.0.1:7077", "")).getType());

    statusJson = "{\"status\":\"ALIVE\",\"workers\":[]}";
    assertEquals(
        ICheckResult.TYPE_RESULT_ERROR,
        SparkConfigurationChecker.run(check, context("spark://127.0.0.1:7077", "")).getType(),
        "no workers");

    statusCode = 500;
    assertEquals(
        ICheckResult.TYPE_RESULT_WARNING,
        SparkConfigurationChecker.run(check, context("spark://127.0.0.1:7077", "")).getType(),
        "an unreadable status page only stops this check");
  }

  @Test
  void clusterModeNeedsTheRestServerWithHopsSparkVersion() {
    SparkCheckContext context = context("spark://127.0.0.1:7077", "");
    context.getConfiguration().setDeployMode(SparkDeployMode.CLUSTER);
    context.getConfiguration().setSubmitUrl("http://127.0.0.1:" + port());

    restJson =
        "{\"action\":\"SubmissionStatusResponse\",\"serverSparkVersion\":\""
            + VersionsCheck.sparkVersion()
            + "\",\"success\":false}";
    ICheckResult same = SparkConfigurationChecker.run(new StandaloneRestCheck(), context);
    assertEquals(ICheckResult.TYPE_RESULT_OK, same.getType(), same.getText());

    restJson = "{\"action\":\"SubmissionStatusResponse\",\"serverSparkVersion\":\"3.5.1\"}";
    ICheckResult other = SparkConfigurationChecker.run(new StandaloneRestCheck(), context);
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, other.getType());
    assertTrue(other.getText().contains("3.5.1"), other.getText());

    context.getConfiguration().setSubmitUrl("http://127.0.0.1:1");
    assertEquals(
        ICheckResult.TYPE_RESULT_ERROR,
        SparkConfigurationChecker.run(new StandaloneRestCheck(), context).getType());
  }

  @Test
  void sizesAreReadAsSparkReadsThem() {
    assertEquals(512, StandaloneStatusCheck.sizeInMib("512m"));
    assertEquals(2048, StandaloneStatusCheck.sizeInMib("2g"));
    assertEquals(2048, StandaloneStatusCheck.sizeInMib("2GB"));
    assertEquals(1500, StandaloneStatusCheck.sizeInMib("1500"));
    assertEquals(1024, StandaloneStatusCheck.sizeInMib(""));
  }
}
