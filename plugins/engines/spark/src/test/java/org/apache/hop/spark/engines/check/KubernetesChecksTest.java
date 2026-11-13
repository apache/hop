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

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
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
 * The Kubernetes checks against a stand-in for the API server; they also ran by hand against k3s,
 * as cluster admin and as a service account without rights.
 */
class KubernetesChecksTest {

  private HttpServer server;
  private boolean allowed;
  private int accountReviewCode;
  private String previousKubeconfig;

  @BeforeAll
  static void init() throws Exception {
    HopClientEnvironment.init();
  }

  @BeforeEach
  void start() throws Exception {
    // No kubeconfig of the developer running the test
    previousKubeconfig = System.setProperty("kubeconfig", "/nonexistent/kubeconfig");
    allowed = true;
    accountReviewCode = 201;
    server = HttpServer.create(new InetSocketAddress("127.0.0.1", 0), 0);
    server.createContext("/version", e -> respond(e, 200, "{\"gitVersion\":\"v1.33.0\"}"));
    server.createContext(
        "/api/v1/namespaces/",
        e -> {
          String path = e.getRequestURI().getPath();
          if (path.equals("/api/v1/namespaces/default")) {
            respond(
                e,
                200,
                "{\"kind\":\"Namespace\",\"apiVersion\":\"v1\",\"metadata\":{\"name\":\"default\"}}");
          } else if (path.equals("/api/v1/namespaces/default/serviceaccounts/spark")) {
            respond(
                e,
                200,
                "{\"kind\":\"ServiceAccount\",\"apiVersion\":\"v1\",\"metadata\":{\"name\":\"spark\"}}");
          } else {
            respond(
                e,
                404,
                "{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"status\":\"Failure\",\"code\":404}");
          }
        });
    server.createContext(
        "/apis/authorization.k8s.io/v1/selfsubjectaccessreviews",
        e -> respond(e, 201, review("SelfSubjectAccessReview", allowed)));
    server.createContext(
        "/apis/authorization.k8s.io/v1/subjectaccessreviews",
        e ->
            respond(
                e,
                accountReviewCode,
                accountReviewCode == 403
                    ? "{\"kind\":\"Status\",\"apiVersion\":\"v1\",\"status\":\"Failure\",\"code\":403}"
                    : review("SubjectAccessReview", true)));
    server.start();
  }

  @AfterEach
  void stop() {
    server.stop(0);
    if (previousKubeconfig == null) {
      System.clearProperty("kubeconfig");
    } else {
      System.setProperty("kubeconfig", previousKubeconfig);
    }
  }

  private static String review(String kind, boolean allowed) {
    return "{\"kind\":\""
        + kind
        + "\",\"apiVersion\":\"authorization.k8s.io/v1\",\"status\":{\"allowed\":"
        + allowed
        + "}}";
  }

  private static void respond(HttpExchange exchange, int code, String body) throws IOException {
    exchange.getRequestBody().readAllBytes();
    byte[] bytes = body.getBytes(StandardCharsets.UTF_8);
    exchange.getResponseHeaders().add("Content-Type", "application/json");
    exchange.sendResponseHeaders(code, bytes.length);
    exchange.getResponseBody().write(bytes);
    exchange.close();
  }

  private Map<String, ICheckResult> check(SparkDeployMode mode, String sparkConfigs) {
    SparkPipelineRunConfiguration config = new SparkPipelineRunConfiguration();
    config.setClusterType(SparkClusterType.KUBERNETES);
    config.setSparkMaster("k8s://http://127.0.0.1:" + server.getAddress().getPort());
    config.setDeployMode(mode);
    config.setSparkConfigs(sparkConfigs);
    config.setStagingFolder(System.getProperty("java.io.tmpdir"));
    config.setStagingClusterUrl("s3a://staging");
    Map<String, ICheckResult> byName = new HashMap<>();
    for (ICheckResult result :
        SparkConfigurationChecker.standard().check(config, new Variables(), null)) {
      byName.put(result.getSourceInfo().getName(), result);
    }
    return byName;
  }

  @Test
  void aClusterWithEverythingInPlacePasses() {
    Map<String, ICheckResult> results =
        check(
            SparkDeployMode.CLUSTER,
            "spark.kubernetes.container.image=spark:4.1\n"
                + "spark.kubernetes.authenticate.driver.serviceAccountName=spark");
    for (String name :
        new String[] {
          "Kubernetes API server",
          "Kubernetes namespace and permissions",
          "Driver service account",
          "Container image"
        }) {
      assertEquals(
          ICheckResult.TYPE_RESULT_OK,
          results.get(name).getType(),
          name + ": " + results.get(name).getText());
    }
    assertTrue(results.get("Kubernetes API server").getText().contains("v1.33.0"));
  }

  @Test
  void missingRightsNamespaceAndAccountSayWhatToDo() {
    allowed = false;
    ICheckResult rights =
        check(SparkDeployMode.CLIENT, "").get("Kubernetes namespace and permissions");
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, rights.getType());
    assertTrue(rights.getText().contains("create pods"), rights.getText());
    assertTrue(rights.getText().contains("kubectl auth can-i"), rights.getText());

    allowed = true;
    ICheckResult namespace =
        check(SparkDeployMode.CLIENT, "spark.kubernetes.namespace=nope")
            .get("Kubernetes namespace and permissions");
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, namespace.getType());
    assertTrue(namespace.getText().contains("'nope' does not exist"), namespace.getText());

    ICheckResult account =
        check(
                SparkDeployMode.CLUSTER,
                "spark.kubernetes.authenticate.driver.serviceAccountName=nobody")
            .get("Driver service account");
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, account.getType());

    accountReviewCode = 403;
    ICheckResult cannotTell =
        check(
                SparkDeployMode.CLUSTER,
                "spark.kubernetes.authenticate.driver.serviceAccountName=spark")
            .get("Driver service account");
    assertEquals(ICheckResult.TYPE_RESULT_WARNING, cannotTell.getType());
    assertTrue(
        cannotTell.getText().contains("--as=system:serviceaccount:default:spark"),
        cannotTell.getText());
  }

  @Test
  void anImageIsNeededAndClientModeNeedsTheWayBack() {
    Map<String, ICheckResult> client = check(SparkDeployMode.CLIENT, "");
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, client.get("Container image").getType());
    assertEquals(
        ICheckResult.TYPE_RESULT_WARNING, client.get("Connection back to the driver").getType());

    client =
        check(
            SparkDeployMode.CLIENT,
            "spark.kubernetes.container.image=spark:4.1\nspark.driver.host=10.0.0.5");
    assertEquals(ICheckResult.TYPE_RESULT_OK, client.get("Container image").getType());
    assertEquals(
        ICheckResult.TYPE_RESULT_OK, client.get("Connection back to the driver").getType());
  }

  @Test
  void theApiServerComesFromTheMaster() {
    assertEquals("https://k8s:6443", KubernetesApiCheck.apiServer("k8s://https://k8s:6443"));
    assertEquals("https://k8s:6443", KubernetesApiCheck.apiServer("k8s://k8s:6443"));
    server.stop(0);
    ICheckResult down = check(SparkDeployMode.CLIENT, "").get("Kubernetes API server");
    assertEquals(ICheckResult.TYPE_RESULT_ERROR, down.getType());
  }
}
