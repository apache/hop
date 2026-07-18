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

package org.apache.hop.databricks.client;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Map;
import java.util.Optional;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.util.EnvUtil;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.databricks.metadata.DatabricksConnection;
import org.apache.hop.junit.rules.RestoreHopEngineEnvironmentExtension;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.RegisterExtension;

class RestDatabricksJobsClientTest {

  @RegisterExtension
  static RestoreHopEngineEnvironmentExtension env = new RestoreHopEngineEnvironmentExtension();

  private WireMockServer server;
  private RestDatabricksJobsClient client;

  @BeforeAll
  static void setUpClass() throws HopException {
    PluginRegistry.addPluginType(TwoWayPasswordEncoderPluginType.getInstance());
    PluginRegistry.init();
    String passwordEncoderPluginID =
        Const.NVL(EnvUtil.getSystemProperty(Const.HOP_PASSWORD_ENCODER_PLUGIN), "Hop");
    Encr.init(passwordEncoderPluginID);
  }

  @BeforeEach
  void setUp() {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    server.start();
    WireMock.configureFor("localhost", server.port());
    client =
        RestDatabricksJobsClient.createForTest(
            "http://localhost:" + server.port(),
            "/api/2.1",
            "test-token",
            HttpClient.newHttpClient());
  }

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  void normalizeHostAddsHttps() {
    assertEquals(
        "https://my.databricks.net", RestDatabricksJobsClient.normalizeHost("my.databricks.net"));
    assertEquals(
        "https://my.databricks.net",
        RestDatabricksJobsClient.normalizeHost("https://my.databricks.net/"));
  }

  @Test
  void createResolvesHostAndDecryptsTokenVariable() throws Exception {
    String plainToken = "dapi-secret-token";
    Variables variables = new Variables();
    variables.setVariable("DBX_HOST", "http://localhost:" + server.port());
    variables.setVariable("DBX_TOKEN", Encr.encryptPasswordIfNotUsingVariables(plainToken));

    DatabricksConnection connection = new DatabricksConnection();
    connection.setHost("${DBX_HOST}");
    connection.setToken("${DBX_TOKEN}");
    connection.setApiBasePath("/api/2.1");

    server.stubFor(
        WireMock.get(WireMock.urlEqualTo("/api/2.0/preview/scim/v2/Me"))
            .withHeader("Authorization", WireMock.equalTo("Bearer " + plainToken))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"userName\":\"bob@example.com\"}")));

    try (RestDatabricksJobsClient resolved =
        RestDatabricksJobsClient.create(connection, variables)) {
      assertEquals("bob@example.com", resolved.testConnection());
    }
  }

  @Test
  void createDecryptsEncryptedLiteralToken() throws Exception {
    String plainToken = "literal-pat";
    DatabricksConnection connection = new DatabricksConnection();
    connection.setHost("http://localhost:" + server.port());
    connection.setToken(Encr.encryptPasswordIfNotUsingVariables(plainToken));
    connection.setApiBasePath("/api/2.1");

    server.stubFor(
        WireMock.get(WireMock.urlEqualTo("/api/2.0/preview/scim/v2/Me"))
            .withHeader("Authorization", WireMock.equalTo("Bearer " + plainToken))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"userName\":\"carol@example.com\"}")));

    try (RestDatabricksJobsClient resolved =
        RestDatabricksJobsClient.create(connection, new Variables())) {
      assertEquals("carol@example.com", resolved.testConnection());
    }
  }

  @Test
  void sanitizeErrorRedactsBearer() {
    String s = RestDatabricksJobsClient.sanitizeError("error bearer abc.def.ghi more");
    assertTrue(s.contains("***"));
    assertFalse(s.contains("abc.def.ghi"));
  }

  @Test
  void testConnectionUsesScimMe() throws Exception {
    server.stubFor(
        WireMock.get(WireMock.urlEqualTo("/api/2.0/preview/scim/v2/Me"))
            .withHeader("Authorization", WireMock.equalTo("Bearer test-token"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"userName\":\"alice@example.com\"}")));

    assertEquals("alice@example.com", client.testConnection());
  }

  @Test
  void runNowReturnsRunId() throws Exception {
    server.stubFor(
        WireMock.post(WireMock.urlEqualTo("/api/2.1/jobs/run-now"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"run_id\": 99, \"number_in_job\": 1}")));

    long runId = client.runNow(42L, Map.of("k", "v"));
    assertEquals(99L, runId);
  }

  @Test
  void getRunParsesState() throws Exception {
    server.stubFor(
        WireMock.get(WireMock.urlPathEqualTo("/api/2.1/jobs/runs/get"))
            .withQueryParam("run_id", WireMock.equalTo("7"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        """
                        {
                          "run_id": 7,
                          "job_id": 3,
                          "run_page_url": "https://example/runs/7",
                          "state": {
                            "life_cycle_state": "TERMINATED",
                            "result_state": "SUCCESS",
                            "state_message": "ok"
                          }
                        }
                        """)));

    DatabricksRunStatus status = client.getRun(7L);
    assertEquals(7L, status.getRunId());
    assertEquals(3L, status.getJobId());
    assertTrue(status.isSuccess());
    assertEquals("SUCCESS", status.toStatusVariable());
    assertEquals("https://example/runs/7", status.getRunPageUrl());
  }

  @Test
  void httpErrorSurfacesStatus() {
    server.stubFor(
        WireMock.get(WireMock.urlEqualTo("/api/2.0/preview/scim/v2/Me"))
            .willReturn(
                WireMock.aResponse().withStatus(401).withBody("{\"error\":\"unauthorized\"}")));
    server.stubFor(
        WireMock.get(WireMock.urlPathEqualTo("/api/2.1/jobs/list"))
            .willReturn(
                WireMock.aResponse().withStatus(401).withBody("{\"error\":\"unauthorized\"}")));

    HopException ex = assertThrows(HopException.class, () -> client.testConnection());
    assertTrue(ex.getMessage().contains("401"));
  }

  @Test
  void createJobReturnsJobId() throws Exception {
    server.stubFor(
        WireMock.post(WireMock.urlEqualTo("/api/2.1/jobs/create"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"job_id\": 55}")));
    assertEquals(55L, client.createJob("{\"name\":\"n\",\"tasks\":[]}"));
  }

  @Test
  void lifeCycleTerminalHelpers() {
    assertTrue(DatabricksRunLifeCycleState.TERMINATED.isTerminal());
    assertFalse(DatabricksRunLifeCycleState.RUNNING.isTerminal());
    assertEquals(
        DatabricksRunLifeCycleState.PENDING, DatabricksRunLifeCycleState.fromApi("pending"));
  }

  @Test
  void isFilesApiPathDetectsVolumesAndWorkspace() {
    assertTrue(RestDatabricksJobsClient.isFilesApiPath("/Volumes/c/s/v/file.jar"));
    assertTrue(RestDatabricksJobsClient.isFilesApiPath("/Workspace/Users/a@b.com/x"));
    assertFalse(RestDatabricksJobsClient.isFilesApiPath("/FileStore/hop/x.jar"));
    assertFalse(RestDatabricksJobsClient.isFilesApiPath("/tmp/x"));
  }

  @Test
  void normalizeDbfsPathStripsSchemeAndSelectsVolumes() throws Exception {
    assertEquals(
        "/Volumes/c/s/v/j.jar",
        RestDatabricksJobsClient.normalizeDbfsPath("dbfs:/Volumes/c/s/v/j.jar"));
    assertEquals(
        "/FileStore/hop/j.jar",
        RestDatabricksJobsClient.normalizeDbfsPath("dbfs:/FileStore/hop/j.jar"));
    assertTrue(
        RestDatabricksJobsClient.isFilesApiPath(
            RestDatabricksJobsClient.normalizeDbfsPath("/Volumes/c/s/v/j.jar")));
  }

  @Test
  void encodeFilesApiPathEncodesSegments() {
    assertEquals(
        "/Volumes/apache-hop/default/jars/hop-native.jar",
        RestDatabricksJobsClient.encodeFilesApiPath(
            "/Volumes/apache-hop/default/jars/hop-native.jar"));
    assertEquals(
        "/Volumes/c/s/v/my%20file.jar",
        RestDatabricksJobsClient.encodeFilesApiPath("/Volumes/c/s/v/my file.jar"));
  }

  @Test
  void uploadVolumePathUsesFilesApiPut() throws Exception {
    Path temp = Files.createTempFile("hop-dbx-upload-", ".bin");
    try {
      byte[] payload = "hop-volume-upload".getBytes(StandardCharsets.UTF_8);
      Files.write(temp, payload);

      server.stubFor(
          WireMock.put(
                  WireMock.urlEqualTo(
                      "/api/2.0/fs/files/Volumes/apache-hop/default/jars/hop-native.jar?overwrite=true"))
              .withHeader("Authorization", WireMock.equalTo("Bearer test-token"))
              .withHeader("Content-Type", WireMock.equalTo("application/octet-stream"))
              .withRequestBody(WireMock.binaryEqualTo(payload))
              .willReturn(WireMock.aResponse().withStatus(204)));

      client.uploadToDbfs(temp, "/Volumes/apache-hop/default/jars/hop-native.jar");

      server.verify(
          WireMock.putRequestedFor(
              WireMock.urlEqualTo(
                  "/api/2.0/fs/files/Volumes/apache-hop/default/jars/hop-native.jar?overwrite=true")));
      server.verify(0, WireMock.postRequestedFor(WireMock.urlEqualTo("/api/2.0/dbfs/create")));
    } finally {
      Files.deleteIfExists(temp);
    }
  }

  @Test
  void getFileMetadataFilesApiUsesHeadContentLength() throws Exception {
    server.stubFor(
        WireMock.head(
                WireMock.urlEqualTo(
                    "/api/2.0/fs/files/Volumes/apache-hop/default/jars/hop-native.jar"))
            .withHeader("Authorization", WireMock.equalTo("Bearer test-token"))
            .willReturn(
                WireMock.aResponse().withStatus(200).withHeader("Content-Length", "84200123")));

    WorkspaceFileMetadata meta =
        client.getFileMetadata("/Volumes/apache-hop/default/jars/hop-native.jar");
    assertTrue(meta.exists());
    assertEquals(84200123L, meta.sizeBytes());
  }

  @Test
  void getFileMetadataMissingReturnsNotExists() throws Exception {
    server.stubFor(
        WireMock.head(WireMock.urlPathMatching("/api/2.0/fs/files/Volumes/.*missing.jar"))
            .willReturn(WireMock.aResponse().withStatus(404)));
    server.stubFor(
        WireMock.get(WireMock.urlPathMatching("/api/2.0/fs/files/Volumes/.*missing.jar"))
            .willReturn(WireMock.aResponse().withStatus(404)));

    WorkspaceFileMetadata meta = client.getFileMetadata("/Volumes/c/s/v/missing.jar");
    assertFalse(meta.exists());
  }

  @Test
  void downloadTextIfExistsReturnsSidecar() throws Exception {
    String hash = "a".repeat(64);
    server.stubFor(
        WireMock.get(
                WireMock.urlEqualTo(
                    "/api/2.0/fs/files/Volumes/apache-hop/default/jars/hop-native.jar.sha256"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "text/plain")
                    .withBody(hash + "\n")));

    assertEquals(
        Optional.of(hash + "\n"),
        client.downloadTextIfExists("/Volumes/apache-hop/default/jars/hop-native.jar.sha256"));
  }

  @Test
  void uploadFileStorePathUsesDbfsApi() throws Exception {
    Path temp = Files.createTempFile("hop-dbx-upload-dbfs-", ".bin");
    try {
      Files.writeString(temp, "x");

      server.stubFor(
          WireMock.post(WireMock.urlEqualTo("/api/2.0/dbfs/create"))
              .willReturn(
                  WireMock.aResponse()
                      .withStatus(200)
                      .withHeader("Content-Type", "application/json")
                      .withBody("{\"handle\": 1}")));
      server.stubFor(
          WireMock.post(WireMock.urlEqualTo("/api/2.0/dbfs/add-block"))
              .willReturn(WireMock.aResponse().withStatus(200).withBody("{}")));
      server.stubFor(
          WireMock.post(WireMock.urlEqualTo("/api/2.0/dbfs/close"))
              .willReturn(WireMock.aResponse().withStatus(200).withBody("{}")));

      client.uploadToDbfs(temp, "dbfs:/FileStore/hop/hop-native.jar");

      server.verify(WireMock.postRequestedFor(WireMock.urlEqualTo("/api/2.0/dbfs/create")));
      server.verify(0, WireMock.putRequestedFor(WireMock.urlPathMatching("/api/2.0/fs/files/.*")));
    } finally {
      Files.deleteIfExists(temp);
    }
  }
}
