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
import java.io.InputStream;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class RestDatabricksFilesClientTest {

  private WireMockServer server;
  private RestDatabricksFilesClient client;

  @BeforeEach
  void setUp() {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    server.start();
    WireMock.configureFor("localhost", server.port());
    client =
        RestDatabricksFilesClient.createForTest(
            "http://localhost:" + server.port(), "test-token", HttpClient.newHttpClient());
  }

  @AfterEach
  void tearDown() {
    if (server != null) {
      server.stop();
    }
  }

  @Test
  void isFilesApiPathDetectsVolumesAndWorkspace() {
    assertTrue(RestDatabricksFilesClient.isFilesApiPath("/Volumes/c/s/v/file.jar"));
    assertTrue(RestDatabricksFilesClient.isFilesApiPath("/Workspace/Users/a@b.com/x"));
    assertFalse(RestDatabricksFilesClient.isFilesApiPath("/FileStore/hop/x.jar"));
  }

  @Test
  void requireFilesApiPathRejectsClassicDbfs() {
    HopException ex =
        assertThrows(
            HopException.class,
            () -> RestDatabricksFilesClient.requireFilesApiPath("/FileStore/hop/x.jar"));
    assertTrue(ex.getMessage().contains("Files API"));
  }

  @Test
  void listDirectoryPaginates() throws Exception {
    server.stubFor(
        WireMock.get(WireMock.urlEqualTo("/api/2.0/fs/directories/Volumes/apache-hop/default/jars"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        """
                        {"contents":[
                          {"name":"a.jar","path":"/Volumes/apache-hop/default/jars/a.jar","is_directory":false,"file_size":10,"last_modified":1000}
                        ],"next_page_token":"page2"}
                        """)));
    server.stubFor(
        WireMock.get(
                WireMock.urlEqualTo(
                    "/api/2.0/fs/directories/Volumes/apache-hop/default/jars?page_token=page2"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        """
                        {"contents":[
                          {"name":"sub","path":"/Volumes/apache-hop/default/jars/sub","is_directory":true,"last_modified":2000}
                        ]}
                        """)));

    List<DirectoryEntry> entries = client.listDirectory("/Volumes/apache-hop/default/jars");
    assertEquals(2, entries.size());
    assertEquals("a.jar", entries.get(0).name());
    assertFalse(entries.get(0).directory());
    assertEquals(10L, entries.get(0).sizeBytes());
    assertEquals("sub", entries.get(1).name());
    assertTrue(entries.get(1).directory());
  }

  @Test
  void createDirectoryPutsWithTrailingSlash() throws Exception {
    server.stubFor(
        WireMock.put(
                WireMock.urlEqualTo(
                    "/api/2.0/fs/directories/Volumes/apache-hop/default/jars/newdir/"))
            .withHeader("Authorization", WireMock.equalTo("Bearer test-token"))
            .willReturn(WireMock.aResponse().withStatus(204)));

    client.createDirectory("/Volumes/apache-hop/default/jars/newdir");

    server.verify(
        WireMock.putRequestedFor(
            WireMock.urlEqualTo(
                "/api/2.0/fs/directories/Volumes/apache-hop/default/jars/newdir/")));
  }

  @Test
  void deleteFileUsesFilesApi() throws Exception {
    server.stubFor(
        WireMock.delete(
                WireMock.urlEqualTo("/api/2.0/fs/files/Volumes/apache-hop/default/jars/old.jar"))
            .willReturn(WireMock.aResponse().withStatus(204)));

    client.deleteFile("/Volumes/apache-hop/default/jars/old.jar");

    server.verify(
        WireMock.deleteRequestedFor(
            WireMock.urlEqualTo("/api/2.0/fs/files/Volumes/apache-hop/default/jars/old.jar")));
  }

  @Test
  void deleteDirectoryDoesNotSendRecursiveQueryParam() throws Exception {
    // Empty directory: no list children needed beyond empty contents
    server.stubFor(
        WireMock.get(
                WireMock.urlEqualTo("/api/2.0/fs/directories/Volumes/apache-hop/default/jars/tmp"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"contents\":[]}")));
    server.stubFor(
        WireMock.delete(
                WireMock.urlEqualTo("/api/2.0/fs/directories/Volumes/apache-hop/default/jars/tmp/"))
            .willReturn(WireMock.aResponse().withStatus(204)));

    client.deleteDirectory("/Volumes/apache-hop/default/jars/tmp", true);

    server.verify(
        WireMock.deleteRequestedFor(
            WireMock.urlEqualTo("/api/2.0/fs/directories/Volumes/apache-hop/default/jars/tmp/")));
    // API rejects ?recursive=…
    server.verify(
        0, WireMock.deleteRequestedFor(WireMock.urlMatching(".*/fs/directories/.*recursive.*")));
  }

  @Test
  void deleteDirectoryRecursiveDeletesChildrenThenDir() throws Exception {
    server.stubFor(
        WireMock.get(
                WireMock.urlEqualTo("/api/2.0/fs/directories/Volumes/apache-hop/default/jars/tmp"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        """
                        {"contents":[
                          {"name":"f.txt","path":"/Volumes/apache-hop/default/jars/tmp/f.txt","is_directory":false,"file_size":1}
                        ]}
                        """)));
    server.stubFor(
        WireMock.delete(
                WireMock.urlEqualTo("/api/2.0/fs/files/Volumes/apache-hop/default/jars/tmp/f.txt"))
            .willReturn(WireMock.aResponse().withStatus(204)));
    server.stubFor(
        WireMock.delete(
                WireMock.urlEqualTo("/api/2.0/fs/directories/Volumes/apache-hop/default/jars/tmp/"))
            .willReturn(WireMock.aResponse().withStatus(204)));

    client.deleteDirectory("/Volumes/apache-hop/default/jars/tmp", true);

    server.verify(
        WireMock.deleteRequestedFor(
            WireMock.urlEqualTo("/api/2.0/fs/files/Volumes/apache-hop/default/jars/tmp/f.txt")));
    server.verify(
        WireMock.deleteRequestedFor(
            WireMock.urlEqualTo("/api/2.0/fs/directories/Volumes/apache-hop/default/jars/tmp/")));
  }

  @Test
  void openInputStreamDownloadsBytes() throws Exception {
    byte[] payload = "hello-volume".getBytes(StandardCharsets.UTF_8);
    server.stubFor(
        WireMock.get(
                WireMock.urlEqualTo("/api/2.0/fs/files/Volumes/apache-hop/default/jars/hello.txt"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/octet-stream")
                    .withBody(payload)));

    try (InputStream in = client.openInputStream("/Volumes/apache-hop/default/jars/hello.txt")) {
      assertEquals("hello-volume", new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void uploadUsesPutOverwrite() throws Exception {
    Path temp = Files.createTempFile("hop-dbx-files-", ".bin");
    try {
      byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
      Files.write(temp, payload);
      server.stubFor(
          WireMock.put(
                  WireMock.urlEqualTo(
                      "/api/2.0/fs/files/Volumes/apache-hop/default/jars/x.bin?overwrite=true"))
              .withRequestBody(WireMock.binaryEqualTo(payload))
              .willReturn(WireMock.aResponse().withStatus(204)));

      client.upload(temp, "/Volumes/apache-hop/default/jars/x.bin");

      server.verify(
          WireMock.putRequestedFor(
              WireMock.urlEqualTo(
                  "/api/2.0/fs/files/Volumes/apache-hop/default/jars/x.bin?overwrite=true")));
    } finally {
      Files.deleteIfExists(temp);
    }
  }
}
