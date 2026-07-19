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

package org.apache.hop.vfs.databricks;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.github.tomakehurst.wiremock.WireMockServer;
import com.github.tomakehurst.wiremock.client.WireMock;
import com.github.tomakehurst.wiremock.core.WireMockConfiguration;
import java.io.OutputStream;
import java.net.http.HttpClient;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collection;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileObject;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.commons.vfs2.impl.DefaultFileSystemManager;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.hop.databricks.client.RestDatabricksFilesClient;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class DatabricksFileObjectTest {

  private WireMockServer server;
  private RestDatabricksFilesClient client;
  private DefaultFileSystemManager fsm;
  private DefaultFileSystemManager rootedFsm;

  @BeforeEach
  void setUp() throws Exception {
    server = new WireMockServer(WireMockConfiguration.wireMockConfig().dynamicPort());
    server.start();
    WireMock.configureFor("localhost", server.port());
    client =
        RestDatabricksFilesClient.createForTest(
            "http://localhost:" + server.port(), "test-token", HttpClient.newHttpClient());

    fsm = new DefaultFileSystemManager();
    fsm.addProvider("testdbx", new TestProvider(client, ""));
    fsm.init();

    rootedFsm = new DefaultFileSystemManager();
    rootedFsm.addProvider("dbvol", new TestProvider(client, "/Volumes/apache-hop/default/testing"));
    rootedFsm.init();
  }

  @AfterEach
  void tearDown() {
    if (fsm != null) {
      fsm.close();
    }
    if (rootedFsm != null) {
      rootedFsm.close();
    }
    if (server != null) {
      server.stop();
    }
  }

  private FileObject resolve(String path) throws Exception {
    return fsm.resolveFile("testdbx://" + path);
  }

  private FileObject resolveRooted(String path) throws Exception {
    return rootedFsm.resolveFile("dbvol://" + path);
  }

  @Test
  void listDirectoryReturnsChildren() throws Exception {
    server.stubFor(
        WireMock.head(WireMock.urlEqualTo("/api/2.0/fs/files/Volumes/c/s/v"))
            .willReturn(WireMock.aResponse().withStatus(404)));
    server.stubFor(
        WireMock.get(WireMock.urlEqualTo("/api/2.0/fs/files/Volumes/c/s/v"))
            .willReturn(WireMock.aResponse().withStatus(404)));
    server.stubFor(
        WireMock.get(WireMock.urlEqualTo("/api/2.0/fs/directories/Volumes/c/s/v"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody(
                        """
                        {"contents":[
                          {"name":"data.csv","path":"/Volumes/c/s/v/data.csv","is_directory":false,"file_size":42,"last_modified":1},
                          {"name":"subdir","path":"/Volumes/c/s/v/subdir","is_directory":true,"last_modified":2}
                        ]}
                        """)));

    FileObject dir = resolve("/Volumes/c/s/v");
    assertEquals(FileType.FOLDER, dir.getType());
    String[] names =
        Arrays.stream(dir.getChildren())
            .map(fo -> fo.getName().getBaseName())
            .sorted()
            .toArray(String[]::new);
    assertArrayEquals(new String[] {"data.csv", "subdir"}, names);
  }

  @Test
  void writeThenReadRoundTrip() throws Exception {
    byte[] payload = "vfs-round-trip".getBytes(StandardCharsets.UTF_8);
    server.stubFor(
        WireMock.head(WireMock.urlEqualTo("/api/2.0/fs/files/Volumes/c/s/v/out.txt"))
            .willReturn(WireMock.aResponse().withStatus(404)));
    server.stubFor(
        WireMock.get(WireMock.urlEqualTo("/api/2.0/fs/files/Volumes/c/s/v/out.txt"))
            .willReturn(WireMock.aResponse().withStatus(404)));
    server.stubFor(
        WireMock.get(WireMock.urlEqualTo("/api/2.0/fs/directories/Volumes/c/s/v/out.txt"))
            .willReturn(WireMock.aResponse().withStatus(404)));
    server.stubFor(
        WireMock.put(WireMock.urlEqualTo("/api/2.0/fs/files/Volumes/c/s/v/out.txt?overwrite=true"))
            .withRequestBody(WireMock.binaryEqualTo(payload))
            .willReturn(WireMock.aResponse().withStatus(204)));

    FileObject file = resolve("/Volumes/c/s/v/out.txt");
    try (OutputStream out = file.getContent().getOutputStream()) {
      out.write(payload);
    }

    server.stubFor(
        WireMock.head(WireMock.urlEqualTo("/api/2.0/fs/files/Volumes/c/s/v/out.txt"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Length", String.valueOf(payload.length))));
    server.stubFor(
        WireMock.get(WireMock.urlEqualTo("/api/2.0/fs/files/Volumes/c/s/v/out.txt"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/octet-stream")
                    .withBody(payload)));

    fsm.getFilesCache().clear(file.getFileSystem());
    FileObject written = resolve("/Volumes/c/s/v/out.txt");
    assertEquals(FileType.FILE, written.getType());
    try (var in = written.getContent().getInputStream()) {
      assertEquals("vfs-round-trip", new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void createFolderUnderRootPathUsesJoinedWorkspacePath() throws Exception {
    server.stubFor(
        WireMock.get(
                WireMock.urlEqualTo("/api/2.0/fs/directories/Volumes/apache-hop/default/testing"))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/json")
                    .withBody("{\"contents\":[]}")));
    server.stubFor(
        WireMock.put(
                WireMock.urlEqualTo(
                    "/api/2.0/fs/directories/Volumes/apache-hop/default/testing/input/"))
            .withHeader("Authorization", WireMock.equalTo("Bearer test-token"))
            .willReturn(WireMock.aResponse().withStatus(204)));

    FileObject folder = resolveRooted("/input");
    folder.createFolder();

    server.verify(
        WireMock.putRequestedFor(
            WireMock.urlEqualTo(
                "/api/2.0/fs/directories/Volumes/apache-hop/default/testing/input/")));
    assertTrue(folder.exists());
  }

  @Test
  void renameFileCopiesThenDeletesViaFilesApi() throws Exception {
    byte[] payload = "rename-me".getBytes(StandardCharsets.UTF_8);
    String src = "/Volumes/apache-hop/default/testing/input/a.txt";
    String dst = "/Volumes/apache-hop/default/testing/processing/a.txt";

    server.stubFor(
        WireMock.head(WireMock.urlEqualTo("/api/2.0/fs/files" + src))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Length", String.valueOf(payload.length))));
    server.stubFor(
        WireMock.get(WireMock.urlEqualTo("/api/2.0/fs/files" + src))
            .willReturn(
                WireMock.aResponse()
                    .withStatus(200)
                    .withHeader("Content-Type", "application/octet-stream")
                    .withBody(payload)));
    server.stubFor(
        WireMock.put(WireMock.urlEqualTo("/api/2.0/fs/files" + dst + "?overwrite=true"))
            .withRequestBody(WireMock.binaryEqualTo(payload))
            .willReturn(WireMock.aResponse().withStatus(204)));
    server.stubFor(
        WireMock.delete(WireMock.urlEqualTo("/api/2.0/fs/files" + src))
            .willReturn(WireMock.aResponse().withStatus(204)));

    FileObject source = resolveRooted("/input/a.txt");
    FileObject target = resolveRooted("/processing/a.txt");
    source.moveTo(target);

    server.verify(
        WireMock.putRequestedFor(
            WireMock.urlEqualTo("/api/2.0/fs/files" + dst + "?overwrite=true")));
    server.verify(WireMock.deleteRequestedFor(WireMock.urlEqualTo("/api/2.0/fs/files" + src)));
  }

  /** Provider that injects a pre-built Files client and optional scheme root. */
  private static final class TestProvider extends AbstractOriginatingFileProvider {
    private final RestDatabricksFilesClient client;
    private final String rootPath;

    TestProvider(RestDatabricksFilesClient client, String rootPath) {
      this.client = client;
      this.rootPath = rootPath == null ? "" : rootPath;
      setFileNameParser(DatabricksFileNameParser.getInstance());
    }

    @Override
    public Collection<Capability> getCapabilities() {
      return DatabricksFileProvider.CAPABILITIES;
    }

    @Override
    protected FileSystem doCreateFileSystem(FileName rootName, FileSystemOptions fileSystemOptions)
        throws FileSystemException {
      FileSystemOptions opts =
          fileSystemOptions != null ? fileSystemOptions : new FileSystemOptions();
      return new DatabricksFileSystem(rootName, client, opts, rootPath);
    }
  }
}
