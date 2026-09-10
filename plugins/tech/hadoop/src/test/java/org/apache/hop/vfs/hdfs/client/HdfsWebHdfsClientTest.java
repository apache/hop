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
package org.apache.hop.vfs.hdfs.client;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.security.PrivilegedExceptionAction;
import java.util.List;
import java.util.concurrent.Executors;
import javax.security.auth.login.LoginException;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.vfs.hdfs.HdfsTransport;
import org.apache.hop.vfs.hdfs.kerberos.HdfsKerberosSession;
import org.apache.hop.vfs.hdfs.metadata.HdfsMeta;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class HdfsWebHdfsClientTest {
  private WebHdfsTestServer server;
  private HdfsWebHdfsClient httpfs;
  private HdfsWebHdfsClient webhdfs;

  @BeforeEach
  void setUp() throws Exception {
    server = new WebHdfsTestServer();
    server.start();
    var executor = Executors.newCachedThreadPool();
    var http =
        HttpClientBuilder.create().disableContentCompression().disableRedirectHandling().build();
    List<String> endpoints = List.of(server.endpoint());
    httpfs =
        new HdfsWebHdfsClient(
            http,
            HdfsTransport.HttpFS,
            endpoints,
            "http",
            "/webhdfs/v1",
            "hop",
            false,
            null,
            executor);
    webhdfs =
        new HdfsWebHdfsClient(
            http,
            HdfsTransport.WebHDFS,
            endpoints,
            "http",
            "/webhdfs/v1",
            "hop",
            false,
            null,
            executor);
  }

  @AfterEach
  void tearDown() {
    server.stop();
  }

  @Test
  void encodePathKeepsSlashesAndEncodesSpaces() {
    assertEquals("/", HdfsWebHdfsClient.encodePath("/"));
    assertEquals("/user/hop", HdfsWebHdfsClient.encodePath("/user/hop"));
    assertEquals("/user/hop/a%20b", HdfsWebHdfsClient.encodePath("/user/hop/a b"));
  }

  @Test
  void endpointListParsesExtraHosts() {
    List<String> list = HdfsWebHdfsClient.endpointList("nn1", 9870, "nn2:9870\nnn3");
    assertEquals(List.of("nn1:9870", "nn2:9870", "nn3:9870"), list);
  }

  @Test
  void endpointListParsesCommaSeparatedHaHosts() {
    List<String> list =
        HdfsWebHdfsClient.endpointList("master1.example.com,master2.example.com", 9871, null);
    assertEquals(List.of("master1.example.com:9871", "master2.example.com:9871"), list);
    assertEquals("master1.example.com", HdfsWebHdfsClient.hostOfEndpoint(list.get(0)));
  }

  @Test
  void getFileStatusFailsOverFromStandbyNamenode() throws Exception {
    WebHdfsTestServer standbyNn = new WebHdfsTestServer();
    WebHdfsTestServer activeNn = new WebHdfsTestServer();
    standbyNn.start();
    activeNn.start();
    standbyNn.setStandby(true);
    var executor = Executors.newCachedThreadPool();
    try {
      HdfsWebHdfsClient client =
          new HdfsWebHdfsClient(
              HttpClientBuilder.create()
                  .disableContentCompression()
                  .disableRedirectHandling()
                  .build(),
              HdfsTransport.WebHDFS,
              List.of(standbyNn.endpoint(), activeNn.endpoint()),
              "http",
              "/webhdfs/v1",
              "hop",
              false,
              null,
              executor);
      HdfsFileStatus status = client.getFileStatus("/");
      assertTrue(status.isDirectory());
      assertEquals(1, standbyNn.requestCount());
      assertEquals(1, activeNn.requestCount());

      assertTrue(client.getFileStatus("/").isDirectory());
      assertEquals(1, standbyNn.requestCount(), "later calls must skip the known standby");
      assertEquals(2, activeNn.requestCount());
    } finally {
      executor.shutdownNow();
      standbyNn.stop();
      activeNn.stop();
    }
  }

  @Test
  void httpfsMkdirListWriteReadDelete() throws Exception {
    httpfs.mkdirs("/it");
    HdfsFileStatus dir = httpfs.getFileStatus("/it");
    assertTrue(dir.isDirectory());

    try (OutputStream out = httpfs.create("/it/hello.txt", true)) {
      out.write("hello parquet".getBytes(StandardCharsets.UTF_8));
    }
    assertArrayEquals(
        "hello parquet".getBytes(StandardCharsets.UTF_8), server.file("/it/hello.txt"));

    try (InputStream in = httpfs.open("/it/hello.txt")) {
      assertEquals("hello parquet", new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }

    List<HdfsFileStatus> children = httpfs.listStatus("/it");
    assertEquals(1, children.size());
    assertEquals("hello.txt", children.get(0).getPathSuffix());
    assertFalse(children.get(0).isDirectory());

    httpfs.delete("/it/hello.txt", false);
  }

  @Test
  void webhdfsCreateUsesNoredirectThenPut() throws Exception {
    try (OutputStream out = webhdfs.create("/warehouse/data.parquet", true)) {
      out.write(new byte[2048]);
    }
    assertEquals(2048, server.file("/warehouse/data.parquet").length);
  }

  @Test
  void webhdfsOpenFollowsNoredirectLocation() throws Exception {
    try (OutputStream out = webhdfs.create("/warehouse/read.txt", true)) {
      out.write("hello parquet".getBytes(StandardCharsets.UTF_8));
    }
    try (InputStream in = webhdfs.open("/warehouse/read.txt")) {
      assertEquals("hello parquet", new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void webhdfsOpenFollows307WithEmptyBody() throws Exception {
    server.setOpenUses307(true);
    try (OutputStream out = webhdfs.create("/warehouse/redirect.txt", true)) {
      out.write("not empty".getBytes(StandardCharsets.UTF_8));
    }
    try (InputStream in = webhdfs.open("/warehouse/redirect.txt")) {
      assertEquals("not empty", new String(in.readAllBytes(), StandardCharsets.UTF_8));
    }
  }

  @Test
  void spnegoIsSkippedForDatanodeLocation() {
    HdfsMeta meta = new HdfsMeta();
    meta.setPrincipal("hop@EXAMPLE.COM");
    meta.setKeytabPath("/tmp/hop.keytab");
    TrackingSession session = new TrackingSession(meta);
    var executor = Executors.newCachedThreadPool();
    try {
      HdfsWebHdfsClient client =
          new HdfsWebHdfsClient(
              HttpClientBuilder.create().build(),
              HdfsTransport.WebHDFS,
              List.of("master1.example.com:9871"),
              "https",
              "/webhdfs/v1",
              "hop",
              true,
              session,
              executor);
      assertTrue(client.shouldSpnegoForLocation("https://master1.example.com:9871/webhdfs/v1/x"));
      assertFalse(client.shouldSpnegoForLocation("http://datanode.example.com:9864/webhdfs/v1/x"));
      assertFalse(session.doAsCalled);
    } finally {
      executor.shutdownNow();
    }
  }

  @Test
  void httpsConnectionRejectsHttpDatanodeLocation() {
    IOException error =
        assertThrows(
            IOException.class,
            () ->
                HdfsWebHdfsClient.rejectHttpDowngrade(
                    "https", false, "http://datanode.example.com:9864/webhdfs/v1/file"));
    assertTrue(error.getMessage().contains("http://datanode.example.com:9864"));
  }

  @Test
  void httpsConnectionAllowsHttpDatanodeLocationWhenOptedIn() throws Exception {
    HdfsWebHdfsClient.rejectHttpDowngrade(
        "https", true, "http://datanode.example.com:9864/webhdfs/v1/file");
    HdfsWebHdfsClient.rejectHttpDowngrade(
        "https", false, "https://datanode.example.com:9865/webhdfs/v1/file");
    HdfsWebHdfsClient.rejectHttpDowngrade(
        "http", false, "http://datanode.example.com:9864/webhdfs/v1/file");
  }

  @Test
  void spnegoTokenIsBuiltInsideDoAs() {
    HdfsMeta meta = new HdfsMeta();
    meta.setName("t");
    meta.setPrincipal("hop@EXAMPLE.COM");
    meta.setKeytabPath("/tmp/hop.keytab");
    TrackingSession session = new TrackingSession(meta);
    var executor = Executors.newCachedThreadPool();
    try {
      HdfsWebHdfsClient client =
          new HdfsWebHdfsClient(
              HttpClientBuilder.create()
                  .disableContentCompression()
                  .disableRedirectHandling()
                  .build(),
              HdfsTransport.WebHDFS,
              List.of("master1.example.com:9871"),
              "https",
              "/webhdfs/v1",
              "hop",
              true,
              session,
              executor);
      IOException error = assertThrows(IOException.class, () -> client.getFileStatus("/"));
      assertTrue(session.doAsCalled, "SPNEGO must run inside Subject.doAs so the TGT is visible");
      assertTrue(error.getMessage().contains("doAs-was-called"));
    } finally {
      executor.shutdownNow();
    }
  }

  static class TrackingSession extends HdfsKerberosSession {
    boolean doAsCalled;

    TrackingSession(HdfsMeta meta) {
      super(new Variables(), meta);
    }

    @Override
    public <T> T doAs(PrivilegedExceptionAction<T> action) throws Exception {
      doAsCalled = true;
      throw new LoginException("doAs-was-called");
    }
  }

  @Test
  void buildUriAddsSimpleUser() throws Exception {
    String uri =
        httpfs.buildUri(server.endpoint(), "/user/hop", java.util.Map.of("op", "GETFILESTATUS"));
    assertTrue(uri.contains("user.name=hop"));
    assertTrue(uri.contains("/webhdfs/v1/user/hop"));
    assertTrue(uri.startsWith("http://"));
  }
}
