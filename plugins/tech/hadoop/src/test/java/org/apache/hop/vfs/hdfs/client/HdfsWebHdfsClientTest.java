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
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.Executors;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hop.vfs.hdfs.HdfsTransport;
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
    var http = HttpClients.createDefault();
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
  void buildUriAddsSimpleUser() throws Exception {
    String uri =
        httpfs.buildUri(server.endpoint(), "/user/hop", java.util.Map.of("op", "GETFILESTATUS"));
    assertTrue(uri.contains("user.name=hop"));
    assertTrue(uri.contains("/webhdfs/v1/user/hop"));
    assertTrue(uri.startsWith("http://"));
  }
}
