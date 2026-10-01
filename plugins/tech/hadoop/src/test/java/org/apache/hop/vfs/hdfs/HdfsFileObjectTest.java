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
package org.apache.hop.vfs.hdfs;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.FileType;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hop.vfs.hdfs.client.HdfsWebHdfsClient;
import org.apache.hop.vfs.hdfs.client.WebHdfsTestServer;
import org.junit.jupiter.api.Test;

class HdfsFileObjectTest {

  @Test
  void missingFileAfterStandbyFailoverCanBeCreated() throws Exception {
    WebHdfsTestServer standby = new WebHdfsTestServer();
    WebHdfsTestServer active = new WebHdfsTestServer();
    standby.start();
    active.start();
    standby.setStandby(true);
    var executor = Executors.newCachedThreadPool();
    try {
      HdfsFileObject file =
          fileObject(
              standby.endpoint(), active.endpoint(), "/vdp/matt/random-data_0.csv", executor);
      assertFalse(file.exists());
      assertFalse(file.exists());
      try (OutputStream out = file.doGetOutputStream(false)) {
        out.write("hello".getBytes(StandardCharsets.UTF_8));
      }
      assertArrayEquals(
          "hello".getBytes(StandardCharsets.UTF_8), active.file("/vdp/matt/random-data_0.csv"));
    } finally {
      executor.shutdownNow();
      standby.stop();
      active.stop();
    }
  }

  @Test
  void retryAfterUnreachableClusterDoesNotOverflow() throws Exception {
    WebHdfsTestServer standby = new WebHdfsTestServer();
    standby.start();
    standby.setStandby(true);
    var executor = Executors.newCachedThreadPool();
    try {
      HdfsFileObject file = fileObject(standby.endpoint(), "/vdp/matt/random-data_0.csv", executor);
      assertThrows(FileSystemException.class, file::exists);
      assertThrows(FileSystemException.class, file::exists);
    } finally {
      executor.shutdownNow();
      standby.stop();
    }
  }

  private static HdfsFileObject fileObject(String endpoint, String path, ExecutorService executor)
      throws Exception {
    return fileObject(endpoint, null, path, executor);
  }

  private static HdfsFileObject fileObject(
      String first, String second, String path, ExecutorService executor) throws Exception {
    List<String> endpoints = second == null ? List.of(first) : List.of(first, second);
    HdfsWebHdfsClient client =
        new HdfsWebHdfsClient(
            HttpClientBuilder.create()
                .disableContentCompression()
                .disableRedirectHandling()
                .build(),
            HdfsTransport.WebHDFS,
            endpoints,
            "http",
            "/webhdfs/v1",
            "hop",
            false,
            null,
            executor);
    HdfsFileName root = new HdfsFileName("cdp", "/", FileType.FOLDER);
    HdfsFileSystem fs = new HdfsFileSystem(root, new FileSystemOptions());
    fs.setClient(client);
    return new HdfsFileObject(new HdfsFileName("cdp", path, FileType.FILE), fs);
  }
}
