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
package org.apache.hop.vfs.sftp;

import static org.junit.jupiter.api.Assertions.assertEquals;

import org.apache.hop.vfs.sftp.metadata.SftpConnection;
import org.junit.jupiter.api.Test;

class SftpConnectionsTest {

  private final SftpConnection connection = connection();

  @Test
  void testBuildUri() {
    assertEquals(
        "prod://tmp/upload/file.txt",
        SftpConnections.buildUri(connection, "/tmp/upload", "file.txt"));
    assertEquals(
        "prod://tmp/upload/file.txt",
        SftpConnections.buildUri(connection, "/tmp/upload/", "file.txt"));
    assertEquals(
        "prod://tmp/upload/file.txt",
        SftpConnections.buildUri(connection, "tmp/upload", "file.txt"));
  }

  @Test
  void testBuildUriWithoutFolderOrFile() {
    assertEquals("prod://file.txt", SftpConnections.buildUri(connection, null, "file.txt"));
    assertEquals("prod://file.txt", SftpConnections.buildUri(connection, "", "file.txt"));
    assertEquals("prod://tmp/upload", SftpConnections.buildUri(connection, "/tmp/upload", null));
    assertEquals("prod://", SftpConnections.buildUri(connection, null, null));
  }

  /** Windows style folders make it into the pipelines of people migrating from Kettle. */
  @Test
  void testBuildUriWithBackslashes() {
    assertEquals(
        "prod://tmp/upload/file.txt",
        SftpConnections.buildUri(connection, "\\tmp\\upload", "file.txt"));
  }

  private SftpConnection connection() {
    SftpConnection sftpConnection = new SftpConnection();
    sftpConnection.setName("prod");
    sftpConnection.setServerName("sftp.example.com");
    return sftpConnection;
  }
}
