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

package org.apache.hop.vfs.sftp.provider;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.jcraft.jsch.ChannelSftp;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.TestIdentityRepositoryFactory;
import java.time.Duration;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.GenericFileName;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * JSch gives no channel, instead of throwing, while a session is being disconnected - by a server
 * that drops an idle session, say - and the session still says it is connected. The file system
 * then opens a new session, rather than failing with a NullPointerException on the missing channel.
 */
class SftpSessionGoingDownTest {

  @BeforeAll
  static void startServer() throws Exception {
    SftpTestServerHelper.startServer();
  }

  @AfterAll
  static void stopServer() {
    SftpTestServerHelper.stopServer();
  }

  @Test
  void anSftpChannelComesFromANewSessionWhenTheOldOneIsGoingDown() throws Exception {
    Session goingDown = goingDownSession();
    SftpFileSystem fileSystem = new SftpFileSystem(rootName(), goingDown, options());
    try {
      ChannelSftp channel = fileSystem.getChannel();
      try {
        assertTrue(channel.isConnected());
        assertNotSame(goingDown, channel.getSession());
      } finally {
        channel.disconnect();
      }
    } finally {
      fileSystem.doCloseCommunicationLink();
    }
    verify(goingDown).disconnect();
  }

  @Test
  void aCommandRunsOnANewSessionWhenTheOldOneIsGoingDown() throws Exception {
    Session goingDown = goingDownSession();
    SftpFileSystem fileSystem = new SftpFileSystem(rootName(), goingDown, options());
    try {
      // The test server answers "id -u" with 1000
      assertEquals(1000, fileSystem.getUId());
    } finally {
      fileSystem.doCloseCommunicationLink();
    }
    verify(goingDown).disconnect();
  }

  /** A session as JSch has it while it is being disconnected: still connected, but no channels. */
  private static Session goingDownSession() throws Exception {
    Session session = mock(Session.class);
    when(session.isConnected()).thenReturn(true);
    when(session.openChannel(anyString())).thenReturn(null);
    return session;
  }

  private static GenericFileName rootName() throws Exception {
    return (GenericFileName)
        SftpFileNameParser.getInstance()
            .parseUri(null, null, SftpTestServerHelper.getConnectionUri() + "/");
  }

  private static FileSystemOptions options() throws Exception {
    FileSystemOptions options = new FileSystemOptions();
    SftpFileSystemConfigBuilder builder = SftpFileSystemConfigBuilder.getInstance();
    builder.setStrictHostKeyChecking(options, "no");
    builder.setUserInfo(options, new TrustEveryoneUserInfo());
    builder.setIdentityRepositoryFactory(options, new TestIdentityRepositoryFactory());
    builder.setConnectTimeout(options, Duration.ofSeconds(60));
    builder.setSessionTimeout(options, Duration.ofSeconds(60));
    // Leave the session alone while the file system is made: each test opens the channel itself
    builder.setDisableDetectExecChannel(options, true);
    return options;
  }
}
