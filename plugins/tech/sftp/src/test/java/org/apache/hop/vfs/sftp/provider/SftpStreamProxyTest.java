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

package org.apache.hop.vfs.sftp.provider;

import static org.junit.jupiter.api.Assertions.assertSame;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyInt;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.mockStatic;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.Session;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import org.apache.commons.vfs2.FileSystemOptions;
import org.junit.jupiter.api.Test;
import org.mockito.InOrder;
import org.mockito.MockedStatic;

class SftpStreamProxyTest {

  @Test
  void connectAttachesTheInputStreamBeforeConnectingTheChannel() throws Exception {
    // JSch drops output that arrives before a stream is attached: the target server's SSH greeting
    // could be lost before the session read it
    Session session = mock(Session.class);
    ChannelExec channel = mock(ChannelExec.class);
    InputStream input = new ByteArrayInputStream(new byte[0]);
    when(session.openChannel("exec")).thenReturn(channel);
    when(channel.getInputStream()).thenReturn(input);

    SftpStreamProxy proxy =
        new SftpStreamProxy(
            SftpStreamProxy.NETCAT_COMMAND, "user", "proxy", 22, "secret", new FileSystemOptions());
    try (MockedStatic<SftpClientFactory> factory = mockStatic(SftpClientFactory.class)) {
      factory
          .when(
              () ->
                  SftpClientFactory.createConnection(
                      anyString(), anyInt(), any(char[].class), any(char[].class), any()))
          .thenReturn(session);

      proxy.connect(null, "target", 2222, 1000);
    }

    verify(channel).setCommand("nc -q 0 target 2222");
    InOrder order = inOrder(channel);
    order.verify(channel).getInputStream();
    order.verify(channel).connect(1000);
    assertSame(input, proxy.getInputStream());
  }
}
