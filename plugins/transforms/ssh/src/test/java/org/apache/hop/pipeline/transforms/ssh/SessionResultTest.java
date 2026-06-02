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

package org.apache.hop.pipeline.transforms.ssh;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.exception.HopException;
import org.junit.jupiter.api.Test;

class SessionResultTest {

  @Test
  void getStdWithStdoutOnly() {
    SessionResult result = new SessionResult();
    result.setStdOut("out");
    assertEquals("out", result.getStd());
    assertFalse(result.isStdErrorType());
  }

  @Test
  void executeCommandReadsStdoutAndStderrFromChannel() throws Exception {
    Session session = mock(Session.class);
    ChannelExec channel = mock(ChannelExec.class);
    when(session.openChannel("exec")).thenReturn(channel);
    doNothing().when(channel).connect();
    when(channel.getInputStream()).thenReturn(stream("line1\nline2\n"));
    when(channel.getErrStream()).thenReturn(stream("oops\n"));
    when(channel.isClosed()).thenReturn(true);

    SessionResult result = SessionResult.executeCommand(session, "echo test");

    assertEquals("line1\nline2\n", result.getStdOut());
    assertEquals("oops\n", result.getStdErr());
    assertTrue(result.isStdErrorType());
    assertEquals("line1\nline2\noops\n", result.getStd());
    verify(channel).setCommand("echo test");
    verify(channel).disconnect();
  }

  @Test
  void executeCommandWrapsJSchException() throws Exception {
    Session session = mock(Session.class);
    when(session.openChannel("exec")).thenThrow(new JSchException("channel failed"));

    HopException ex =
        assertThrows(HopException.class, () -> SessionResult.executeCommand(session, "id"));

    assertInstanceOf(JSchException.class, ex.getCause());
  }

  private static InputStream stream(String text) {
    return new ByteArrayInputStream(text.getBytes(StandardCharsets.UTF_8));
  }
}
