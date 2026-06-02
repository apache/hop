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

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.JSchException;
import com.jcraft.jsch.Session;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopException;

public class SessionResult {

  private static final int CHANNEL_POLL_MS = 50;
  private static final int CHANNEL_MAX_WAIT_MS = 120_000;

  @Getter @Setter private String stdOut;
  @Getter private String stdErr;
  @Getter @Setter private boolean stdErrorType;

  public static SessionResult executeCommand(Session session, String command) throws HopException {
    ChannelExec channel = null;
    try {
      channel = (ChannelExec) session.openChannel("exec");
      channel.setCommand(command);
      channel.connect();
      SessionResult result = new SessionResult();
      result.readFromChannel(channel);
      return result;
    } catch (JSchException e) {
      throw new HopException(e);
    } finally {
      if (channel != null) {
        channel.disconnect();
      }
    }
  }

  private void setStdErr(String value) {
    this.stdErr = value;
    if (stdErr != null && !stdErr.isEmpty()) {
      setStdErrorType(true);
    }
  }

  public String getStd() {
    return (getStdOut() == null ? "" : getStdOut()) + (getStdErr() == null ? "" : getStdErr());
  }

  private void readFromChannel(ChannelExec channel) throws HopException {
    try {
      InputStream isOut = channel.getInputStream();
      InputStream isErr = channel.getErrStream();
      byte[] buffer = new byte[8192];
      StringBuilder stdout = new StringBuilder();
      StringBuilder stderr = new StringBuilder();

      long deadline = System.currentTimeMillis() + CHANNEL_MAX_WAIT_MS;
      while (true) {
        appendAvailable(isOut, buffer, stdout);
        appendAvailable(isErr, buffer, stderr);
        if (channel.isClosed()) {
          if (isStreamDrained(isOut) && isStreamDrained(isErr)) {
            break;
          }
        } else if (System.currentTimeMillis() > deadline) {
          throw new HopException("Timed out waiting for SSH command to complete");
        } else {
          try {
            Thread.sleep(CHANNEL_POLL_MS);
          } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new HopException(e);
          }
        }
      }

      setStdOut(stdout.toString());
      setStdErr(stderr.toString());
    } catch (IOException e) {
      throw new HopException(e);
    }
  }

  private static void appendAvailable(InputStream in, byte[] buffer, StringBuilder target)
      throws IOException {
    if (in == null) {
      return;
    }
    while (in.available() > 0) {
      int read = in.read(buffer, 0, buffer.length);
      if (read < 0) {
        break;
      }
      target.append(new String(buffer, 0, read, StandardCharsets.UTF_8));
    }
  }

  private boolean isStreamDrained(InputStream in) throws IOException {
    return in == null || in.available() == 0;
  }
}
