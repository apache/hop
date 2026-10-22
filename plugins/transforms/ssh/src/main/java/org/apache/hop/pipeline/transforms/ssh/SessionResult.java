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
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.i18n.BaseMessages;

public class SessionResult {
  private static final Class<?> PKG = SshMeta.class;

  private static final int CHANNEL_POLL_MS = 50;

  /**
   * After JSch reports the channel closed, {@link InputStream#available()} can still briefly return
   * 0 while stdout/stderr packets are in flight (common under CI load). Keep polling a few times
   * before treating empty streams as complete so we do not drop command output.
   */
  private static final int POST_CLOSE_EMPTY_POLLS = 10;

  @Getter @Setter private String stdOut;
  @Getter private String stdErr;
  @Getter @Setter private boolean stdErrorType;

  public static SessionResult executeCommand(Session session, String command) throws HopException {
    return executeCommand(session, command, 0L, () -> false);
  }

  /**
   * Runs a command and waits for it to finish.
   *
   * @param commandTimeoutMs maximum time to wait for the command to complete, 0 or less waits until
   *     the command ends
   * @param stopped polled while waiting; when it returns true the channel is closed and a {@link
   *     HopException} is thrown
   */
  public static SessionResult executeCommand(
      Session session, String command, long commandTimeoutMs, BooleanSupplier stopped)
      throws HopException {
    return executeCommand(session, command, commandTimeoutMs, stopped, System::currentTimeMillis);
  }

  static SessionResult executeCommand(
      Session session,
      String command,
      long commandTimeoutMs,
      BooleanSupplier stopped,
      LongSupplier clock)
      throws HopException {
    ChannelExec channel = null;
    try {
      channel = (ChannelExec) session.openChannel("exec");
      channel.setCommand(command);
      channel.connect();
      SessionResult result = new SessionResult();
      result.readFromChannel(channel, commandTimeoutMs, stopped, clock);
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

  private void readFromChannel(
      ChannelExec channel, long commandTimeoutMs, BooleanSupplier stopped, LongSupplier clock)
      throws HopException {
    try {
      InputStream isOut = channel.getInputStream();
      InputStream isErr = channel.getErrStream();
      byte[] buffer = new byte[8192];
      StringBuilder stdout = new StringBuilder();
      StringBuilder stderr = new StringBuilder();

      long deadline = commandTimeoutMs > 0 ? clock.getAsLong() + commandTimeoutMs : Long.MAX_VALUE;
      int emptyPollsAfterClose = 0;
      while (true) {
        boolean readOut = appendAvailable(isOut, buffer, stdout);
        boolean readErr = appendAvailable(isErr, buffer, stderr);
        boolean readSomething = readOut || readErr;

        if (channel.isClosed()) {
          if (readSomething) {
            emptyPollsAfterClose = 0;
          } else if (isStreamDrained(isOut) && isStreamDrained(isErr)) {
            // Already captured output: channel close is enough. Empty output needs a short grace
            // window because available()==0 right after close is not a reliable EOF under load.
            if (stdout.length() > 0 || stderr.length() > 0) {
              break;
            }
            emptyPollsAfterClose++;
            if (emptyPollsAfterClose >= POST_CLOSE_EMPTY_POLLS) {
              break;
            }
          }
        } else if (stopped.getAsBoolean()) {
          throw new HopException(BaseMessages.getString(PKG, "SSH.Error.CommandStopped"));
        } else if (clock.getAsLong() > deadline) {
          throw new HopException(
              BaseMessages.getString(PKG, "SSH.Error.CommandTimedOut", commandTimeoutMs / 1000));
        } else {
          emptyPollsAfterClose = 0;
        }

        try {
          Thread.sleep(CHANNEL_POLL_MS);
        } catch (InterruptedException e) {
          Thread.currentThread().interrupt();
          throw new HopException(e);
        }
      }

      setStdOut(stdout.toString());
      setStdErr(stderr.toString());
    } catch (IOException e) {
      throw new HopException(e);
    }
  }

  /**
   * @return true if at least one byte was read
   */
  private static boolean appendAvailable(InputStream in, byte[] buffer, StringBuilder target)
      throws IOException {
    if (in == null) {
      return false;
    }
    boolean readSomething = false;
    while (in.available() > 0) {
      int read = in.read(buffer, 0, buffer.length);
      if (read < 0) {
        break;
      }
      target.append(new String(buffer, 0, read, StandardCharsets.UTF_8));
      readSomething = true;
    }
    return readSomething;
  }

  private boolean isStreamDrained(InputStream in) throws IOException {
    return in == null || in.available() == 0;
  }
}
