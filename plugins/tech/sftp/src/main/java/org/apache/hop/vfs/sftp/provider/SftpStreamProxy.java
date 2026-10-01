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

import com.jcraft.jsch.ChannelExec;
import com.jcraft.jsch.Proxy;
import com.jcraft.jsch.Session;
import com.jcraft.jsch.SocketFactory;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.Socket;
import org.apache.commons.vfs2.FileSystemOptions;

/**
 * Stream based proxy for JSch.
 *
 * <p>Use a command on the proxy that will forward the SSH stream to the target host and port.
 *
 * @since 2.1
 */
public class SftpStreamProxy implements Proxy {
  /** Command format using bash built-in TCP stream. */
  public static final String BASH_TCP_COMMAND =
      "/bin/bash -c 'exec 3<>/dev/tcp/%s/%d; cat <&3 & cat >&3; kill $!";

  /** Command format using netcat command. */
  public static final String NETCAT_COMMAND = "nc -q 0 %s %d";

  private ChannelExec channel;

  /**
   * Command pattern to execute on the proxy host.
   *
   * <p>When run, the command output should be forwarded to the target host and port, and its input
   * should be forwarded from the target host and port.
   *
   * <p>The command will be created for each host/port pair by using {@linkplain
   * String#format(String, Object...)} with two objects: the target host name ({@linkplain String})
   * and the target port ({@linkplain Integer}).
   *
   * <p>Here are two examples (that can be easily used by using the static members of this class):
   *
   * <ul>
   *   <li>{@code nc -q 0 %s %d} to use the netcat command ({@linkplain #NETCAT_COMMAND})
   *   <li>{@code /bin/bash -c 'exec 3<>/dev/tcp/%s/%d; cat <&3 & cat >&3; kill $!} will use bash
   *       built-in TCP stream, which can be useful when there is no netcat available. ({@linkplain
   *       #BASH_TCP_COMMAND})
   * </ul>
   */
  private final String commandFormat;

  /** Hostname used to connect to the proxy host. */
  private final String proxyHost;

  /** The options for connection. */
  private final FileSystemOptions proxyOptions;

  /** The password to be used for connection. */
  private final String proxyPassword;

  /** Port used to connect to the proxy host. */
  private final int proxyPort;

  /** User name used to connect to the proxy host. */
  private final String proxyUser;

  private Session session;

  /**
   * Creates a stream proxy.
   *
   * @param commandFormat A format string that will be used to create the command to execute on the
   *     proxy host using {@linkplain String#format(String, Object...)}. Two parameters are given to
   *     the format command, the target host name (String) and port (Integer).
   * @param proxyUser The proxy user
   * @param proxyPassword The proxy password
   * @param proxyHost The proxy host
   * @param proxyPort The port to connect to on the proxy
   * @param proxyOptions Options used when connecting to the proxy
   */
  public SftpStreamProxy(
      final String commandFormat,
      final String proxyUser,
      final String proxyHost,
      final int proxyPort,
      final String proxyPassword,
      final FileSystemOptions proxyOptions) {
    this.proxyHost = proxyHost;
    this.proxyPort = proxyPort;
    this.proxyUser = proxyUser;
    this.proxyPassword = proxyPassword;
    this.commandFormat = commandFormat;
    this.proxyOptions = proxyOptions;
  }

  @Override
  public void close() {
    if (channel != null) {
      channel.disconnect();
    }
    if (session != null) {
      session.disconnect();
    }
  }

  @Override
  public void connect(
      final SocketFactory socketFactory,
      final String targetHost,
      final int targetPort,
      final int timeout)
      throws Exception {
    session =
        SftpClientFactory.createConnection(
            proxyHost,
            proxyPort,
            proxyUser.toCharArray(),
            proxyPassword.toCharArray(),
            proxyOptions);
    channel = (ChannelExec) session.openChannel("exec");
    channel.setCommand(String.format(commandFormat, targetHost, targetPort));
    channel.connect(timeout);
  }

  @Override
  public InputStream getInputStream() {
    try {
      return channel.getInputStream();
    } catch (final IOException e) {
      throw new IllegalStateException("IOException getting the SSH proxy input stream", e);
    }
  }

  @Override
  public OutputStream getOutputStream() {
    try {
      return channel.getOutputStream();
    } catch (final IOException e) {
      throw new IllegalStateException("IOException getting the SSH proxy output stream", e);
    }
  }

  @Override
  public Socket getSocket() {
    return null;
  }
}
