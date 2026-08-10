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

import com.jcraft.jsch.ConfigRepository;
import com.jcraft.jsch.UserInfo;
import java.io.File;
import java.io.Serializable;
import java.time.Duration;
import java.util.Objects;
import java.util.stream.Stream;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemConfigBuilder;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;

/** The config builder for various SFTP configuration options. */
public final class SftpFileSystemConfigBuilder extends FileSystemConfigBuilder {

  /** Proxy type. */
  public static final class ProxyType implements Serializable, Comparable<ProxyType> {

    /** serialVersionUID format is YYYYMMDD for the date of the last binary change. */
    private static final long serialVersionUID = 20101208L;

    /** The proxy type. */
    private final String proxyType;

    private ProxyType(final String proxyType) {
      this.proxyType = proxyType;
    }

    @Override
    public int compareTo(final ProxyType pType) {
      return proxyType.compareTo(pType.proxyType);
    }

    @Override
    public boolean equals(final Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null || this.getClass() != obj.getClass()) {
        return false;
      }
      return Objects.equals(proxyType, ((ProxyType) obj).proxyType);
    }

    /**
     * @return a hash code value for this object.
     * @since 2.0
     */
    @Override
    public int hashCode() {
      return proxyType.hashCode();
    }
  }

  /** HTTP Proxy. */
  public static final ProxyType PROXY_HTTP = new ProxyType("http");

  /** SOCKS Proxy. */
  public static final ProxyType PROXY_SOCKS5 = new ProxyType("socks");

  /**
   * Connects to the SFTP server through a remote host reached by SSH.
   *
   * <p>On this proxy host, a command (e.g. {@linkplain SftpStreamProxy#NETCAT_COMMAND} or
   * {@linkplain SftpStreamProxy#NETCAT_COMMAND}) is run to forward input/output streams between the
   * target host and the VFS host.
   *
   * <p>When used, the proxy username ({@linkplain #setProxyUser}) and hostname ({@linkplain
   * #setProxyHost}) <strong>must</strong> be set. Optionally, the command ({@linkplain
   * #setProxyCommand}), password ({@linkplain #setProxyPassword}) and connection options
   * ({@linkplain #setProxyOptions}) can be set.
   */
  public static final ProxyType PROXY_STREAM = new ProxyType("stream");

  private static final Duration DEFAULT_CONNECT_TIMEOUT = Duration.ZERO;

  private static final Duration DEFAULT_SESSION_TIMEOUT = Duration.ZERO;

  private static final String PREFIX = SftpFileSystemConfigBuilder.class.getName();
  private static final SftpFileSystemConfigBuilder BUILDER = new SftpFileSystemConfigBuilder();
  private static final String COMPRESSION = PREFIX + "COMPRESSION";
  private static final String CONNECT_TIMEOUT = PREFIX + ".CONNECT_TIMEOUT";
  private static final String ENCODING = PREFIX + ".ENCODING";
  private static final String HOST_KEY_CHECK_ASK = "ask";
  private static final String HOST_KEY_CHECK_NO = "no";
  private static final String HOST_KEY_CHECK_YES = "yes";
  private static final String IDENTITIES = PREFIX + ".IDENTITIES";
  private static final String IDENTITY_REPOSITORY_FACTORY = PREFIX + "IDENTITY_REPOSITORY_FACTORY";
  private static final String CONFIG_REPOSITORY = PREFIX + "CONFIG_REPOSITORY";
  private static final String KEY_EXCHANGE_ALGORITHM = PREFIX + ".KEY_EXCHANGE_ALGORITHM";
  private static final String LOAD_OPENSSH_CONFIG = PREFIX + "LOAD_OPENSSH_CONFIG";
  private static final String KNOWN_HOSTS = PREFIX + ".KNOWN_HOSTS";
  private static final String PREFERRED_AUTHENTICATIONS = PREFIX + ".PREFERRED_AUTHENTICATIONS";
  private static final String PROXY_COMMAND = PREFIX + ".PROXY_COMMAND";
  private static final String PROXY_HOST = PREFIX + ".PROXY_HOST";
  private static final String PROXY_OPTIONS = PREFIX + ".PROXY_OPTIONS";
  private static final String PROXY_PASSWORD = PREFIX + ".PROXY_PASSWORD";
  private static final String PROXY_PORT = PREFIX + ".PROXY_PORT";
  private static final String DISABLE_DETECT_EXEC_CHANNEL = PREFIX + ".DISABLE_DETECT_EXEC_CHANNEL";

  private static final String PROXY_TYPE = PREFIX + ".PROXY_TYPE";
  private static final String PROXY_USER = PREFIX + ".PROXY_USER";
  private static final String SESSION_TIMEOUT = PREFIX + ".TIMEOUT";
  private static final String STRICT_HOST_KEY_CHECKING = PREFIX + ".STRICT_HOST_KEY_CHECKING";
  private static final String USER_DIR_IS_ROOT = PREFIX + ".USER_DIR_IS_ROOT";

  /**
   * Gets the singleton builder.
   *
   * @return the singleton builder.
   */
  public static SftpFileSystemConfigBuilder getInstance() {
    return BUILDER;
  }

  private SftpFileSystemConfigBuilder() {
    super("sftp.");
  }

  /**
   * Gets the names of the compression algorithms, comma-separated.
   *
   * @param options The FileSystem options.
   * @return The names of the compression algorithms, comma-separated.
   * @see #setCompression
   */
  public String getCompression(final FileSystemOptions options) {
    return this.getString(options, COMPRESSION);
  }

  @Override
  protected Class<? extends FileSystem> getConfigClass() {
    return SftpFileSystem.class;
  }

  /**
   * Gets the config repository.
   *
   * @param options The FileSystem options.
   * @return the ConfigRepository
   */
  public ConfigRepository getConfigRepository(final FileSystemOptions options) {
    return getParam(options, CONFIG_REPOSITORY);
  }

  /**
   * Gets the connect timeout duration.
   *
   * @param options The FileSystem options.
   * @return The connect timeout duration.
   * @see #setConnectTimeoutMillis
   * @since 2.8.0
   */
  public Duration getConnectTimeout(final FileSystemOptions options) {
    return this.getDuration(options, CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
  }

  /**
   * Gets the connect timeout duration.
   *
   * @param options The FileSystem options.
   * @return The connect timeout value in milliseconds.
   * @see #setConnectTimeoutMillis
   * @since 2.3
   * @deprecated Use {@link #getConnectTimeout(FileSystemOptions)}.
   */
  @Deprecated
  public Integer getConnectTimeoutMillis(final FileSystemOptions options) {
    return this.getDurationInteger(options, CONNECT_TIMEOUT, DEFAULT_CONNECT_TIMEOUT);
  }

  /**
   * Gets the file name encoding.
   *
   * @param options The FileSystem options.
   * @return the file name encoding
   */
  public String getFileNameEncoding(final FileSystemOptions options) {
    return this.getString(options, ENCODING);
  }

  /**
   * Gets the identity files (your private key files).
   *
   * <p>We use java.io.File because JSch cannot deal with VFS FileObjects.
   *
   * @param options The FileSystem options.
   * @return the array of identity Files.
   * @see #setIdentities
   * @deprecated As of 2.1 use {@link #getIdentityInfo(FileSystemOptions)}
   */
  @Deprecated
  public File[] getIdentities(final FileSystemOptions options) {
    final IdentityInfo[] info = getIdentityInfo(options);
    if (info != null) {
      return Stream.of(info).map(IdentityInfo::getPrivateKey).toArray(File[]::new);
    }
    return null;
  }

  /**
   * Gets the identity infos.
   *
   * @param options The FileSystem options.
   * @return the array of identity info.
   * @see #setIdentityInfo
   */
  public IdentityInfo[] getIdentityInfo(final FileSystemOptions options) {
    final IdentityProvider[] infos = getIdentityProvider(options);
    if (infos != null) {
      return Stream.of(infos)
          .filter(IdentityInfo.class::isInstance)
          .map(info -> (IdentityInfo) info)
          .toArray(IdentityInfo[]::new);
    }
    return null;
  }

  /**
   * Gets the identity providers.
   *
   * @param options The FileSystem options.
   * @return the array of identity providers.
   * @see #setIdentityProvider
   * @since 2.4
   */
  public IdentityProvider[] getIdentityProvider(final FileSystemOptions options) {
    return getParam(options, IDENTITIES);
  }

  /**
   * Gets the identity repository factory.
   *
   * @param options The FileSystem options.
   * @return the IdentityRepositoryFactory
   */
  public IdentityRepositoryFactory getIdentityRepositoryFactory(final FileSystemOptions options) {
    return getParam(options, IDENTITY_REPOSITORY_FACTORY);
  }

  /**
   * Gets the option value for specific key exchange algorithm.
   *
   * @param options The FileSystem options.
   * @return the option value for specific key exchange algorithm.
   * @see #setKeyExchangeAlgorithm(FileSystemOptions, String)
   * @since 2.4
   */
  public String getKeyExchangeAlgorithm(final FileSystemOptions options) {
    return this.getString(options, KEY_EXCHANGE_ALGORITHM);
  }

  /**
   * Gets the known hosts File.
   *
   * @param options The FileSystem options.
   * @return the known hosts File.
   * @see #setKnownHosts
   */
  public File getKnownHosts(final FileSystemOptions options) {
    return getParam(options, KNOWN_HOSTS);
  }

  /**
   * Gets authentication order.
   *
   * @param options The FileSystem options.
   * @return The authentication order.
   * @since 2.0
   */
  public String getPreferredAuthentications(final FileSystemOptions options) {
    return getString(options, PREFERRED_AUTHENTICATIONS);
  }

  /**
   * Gets the command that will be run on the proxy host when using a {@linkplain SftpStreamProxy}.
   * The command defaults to {@linkplain SftpStreamProxy#NETCAT_COMMAND}.
   *
   * @param options The FileSystem options.
   * @return proxyOptions
   * @see SftpStreamProxy
   * @see #setProxyOptions
   * @since 2.1
   */
  public String getProxyCommand(final FileSystemOptions options) {
    return this.getString(options, PROXY_COMMAND, SftpStreamProxy.NETCAT_COMMAND);
  }

  /**
   * Gets the proxy to use for the SFTP connection.
   *
   * @param options The FileSystem options.
   * @return proxyHost
   * @see #getProxyPort
   * @see #setProxyHost
   */
  public String getProxyHost(final FileSystemOptions options) {
    return this.getString(options, PROXY_HOST);
  }

  /**
   * Gets the proxy options that are used to connect to the proxy host.
   *
   * @param options The FileSystem options.
   * @return proxyOptions
   * @see SftpStreamProxy
   * @see #setProxyOptions
   * @since 2.1
   */
  public FileSystemOptions getProxyOptions(final FileSystemOptions options) {
    return getParam(options, PROXY_OPTIONS);
  }

  /**
   * Gets the proxy password that are used to connect to the proxy host.
   *
   * @param options The FileSystem options.
   * @return proxyOptions
   * @see SftpStreamProxy
   * @see #setProxyPassword
   * @since 2.1
   */
  public String getProxyPassword(final FileSystemOptions options) {
    return this.getString(options, PROXY_PASSWORD);
  }

  /**
   * Gets the proxy-port to use for the SFTP the connection.
   *
   * @param options The FileSystem options.
   * @return proxyPort: the port number or 0 if it is not set
   * @see #setProxyPort
   * @see #getProxyHost
   */
  public int getProxyPort(final FileSystemOptions options) {
    return this.getInteger(options, PROXY_PORT, 0);
  }

  /**
   * Gets the proxy type to use for the SFTP connection.
   *
   * @param options The FileSystem options.
   * @return The ProxyType.
   */
  public ProxyType getProxyType(final FileSystemOptions options) {
    return getParam(options, PROXY_TYPE);
  }

  /**
   * Gets the user name for the proxy used for the SFTP connection.
   *
   * @param options The FileSystem options.
   * @return proxyUser
   * @see #setProxyUser
   * @since 2.1
   */
  public String getProxyUser(final FileSystemOptions options) {
    return this.getString(options, PROXY_USER);
  }

  /**
   * Gets the session timeout value in milliseconds.
   *
   * @param options The FileSystem options.
   * @return The session timeout value in milliseconds.
   * @see #setSessionTimeout
   * @since 2.3
   */
  public Duration getSessionTimeout(final FileSystemOptions options) {
    return this.getDuration(options, SESSION_TIMEOUT, DEFAULT_SESSION_TIMEOUT);
  }

  /**
   * Gets the session timeout value in milliseconds.
   *
   * @param options The FileSystem options.
   * @return The session timeout value in milliseconds.
   * @see #setSessionTimeoutMillis
   * @since 2.3
   * @deprecated Use {@link #getSessionTimeout(FileSystemOptions)}.
   */
  @Deprecated
  public Integer getSessionTimeoutMillis(final FileSystemOptions options) {
    return this.getDurationInteger(options, SESSION_TIMEOUT, DEFAULT_SESSION_TIMEOUT);
  }

  /**
   * Gets the option value The host key checking.
   *
   * @param options The FileSystem options.
   * @return the option value The host key checking.
   * @see #setStrictHostKeyChecking(FileSystemOptions, String)
   */
  public String getStrictHostKeyChecking(final FileSystemOptions options) {
    return this.getString(options, STRICT_HOST_KEY_CHECKING, HOST_KEY_CHECK_NO);
  }

  /**
   * Gets the timeout value in milliseconds.
   *
   * @param options The FileSystem options.
   * @return The timeout value in milliseconds.
   * @see #setTimeout
   * @deprecated Use {@link #getSessionTimeoutMillis(FileSystemOptions)}
   */
  @Deprecated
  public Integer getTimeout(final FileSystemOptions options) {
    return this.getInteger(options, SESSION_TIMEOUT);
  }

  /**
   * Gets {@link Boolean#TRUE} if VFS should treat the user directory as the root directory.
   * Defaults to {@code Boolean.TRUE} if the method {@link #setUserDirIsRoot(FileSystemOptions,
   * boolean)} has not been invoked.
   *
   * @param options The FileSystemOptions.
   * @return {@code Boolean.TRUE} if VFS treats the user directory as the root directory.
   * @see #setUserDirIsRoot
   */
  public Boolean getUserDirIsRoot(final FileSystemOptions options) {
    return this.getBoolean(options, USER_DIR_IS_ROOT, Boolean.TRUE);
  }

  /**
   * Gets the UserInfo.
   *
   * @param options The FileSystem options.
   * @return The UserInfo.
   * @see #setUserInfo
   */
  public UserInfo getUserInfo(final FileSystemOptions options) {
    return getParam(options, UserInfo.class.getName());
  }

  /**
   * Returns {@code true} if the detection of the exec channel should be disabled. Returns {@code
   * false} if the detection of the exec channel should be enabled. Defaults to {@code false} if the
   * method {@link #setDisableDetectExecChannel(FileSystemOptions, boolean)} has not been invoked.
   *
   * @param options The FileSystemOptions.
   * @return {@code true} if detection of exec channel should be disabled.
   * @see #setDisableDetectExecChannel(FileSystemOptions, boolean)
   * @since 2.7.0
   */
  public boolean isDisableDetectExecChannel(final FileSystemOptions options) {
    return this.getBoolean(options, DISABLE_DETECT_EXEC_CHANNEL, Boolean.FALSE);
  }

  /**
   * Returns {@link Boolean#TRUE} if VFS should load the OpenSSH config. Defaults to {@code
   * Boolean.FALSE} if the method {@link #setLoadOpenSSHConfig(FileSystemOptions, boolean)} has not
   * been invoked.
   *
   * @param options The FileSystemOptions.
   * @return {@code Boolean.TRUE} if VFS should load the OpenSSH config.
   * @see #setLoadOpenSSHConfig
   */
  public boolean isLoadOpenSSHConfig(final FileSystemOptions options) {
    return this.getBoolean(options, LOAD_OPENSSH_CONFIG, Boolean.FALSE);
  }

  /**
   * Configures the compression algorithms to use.
   *
   * <p>For example, use {@code "zlib,none"} to enable compression.
   *
   * <p>See the Jsch documentation (in particular the README file) for details.
   *
   * @param options The FileSystem options.
   * @param compression The names of the compression algorithms, comma-separated.
   */
  public void setCompression(final FileSystemOptions options, final String compression) {
    this.setParam(options, COMPRESSION, compression);
  }

  /**
   * Sets the config repository. e.g. {@code /home/user/.ssh/config}.
   *
   * <p>This is useful when you want to use OpenSSHConfig.
   *
   * @param options The FileSystem options.
   * @param configRepository An config repository.
   * @see <a href="http://www.jcraft.com/jsch/examples/OpenSSHConfig.java.html">OpenSSHConfig</a>
   */
  public void setConfigRepository(
      final FileSystemOptions options, final ConfigRepository configRepository) {
    this.setParam(options, CONFIG_REPOSITORY, configRepository);
  }

  /**
   * Sets the timeout value to create a Jsch connection.
   *
   * @param options The FileSystem options.
   * @param timeout The connect timeout in milliseconds.
   * @since 2.8.0
   */
  public void setConnectTimeout(final FileSystemOptions options, final Duration timeout) {
    this.setParam(options, CONNECT_TIMEOUT, timeout);
  }

  /**
   * Sets the timeout value to create a Jsch connection.
   *
   * @param options The FileSystem options.
   * @param timeout The connect timeout in milliseconds.
   * @since 2.3
   * @deprecated Use {@link #setConnectTimeout(FileSystemOptions, Duration)}.
   */
  @Deprecated
  public void setConnectTimeoutMillis(final FileSystemOptions options, final Integer timeout) {
    setConnectTimeout(options, Duration.ofMillis(timeout));
  }

  /**
   * Sets whether detection of exec channel is disabled. If this value is true the FileSystem will
   * not test if the server allows to exec commands and disable the use of the exec channel.
   *
   * @param options The FileSystem options.
   * @param disableDetectExecChannel true if the detection of exec channel should be disabled.
   * @since 2.7.0
   */
  public void setDisableDetectExecChannel(
      final FileSystemOptions options, final boolean disableDetectExecChannel) {
    this.setParam(options, DISABLE_DETECT_EXEC_CHANNEL, toBooleanObject(disableDetectExecChannel));
  }

  /**
   * Sets the file name encoding.
   *
   * @param options The FileSystem options.
   * @param fileNameEncoding The name of the encoding to use for file names.
   */
  public void setFileNameEncoding(final FileSystemOptions options, final String fileNameEncoding) {
    this.setParam(options, ENCODING, fileNameEncoding);
  }

  /**
   * Sets the identity files (your private key files).
   *
   * <p>We use {@link File} because JSch cannot deal with VFS FileObjects.
   *
   * @param options The FileSystem options.
   * @param identityFiles An array of identity Files.
   * @deprecated As of 2.1 use {@link #setIdentityInfo(FileSystemOptions, IdentityInfo...)}
   */
  @Deprecated
  public void setIdentities(final FileSystemOptions options, final File... identityFiles) {
    IdentityProvider[] info = null;
    if (identityFiles != null) {
      info =
          Stream.of(identityFiles)
              .filter(Objects::nonNull)
              .filter(Objects::nonNull)
              .map(IdentityInfo::new)
              .toArray(IdentityProvider[]::new);
    }
    this.setParam(options, IDENTITIES, info);
  }

  /**
   * Sets the identity info (your private key files).
   *
   * @param options The FileSystem options.
   * @param identities An array of identity info.
   * @since 2.1
   * @deprecated Use {@link #setIdentityProvider(FileSystemOptions,IdentityProvider...)}
   */
  @Deprecated
  public void setIdentityInfo(final FileSystemOptions options, final IdentityInfo... identities) {
    this.setParam(options, IDENTITIES, identities);
  }

  /**
   * Sets the identity info (your private key files).
   *
   * @param options The FileSystem options.
   * @param identities An array of identity info.
   * @since 2.4
   */
  public void setIdentityProvider(
      final FileSystemOptions options, final IdentityProvider... identities) {
    this.setParam(options, IDENTITIES, identities);
  }

  /**
   * Sets the identity repository.
   *
   * <p>This is useful when you want to use e.g. an SSH agent as provided.
   *
   * @param options The FileSystem options.
   * @param factory An identity repository.
   * @see <a href="http://www.jcraft.com/jsch-agent-proxy/">JSch agent proxy</a>
   */
  public void setIdentityRepositoryFactory(
      final FileSystemOptions options, final IdentityRepositoryFactory factory) {
    this.setParam(options, IDENTITY_REPOSITORY_FACTORY, factory);
  }

  /**
   * Configures Key exchange algorithm explicitly e.g. diffie-hellman-group14-sha1,
   * diffie-hellman-group-exchange-sha256, diffie-hellman-group-exchange-sha1,
   * diffie-hellman-group1-sha1.
   *
   * @param options The FileSystem options.
   * @param keyExchangeAlgorithm The key exchange algorithm picked.
   * @since 2.4
   */
  public void setKeyExchangeAlgorithm(
      final FileSystemOptions options, final String keyExchangeAlgorithm) {
    setParam(options, KEY_EXCHANGE_ALGORITHM, keyExchangeAlgorithm);
  }

  /**
   * Sets the known_hosts file. e.g. {@code /home/user/.ssh/known_hosts2}.
   *
   * <p>We use {@link File} because JSch cannot deal with VFS FileObjects.
   *
   * @param options The FileSystem options.
   * @param knownHosts The known hosts file.
   */
  public void setKnownHosts(final FileSystemOptions options, final File knownHosts) {
    this.setParam(options, KNOWN_HOSTS, knownHosts);
  }

  /**
   * Sets the whether to load OpenSSH config.
   *
   * @param options The FileSystem options.
   * @param loadOpenSSHConfig true if the OpenSSH config should be loaded.
   */
  public void setLoadOpenSSHConfig(
      final FileSystemOptions options, final boolean loadOpenSSHConfig) {
    this.setParam(options, LOAD_OPENSSH_CONFIG, toBooleanObject(loadOpenSSHConfig));
  }

  /**
   * Configures authentication order.
   *
   * @param options The FileSystem options.
   * @param preferredAuthentications The authentication order.
   * @since 2.0
   */
  public void setPreferredAuthentications(
      final FileSystemOptions options, final String preferredAuthentications) {
    this.setParam(options, PREFERRED_AUTHENTICATIONS, preferredAuthentications);
  }

  /**
   * Sets the proxy username to use for the SFTP connection.
   *
   * @param options The FileSystem options.
   * @param proxyCommand the port
   * @see #getProxyOptions
   * @since 2.1
   */
  public void setProxyCommand(final FileSystemOptions options, final String proxyCommand) {
    this.setParam(options, PROXY_COMMAND, proxyCommand);
  }

  /**
   * Sets the proxy to use for the SFTP connection.
   *
   * <p>You MUST also set the proxy port to use the proxy.
   *
   * @param options The FileSystem options.
   * @param proxyHost the host
   * @see #setProxyPort
   */
  public void setProxyHost(final FileSystemOptions options, final String proxyHost) {
    this.setParam(options, PROXY_HOST, proxyHost);
  }

  /**
   * Sets the proxy username to use for the SFTP connection.
   *
   * @param options The FileSystem options.
   * @param proxyOptions the options
   * @see #getProxyOptions
   * @since 2.1
   */
  public void setProxyOptions(
      final FileSystemOptions options, final FileSystemOptions proxyOptions) {
    this.setParam(options, PROXY_OPTIONS, proxyOptions);
  }

  /**
   * Sets the proxy password to use for the SFTP connection.
   *
   * @param options The FileSystem options.
   * @param proxyPassword the username used to connect to the proxy
   * @see #getProxyPassword
   * @since 2.1
   */
  public void setProxyPassword(final FileSystemOptions options, final String proxyPassword) {
    this.setParam(options, PROXY_PASSWORD, proxyPassword);
  }

  /**
   * Sets the proxy port to use for the SFTP connection.
   *
   * <p>You MUST also set the proxy host to use the proxy.
   *
   * @param options The FileSystem options.
   * @param proxyPort the port
   * @see #setProxyHost
   */
  public void setProxyPort(final FileSystemOptions options, final int proxyPort) {
    this.setParam(options, PROXY_PORT, Integer.valueOf(proxyPort));
  }

  /**
   * Sets the proxy type to use for the SFTP connection.
   *
   * <p>The possibles values are:
   *
   * <ul>
   *   <li>{@linkplain #PROXY_HTTP} connects using an HTTP proxy
   *   <li>{@linkplain #PROXY_SOCKS5} connects using an Socket5 proxy
   *   <li>{@linkplain #PROXY_STREAM} connects through a remote host stream command
   * </ul>
   *
   * @param options The FileSystem options.
   * @param proxyType the type of the proxy to use.
   */
  public void setProxyType(final FileSystemOptions options, final ProxyType proxyType) {
    this.setParam(options, PROXY_TYPE, proxyType);
  }

  /**
   * Sets the proxy username to use for the SFTP connection.
   *
   * @param options The FileSystem options.
   * @param proxyUser the username used to connect to the proxy
   * @see #getProxyUser
   * @since 2.1
   */
  public void setProxyUser(final FileSystemOptions options, final String proxyUser) {
    this.setParam(options, PROXY_USER, proxyUser);
  }

  /**
   * Sets the timeout value on Jsch session.
   *
   * @param options The FileSystem options.
   * @param timeout The session timeout in milliseconds.
   * @since 2.8.0
   */
  public void setSessionTimeout(final FileSystemOptions options, final Duration timeout) {
    this.setParam(options, SESSION_TIMEOUT, timeout);
  }

  /**
   * Sets the timeout value on Jsch session.
   *
   * @param options The FileSystem options.
   * @param timeout The session timeout in milliseconds.
   * @since 2.3
   * @deprecated Use {@link #setSessionTimeout(FileSystemOptions, Duration)}.
   */
  @Deprecated
  public void setSessionTimeoutMillis(final FileSystemOptions options, final Integer timeout) {
    setSessionTimeout(options, Duration.ofMillis(timeout));
  }

  /**
   * Configures the host key checking to use.
   *
   * <p>Valid arguments are: {@code "yes"}, {@code "no"} and {@code "ask"}.
   *
   * <p>See the jsch documentation for details.
   *
   * @param options The FileSystem options.
   * @param hostKeyChecking The host key checking to use.
   * @throws FileSystemException if an error occurs.
   */
  public void setStrictHostKeyChecking(
      final FileSystemOptions options, final String hostKeyChecking) throws FileSystemException {
    if (hostKeyChecking == null
        || !hostKeyChecking.equals(HOST_KEY_CHECK_ASK)
            && !hostKeyChecking.equals(HOST_KEY_CHECK_NO)
            && !hostKeyChecking.equals(HOST_KEY_CHECK_YES)) {
      throw new FileSystemException(
          "vfs.provider.sftp/StrictHostKeyChecking-arg.error", hostKeyChecking);
    }

    this.setParam(options, STRICT_HOST_KEY_CHECKING, hostKeyChecking);
  }

  /**
   * Sets the timeout value on Jsch session.
   *
   * @param options The FileSystem options.
   * @param timeout The timeout in milliseconds.
   * @deprecated Use {@link #setSessionTimeout(FileSystemOptions, Duration)}
   */
  @Deprecated
  public void setTimeout(final FileSystemOptions options, final Integer timeout) {
    this.setParam(options, SESSION_TIMEOUT, timeout);
  }

  /**
   * Sets the whether to use the user directory as root (do not change to file system root).
   *
   * @param options The FileSystem options.
   * @param userDirIsRoot true if the user directory is the root directory.
   */
  public void setUserDirIsRoot(final FileSystemOptions options, final boolean userDirIsRoot) {
    this.setParam(options, USER_DIR_IS_ROOT, toBooleanObject(userDirIsRoot));
  }

  /**
   * Sets the Jsch UserInfo class to use.
   *
   * @param options The FileSystem options.
   * @param info User information.
   */
  public void setUserInfo(final FileSystemOptions options, final UserInfo info) {
    this.setParam(options, UserInfo.class.getName(), info);
  }
}
