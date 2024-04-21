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
 *
 */
package org.apache.hop.databases.cassandra.datastax;

import com.datastax.oss.driver.api.core.CqlSession;
import com.datastax.oss.driver.api.core.CqlSessionBuilder;
import com.datastax.oss.driver.api.core.config.DriverConfigLoader;
import com.datastax.oss.driver.api.core.config.ProgrammaticDriverConfigLoaderBuilder;
import com.datastax.oss.driver.api.core.config.TypedDriverOption;
import com.datastax.oss.driver.api.core.cql.ResultSet;
import com.datastax.oss.driver.api.core.metadata.schema.KeyspaceMetadata;
import com.datastax.oss.driver.api.core.session.Session;
import com.datastax.oss.driver.api.core.type.codec.MappingCodec;
import com.datastax.oss.driver.api.core.type.codec.TypeCodecs;
import com.datastax.oss.driver.api.core.type.codec.registry.MutableCodecRegistry;
import com.datastax.oss.driver.api.core.type.reflect.GenericType;
import com.datastax.oss.driver.api.querybuilder.SchemaBuilder;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspace;
import com.datastax.oss.driver.api.querybuilder.schema.CreateKeyspaceStart;
import com.datastax.oss.driver.internal.core.type.codec.registry.DefaultCodecRegistry;
import edu.umd.cs.findbugs.annotations.Nullable;
import java.net.InetSocketAddress;
import java.sql.Timestamp;
import java.time.Duration;
import java.time.Instant;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.databases.cassandra.spi.Keyspace;
import org.apache.hop.databases.cassandra.util.CassandraUtils;

/**
 * connection using standard datastax driver<br>
 * not thread-safe
 */
public class DriverConnection implements AutoCloseable {

  private String hosts;
  private int port = 9042;
  private String localDataCenter;
  private String username;
  private String password;
  private Map<String, String> opts = new HashMap<>();
  private boolean useCompression;

  private CqlSession session;
  private final Map<String, CqlSession> sessions = new HashMap<>();

  private final boolean expandCollection = true;

  public DriverConnection() {}

  public DriverConnection(String hosts, int port, String localDataCenter) {
    this.hosts = hosts;
    this.port = port;
    this.localDataCenter = localDataCenter;
  }

  public void setHosts(String hosts) {
    this.hosts = hosts;
  }

  public void setDefaultPort(int port) {
    this.port = port;
  }

  public void setAdditionalOptions(Map<String, String> opts) {
    this.opts = opts;
    if (opts.containsKey(CassandraUtils.ConnectionOptions.COMPRESSION)) {
      setUseCompression(true);
    }
  }

  public Map<String, String> getAdditionalOptions() {
    return opts;
  }

  public CqlSession open() {
    session = getSessionBuilder().build();
    return session;
  }

  @Override
  public void close() throws Exception {
    if (session != null) {
      session.close();
    }
    sessions.forEach((name, session) -> session.close());
    sessions.clear();
  }

  public Session getUnderlyingSession() {
    return session;
  }

  public void setUseCompression(boolean useCompression) {
    this.useCompression = useCompression;
  }

  public CqlSessionBuilder getSessionBuilder() {

    CqlSessionBuilder builder = CqlSession.builder().withApplicationName("Apache Hop");
    for (InetSocketAddress inetSocketAddress : getAddresses()) {
      builder = builder.addContactPoint(inetSocketAddress);
    }

    if (StringUtils.isNotEmpty(localDataCenter)) {
      builder = builder.withLocalDatacenter(localDataCenter);
    }

    ProgrammaticDriverConfigLoaderBuilder configLoaderBuilder =
        DriverConfigLoader.programmaticBuilder();

    // Authentication
    //
    if (!StringUtils.isEmpty(username)) {
      builder = builder.withAuthCredentials(username, password);
    }

    // Timeout
    //
    if (opts.containsKey(CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT)) {
      int timeoutMs =
          Integer.parseUnsignedInt(
              opts.get(CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT).trim());

      configLoaderBuilder =
          configLoaderBuilder.withDuration(
              TypedDriverOption.CONNECTION_CONNECT_TIMEOUT.getRawOption(),
              Duration.ofMillis(timeoutMs));
    }
    // Compression
    //
    if (useCompression) {
      configLoaderBuilder =
          configLoaderBuilder.withString(
              TypedDriverOption.PROTOCOL_COMPRESSION.getRawOption(), "lz4");
    }

    DefaultCodecRegistry codecRegistry = new DefaultCodecRegistry("Apache Hop");
    registerCodecs(codecRegistry);
    builder = builder.withCodecRegistry(codecRegistry);

    // Use the configuration loader as well.
    //
    builder = builder.withConfigLoader(configLoaderBuilder.build());

    return builder;
  }

  public CqlSession getSession(String keyspace) {
    return sessions.computeIfAbsent(
        keyspace, ks -> getSessionBuilder().withKeyspace(keyspace).build());
  }

  public Keyspace getKeyspace(String keyspaceName) throws Exception {
    Optional<KeyspaceMetadata> optionalKeyspace =
        getSession(keyspaceName).getMetadata().getKeyspace(keyspaceName);
    if (optionalKeyspace.isEmpty()) {
      throw new Exception("Unable to find keyspace '" + keyspaceName + "'");
    }
    return new DriverKeyspace(this, optionalKeyspace.get());
  }

  public String[] getKeyspaceNames() throws Exception {
    try (CqlSession session = getSessionBuilder().build()) {
      Collection<KeyspaceMetadata> keyspaceList = session.getMetadata().getKeyspaces().values();
      String[] names = new String[keyspaceList.size()];
      int i = 0;
      for (KeyspaceMetadata keyspace : keyspaceList) {
        names[i++] = keyspace.getName().asCql(false);
      }
      return names;
    }
  }

  public void createKeyspace(
      String keyspaceName, boolean ifNotExists, Map<String, Object> createOptions)
      throws Exception {
    CreateKeyspaceStart keyspaceStart = SchemaBuilder.createKeyspace(keyspaceName);
    if (ifNotExists) {
      keyspaceStart = keyspaceStart.ifNotExists();
    }
    CreateKeyspace createKeyspace = keyspaceStart.withReplicationOptions(createOptions);

    // Execute this
    try (CqlSession session = getSessionBuilder().build()) {
      session.execute(createKeyspace.build());
    }
  }

  public ResultSet executeCql(String query, Map<String, Object> values) throws Exception {
    try (CqlSession session = getSessionBuilder().build()) {
      return session.execute(query, values);
    }
  }

  public boolean isExpandCollection() {
    return expandCollection;
  }

  public InetSocketAddress[] getAddresses() {
    if (!hosts.contains(",") && !hosts.contains(":")) {
      if (StringUtils.isEmpty(hosts)) {
        return new InetSocketAddress[] {};
      } else {
        return new InetSocketAddress[] {new InetSocketAddress(hosts, port)};
      }
    } else {
      String[] hostsStrings = StringUtils.split(this.hosts, ",");
      InetSocketAddress[] hosts = new InetSocketAddress[hostsStrings.length];
      for (int i = 0; i < hosts.length; i++) {
        String[] hostPair = StringUtils.split(hostsStrings[i], ":");
        String hostName = hostPair[0].trim();
        int port = this.port;
        if (hostPair.length > 1) {
          try {
            port = Integer.parseInt(hostPair[1].trim());
          } catch (NumberFormatException nfe) {
            // ignored, default
          }
        }
        hosts[i] = new InetSocketAddress(hostName, port);
      }
      return hosts;
    }
  }

  private void registerCodecs(MutableCodecRegistry registry) {
    registry.register(
        new MappingCodec<>(TypeCodecs.INT, GenericType.LONG.unwrap()) {
          @Nullable
          @Override
          protected Long innerToOuter(@Nullable Integer value) {
            return value == null ? null : value.longValue();
          }

          @Nullable
          @Override
          protected Integer outerToInner(@Nullable Long value) {
            return value == null ? null : value.intValue();
          }
        });
    registry.register(
        new MappingCodec<>(TypeCodecs.FLOAT, GenericType.DOUBLE.unwrap()) {
          @Override
          protected Float outerToInner(Double value) {
            return value == null ? null : value.floatValue();
          }

          @Override
          protected Double innerToOuter(Float value) {
            return value == null ? null : value.doubleValue();
          }
        });
    registry.register(
        new MappingCodec<>(TypeCodecs.TIMESTAMP, GenericType.of(Date.class)) {
          @Nullable
          @Override
          protected Date innerToOuter(@Nullable Instant instant) {
            if (instant == null) {
              return null;
            }
            return new Date(instant.toEpochMilli());
          }

          @Nullable
          @Override
          protected Instant outerToInner(@Nullable Date date) {
            if (date == null) {
              return null;
            }
            return Instant.ofEpochMilli(date.getTime());
          }
        });
    registry.register(
        new MappingCodec<>(TypeCodecs.TIMESTAMP, GenericType.of(Timestamp.class)) {
          @Nullable
          @Override
          protected Timestamp innerToOuter(@Nullable Instant value) {
            if (value == null) {
              return null;
            }
            Timestamp timestamp = new Timestamp(value.toEpochMilli());
            timestamp.setNanos(value.getNano());
            return timestamp;
          }

          @Nullable
          @Override
          protected Instant outerToInner(@Nullable Timestamp timestamp) {
            if (timestamp == null) {
              return null;
            }
            return Instant.ofEpochMilli(timestamp.getTime()).plusNanos(timestamp.getNanos());
          }
        });
  }

  /**
   * Gets hosts
   *
   * @return value of hosts
   */
  public String getHosts() {
    return hosts;
  }

  /**
   * Gets port
   *
   * @return value of port
   */
  public int getPort() {
    return port;
  }

  /**
   * Sets port
   *
   * @param port value of port
   */
  public void setPort(int port) {
    this.port = port;
  }

  /**
   * Gets localDataCenter
   *
   * @return value of localDataCenter
   */
  public String getLocalDataCenter() {
    return localDataCenter;
  }

  /**
   * Sets localDataCenter
   *
   * @param localDataCenter value of localDataCenter
   */
  public void setLocalDataCenter(String localDataCenter) {
    this.localDataCenter = localDataCenter;
  }

  /**
   * Gets username
   *
   * @return value of username
   */
  public String getUsername() {
    return username;
  }

  /**
   * Sets username
   *
   * @param username value of username
   */
  public void setUsername(String username) {
    this.username = username;
  }

  /**
   * Gets password
   *
   * @return value of password
   */
  public String getPassword() {
    return password;
  }

  /**
   * Sets password
   *
   * @param password value of password
   */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * Gets opts
   *
   * @return value of opts
   */
  public Map<String, String> getOpts() {
    return opts;
  }

  /**
   * Sets opts
   *
   * @param opts value of opts
   */
  public void setOpts(Map<String, String> opts) {
    this.opts = opts;
  }

  /**
   * Gets useCompression
   *
   * @return value of useCompression
   */
  public boolean isUseCompression() {
    return useCompression;
  }

  /**
   * Gets session
   *
   * @return value of session
   */
  public CqlSession getSession() {
    return session;
  }

  /**
   * Sets session
   *
   * @param session value of session
   */
  public void setSession(CqlSession session) {
    this.session = session;
  }

  /**
   * Gets sessions
   *
   * @return value of sessions
   */
  public Map<String, CqlSession> getSessions() {
    return sessions;
  }
}
