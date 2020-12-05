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
package org.apache.hop.databases.cassandra.driver.datastax;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.CodecRegistry;
import com.datastax.driver.core.KeyspaceMetadata;
import com.datastax.driver.core.ProtocolOptions;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.SocketOptions;
import com.datastax.driver.core.TypeCodec;
import com.datastax.driver.extras.codecs.MappingCodec;
import java.net.InetSocketAddress;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.util.Utils;
import org.apache.hop.databases.cassandra.spi.Connection;
import org.apache.hop.databases.cassandra.spi.Keyspace;
import org.apache.hop.databases.cassandra.util.CassandraUtils;

/**
 * connection using standard datastax driver<br>
 * not thread-safe
 */
public class DriverConnection implements Connection, AutoCloseable {

  private String host;
  private int port = 9042;
  private String username, password;
  private Map<String, String> opts = new HashMap<>();
  private Cluster cluster;
  private boolean useCompression;

  private Session session;
  private Map<String, Session> sessions = new HashMap<>();

  private boolean expandCollection = true;

  public DriverConnection() {}

  public DriverConnection(String host, int port) {
    this.host = host;
    this.port = port;
  }

  @Override
  public void setHosts(String hosts) {
    this.host = hosts;
  }

  @Override
  public void setDefaultPort(int port) {
    this.port = port;
  }

  @Override
  public void setUsername(String username) {
    this.username = username;
  }

  @Override
  public void setPassword(String password) {
    this.password = password;
  }

  @Override
  public void setAdditionalOptions(Map<String, String> opts) {
    this.opts = opts;
    if (opts.containsKey(CassandraUtils.ConnectionOptions.COMPRESSION)) {
      setUseCompression(true);
    }
  }

  @Override
  public Map<String, String> getAdditionalOptions() {
    return opts;
  }

  @Override
  public void openConnection() throws Exception {
    session = getCluster().connect();
  }

  @Override
  public void closeConnection() throws Exception {
    if (session != null) {
      session.close();
    }
    sessions.forEach((name, session) -> session.close());
    sessions.clear();
    if (cluster != null) {
      cluster.closeAsync();
      cluster = null;
    }
  }

  @Override
  public Session getUnderlyingConnection() {
    return session;
  }

  public void setUseCompression(boolean useCompression) {
    this.useCompression = useCompression;
  }

  public Cluster getCluster() {
    if (cluster == null) {
      Cluster.Builder builder = Cluster.builder().addContactPointsWithPorts(getAddresses());
      if (!Utils.isEmpty(username)) {
        builder = builder.withCredentials(username, password);
      }
      if (opts.containsKey(CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT)) {
        int timeoutMs =
            Integer.parseUnsignedInt(
                opts.get(CassandraUtils.ConnectionOptions.SOCKET_TIMEOUT).trim());
        builder.withSocketOptions(new SocketOptions().setConnectTimeoutMillis(timeoutMs));
      }
      builder.withCompression(
          useCompression ? ProtocolOptions.Compression.LZ4 : ProtocolOptions.Compression.NONE);
      cluster = builder.build();
      registerCodecs(cluster.getConfiguration().getCodecRegistry());
    }
    return cluster;
  }

  public Session getSession(String keyspace) {
    return sessions.computeIfAbsent(keyspace, ks -> getCluster().connect(ks));
  }

  @Override
  public Keyspace getKeyspace(String keyspacename) throws Exception {
    KeyspaceMetadata keyspace = getCluster().getMetadata().getKeyspace(keyspacename);
    return new DriverKeyspace(this, keyspace);
  }

  @Override
  public void close() throws Exception {
    closeConnection();
  }

  public boolean isExpandCollection() {
    return expandCollection;
  }

  protected InetSocketAddress[] getAddresses() {
    if (!host.contains(",") && !host.contains(":")) {
      return new InetSocketAddress[] {new InetSocketAddress(host, port)};
    } else {
      String[] hostss = StringUtils.split(this.host, ",");
      InetSocketAddress[] hosts = new InetSocketAddress[hostss.length];
      for (int i = 0; i < hosts.length; i++) {
        String[] hostPair = StringUtils.split(hostss[i], ":");
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

  private void registerCodecs(CodecRegistry registry) {
    // where kettle expects specific types that don't match default deserialization
    registry.register(
        new MappingCodec<Long, Integer>(TypeCodec.cint(), Long.class) {
          @Override
          protected Long deserialize(Integer value) {
            return value == null ? null : value.longValue();
          }

          @Override
          protected Integer serialize(Long value) {
            return value == null ? null : value.intValue();
          }
        });
    registry.register(
        new MappingCodec<Double, Float>(TypeCodec.cfloat(), Double.class) {
          @Override
          protected Double deserialize(Float value) {
            return value == null ? null : value.doubleValue();
          }

          @Override
          protected Float serialize(Double value) {
            return value == null ? null : value.floatValue();
          }
        });
  }
}
