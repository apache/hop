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

package org.apache.hop.neo4j.shared;

import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.row.value.ValueMetaString;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.IHopMetadata;
import org.neo4j.driver.*;

import java.net.URI;
import java.net.URISyntaxException;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;

@HopMetadata(
    key = "neo4j-connection",
    name = "Neo4j Connection",
    description = "A shared connection to a Neo4j server",
    image = "neo4j_logo.svg",
    documentationUrl =
        "https://hop.apache.org/manual/latest/metadata-types/neo4j/neo4j-connection.html")
public class NeoConnection extends HopMetadataBase implements IHopMetadata {

  @HopMetadataProperty private String server;

  @HopMetadataProperty private String databaseName;

  @HopMetadataProperty private String boltPort;

  @HopMetadataProperty private String browserPort;

  @HopMetadataProperty private boolean routing;

  @HopMetadataProperty private String routingVariable;

  @HopMetadataProperty private String routingPolicy;

  @HopMetadataProperty private String username;

  @HopMetadataProperty(password = true)
  private String password;

  @HopMetadataProperty private boolean usingEncryption;

  @HopMetadataProperty private String usingEncryptionVariable;

  @HopMetadataProperty private boolean trustAllCertificates;

  @HopMetadataProperty private String trustAllCertificatesVariable;

  @HopMetadataProperty private List<String> manualUrls;

  @HopMetadataProperty private String connectionLivenessCheckTimeout;

  @HopMetadataProperty private String maxConnectionLifetime;

  @HopMetadataProperty private String maxConnectionPoolSize;

  @HopMetadataProperty private String connectionAcquisitionTimeout;

  @HopMetadataProperty private String connectionTimeout;

  @HopMetadataProperty private String maxTransactionRetryTime;

  @HopMetadataProperty private boolean version4;

  @HopMetadataProperty private String version4Variable;

  public NeoConnection() {
    boltPort = "7687";
    browserPort = "7474";
    manualUrls = new ArrayList<>();
    version4 = true;
  }

  public NeoConnection(IVariables parent) {
    this();
    usingEncryption = true;
    trustAllCertificates = false;
  }

  public NeoConnection(IVariables parent, NeoConnection source) {
    this(parent);
    this.name = source.name;
    this.server = source.server;
    this.boltPort = source.boltPort;
    this.browserPort = source.browserPort;
    this.routing = source.routing;
    this.routingVariable = source.routingVariable;
    this.routingPolicy = source.routingPolicy;
    this.username = source.username;
    this.password = source.password;
    this.usingEncryption = source.usingEncryption;
    this.usingEncryptionVariable = source.usingEncryptionVariable;
    this.trustAllCertificates = source.trustAllCertificates;
    this.trustAllCertificatesVariable = source.trustAllCertificatesVariable;
    this.connectionLivenessCheckTimeout = source.connectionLivenessCheckTimeout;
    this.maxConnectionLifetime = source.maxConnectionLifetime;
    this.maxConnectionPoolSize = source.maxConnectionPoolSize;
    this.connectionAcquisitionTimeout = source.connectionAcquisitionTimeout;
    this.connectionTimeout = source.connectionTimeout;
    this.maxTransactionRetryTime = source.maxTransactionRetryTime;
    this.version4 = source.version4;
    this.version4Variable = source.version4Variable;
  }

  @Override
  public String toString() {
    return name == null ? super.toString() : name;
  }

  @Override
  public int hashCode() {
    return name == null ? super.hashCode() : name.hashCode();
  }

  @Override
  public boolean equals(Object object) {

    if (object == this) {
      return true;
    }
    if (!(object instanceof NeoConnection)) {
      return false;
    }

    NeoConnection connection = (NeoConnection) object;

    return name != null && name.equalsIgnoreCase(connection.name);
  }

  /**
   * Get a Neo4j session to work with
   *
   * @param log The logchannel to log to
   * @param variables
   * @return The Neo4j session
   */
  public Session getSession(ILogChannel log, IVariables variables) {
    Driver driver = getDriver(log, variables);
    SessionConfig.Builder cfgBuilder = SessionConfig.builder();
    if (StringUtils.isNotEmpty(databaseName)) {
      String realDatabaseName = variables.resolve(databaseName);
      if (StringUtils.isNotEmpty(realDatabaseName)) {
        cfgBuilder.withDatabase(realDatabaseName);
      }
    }
    return driver.session(cfgBuilder.build());
  }

  /**
   * Test this connection to Neo4j
   *
   * @throws Exception In case anything goes wrong
   * @param variables
   */
  public void test(IVariables variables) throws Exception {

    Session session = null;
    try {
      Driver driver = getDriver(LogChannel.GENERAL, variables);
      SessionConfig.Builder builder = SessionConfig.builder();
      if (StringUtils.isNotEmpty(databaseName)) {
        builder = builder.withDatabase(variables.resolve(databaseName));
      }
      session = driver.session(builder.build());
      // Do something with the session otherwise it doesn't test the connection
      //
      Result result = session.run("RETURN 0");
      Record record = result.next();
      Value value = record.get(0);
      int zero = value.asInt();
      assert (zero == 0);
    } catch (Exception e) {
      throw new Exception("Unable to connect to database '" + name + "' : " + e.getMessage(), e);
    } finally {
      if (session != null) {
        session.close();
      }
    }
  }

  public List<URI> getURIs(IVariables variables) throws URISyntaxException {

    List<URI> uris = new ArrayList<>();

    if (manualUrls != null && !manualUrls.isEmpty()) {
      // A manual URL is specified
      //
      for (String manualUrl : manualUrls) {
        uris.add(new URI(manualUrl));
      }
    } else {
      // Construct the URIs from the entered values
      //
      List<String> serverStrings = new ArrayList<>();
      String serversString = variables.resolve(server);
      if (isUsingRouting(variables)) {
        for (String serverString : serversString.split(",")) {
          serverStrings.add(serverString);
        }
      } else {
        serverStrings.add(serversString);
      }

      for (String serverString : serverStrings) {
        // Trim excess spaces from server name
        //
        String url = getUrl(Const.trim(serverString), variables);
        uris.add(new URI(url));
      }
    }

    return uris;
  }

  public String getUrl(String hostname, IVariables variables) {

    /*
     * Construct the following URL:
     *
     * bolt://hostname:port
     * bolt+routing://core-server:port/?policy=MyPolicy
     */
    String url = "";
    if (isUsingRouting(variables)) {
      url += "neo4j";
    } else {
      url += "bolt";
    }
    url += "://";

    // Hostname
    //
    url += hostname;

    // Port
    //
    if (StringUtils.isNotEmpty(boltPort) && hostname != null && !hostname.contains(":")) {
      url += ":" + variables.resolve(boltPort);
    }

    String routingPolicyString = variables.resolve(routingPolicy);
    if (isUsingRouting(variables) && StringUtils.isNotEmpty(routingPolicyString)) {
      try {
        url += "?policy=" + URLEncoder.encode(routingPolicyString, "UTF-8");
      } catch (Exception e) {
        LogChannel.GENERAL.logError(
            "Error encoding routing policy context '" + routingPolicyString + "' in connection URL",
            e);
        url += "?policy=" + routingPolicyString;
      }
    }

    return url;
  }

  /**
   * Get a list of all URLs, not just the first in case of routing.
   *
   * @return
   * @param variables
   */
  public String getUrl(IVariables variables) {
    StringBuffer urls = new StringBuffer();
    try {
      for (URI uri : getURIs(variables)) {
        if (urls.length() > 0) {
          urls.append(",");
        }
        urls.append(uri.toString());
      }
    } catch (Exception e) {
      urls.append("ERROR building URLs: " + e.getMessage());
    }
    return urls.toString();
  }

  public boolean encryptionVariableSet(IVariables variables) {
    if (!Utils.isEmpty(usingEncryptionVariable)) {
      String value = variables.resolve(usingEncryptionVariable);
      if (!Utils.isEmpty(value)) {
        return ValueMetaString.convertStringToBoolean(value);
      }
    }
    return false;
  }

  public boolean trustAllCertificatesVariableSet(IVariables variables) {
    if (!Utils.isEmpty(trustAllCertificatesVariable)) {
      String value = variables.resolve(trustAllCertificatesVariable);
      if (!Utils.isEmpty(value)) {
        return ValueMetaString.convertStringToBoolean(value);
      }
    }
    return false;
  }

  public boolean version4VariableSet(IVariables variables) {
    if (!Utils.isEmpty(version4Variable)) {
      String value = variables.resolve(version4Variable);
      if (!Utils.isEmpty(value)) {
        return ValueMetaString.convertStringToBoolean(value);
      }
    }
    return false;
  }

  public Driver getDriver(ILogChannel log, IVariables variables) {

    try {
      List<URI> uris = getURIs(variables);

      String realUsername = variables.resolve(username);
      String realPassword = Encr.decryptPasswordOptionallyEncrypted(variables.resolve(password));
      Config.ConfigBuilder configBuilder;
      if (encryptionVariableSet(variables) || usingEncryption) {
        configBuilder = Config.builder().withEncryption();
        if (trustAllCertificatesVariableSet(variables) || trustAllCertificates) {
          configBuilder =
              configBuilder.withTrustStrategy(Config.TrustStrategy.trustAllCertificates());
        }
      } else {
        configBuilder = Config.builder().withoutEncryption();
      }
      if (StringUtils.isNotEmpty(connectionLivenessCheckTimeout)) {
        long seconds = Const.toLong(variables.resolve(connectionLivenessCheckTimeout), -1L);
        if (seconds > 0) {
          configBuilder =
              configBuilder.withConnectionLivenessCheckTimeout(seconds, TimeUnit.MILLISECONDS);
        }
      }
      if (StringUtils.isNotEmpty(maxConnectionLifetime)) {
        long seconds = Const.toLong(variables.resolve(maxConnectionLifetime), -1L);
        if (seconds > 0) {
          configBuilder = configBuilder.withMaxConnectionLifetime(seconds, TimeUnit.MILLISECONDS);
        }
      }
      if (StringUtils.isNotEmpty(maxConnectionPoolSize)) {
        int size = Const.toInt(variables.resolve(maxConnectionPoolSize), -1);
        if (size > 0) {
          configBuilder = configBuilder.withMaxConnectionPoolSize(size);
        }
      }
      if (StringUtils.isNotEmpty(connectionAcquisitionTimeout)) {
        long seconds = Const.toLong(variables.resolve(connectionAcquisitionTimeout), -1L);
        if (seconds > 0) {
          configBuilder =
              configBuilder.withConnectionAcquisitionTimeout(seconds, TimeUnit.MILLISECONDS);
        }
      }
      if (StringUtils.isNotEmpty(connectionTimeout)) {
        long seconds = Const.toLong(variables.resolve(connectionTimeout), -1L);
        if (seconds > 0) {
          configBuilder = configBuilder.withConnectionTimeout(seconds, TimeUnit.MILLISECONDS);
        }
      }
      if (StringUtils.isNotEmpty(maxTransactionRetryTime)) {
        long seconds = Const.toLong(variables.resolve(maxTransactionRetryTime), -1L);
        if (seconds >= 0) {
          configBuilder = configBuilder.withMaxTransactionRetryTime(seconds, TimeUnit.MILLISECONDS);
        }
      }

      // Disable info messages: only warnings and above...
      //
      configBuilder = configBuilder.withLogging(Logging.javaUtilLogging(Level.WARNING));

      Config config = configBuilder.build();

      if (isUsingRouting(variables)) {
        return GraphDatabase.routingDriver(
            uris, AuthTokens.basic(realUsername, realPassword), config);
      } else {
        return GraphDatabase.driver(
            uris.get(0), AuthTokens.basic(realUsername, realPassword), config);
      }
    } catch (URISyntaxException e) {
      throw new RuntimeException(
          "URI syntax problem, check your settings, hostnames especially.  For routing use comma separated server values.",
          e);
    }
  }

  public boolean isUsingRouting(IVariables variables) {
    if (!Utils.isEmpty(routingVariable)) {
      String value = variables.resolve(routingVariable);
      if (!Utils.isEmpty(value)) {
        return ValueMetaString.convertStringToBoolean(value);
      }
    }
    return routing;
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  public String getName() {
    return name;
  }

  /** @param name The name to set */
  public void setName(String name) {
    this.name = name;
  }

  /**
   * Gets server
   *
   * @return value of server
   */
  public String getServer() {
    return server;
  }

  /** @param server The server to set */
  public void setServer(String server) {
    this.server = server;
  }

  /**
   * Gets databaseName
   *
   * @return value of databaseName
   */
  public String getDatabaseName() {
    return databaseName;
  }

  /** @param databaseName The databaseName to set */
  public void setDatabaseName(String databaseName) {
    this.databaseName = databaseName;
  }

  /**
   * Gets boltPort
   *
   * @return value of boltPort
   */
  public String getBoltPort() {
    return boltPort;
  }

  /** @param boltPort The boltPort to set */
  public void setBoltPort(String boltPort) {
    this.boltPort = boltPort;
  }

  /**
   * Gets browserPort
   *
   * @return value of browserPort
   */
  public String getBrowserPort() {
    return browserPort;
  }

  /** @param browserPort The browserPort to set */
  public void setBrowserPort(String browserPort) {
    this.browserPort = browserPort;
  }

  /**
   * Gets routing
   *
   * @return value of routing
   */
  public boolean isRouting() {
    return routing;
  }

  /** @param routing The routing to set */
  public void setRouting(boolean routing) {
    this.routing = routing;
  }

  /**
   * Gets routingVariable
   *
   * @return value of routingVariable
   */
  public String getRoutingVariable() {
    return routingVariable;
  }

  /** @param routingVariable The routingVariable to set */
  public void setRoutingVariable(String routingVariable) {
    this.routingVariable = routingVariable;
  }

  /**
   * Gets routingPolicy
   *
   * @return value of routingPolicy
   */
  public String getRoutingPolicy() {
    return routingPolicy;
  }

  /** @param routingPolicy The routingPolicy to set */
  public void setRoutingPolicy(String routingPolicy) {
    this.routingPolicy = routingPolicy;
  }

  /**
   * Gets username
   *
   * @return value of username
   */
  public String getUsername() {
    return username;
  }

  /** @param username The username to set */
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

  /** @param password The password to set */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * Gets usingEncryption
   *
   * @return value of usingEncryption
   */
  public boolean isUsingEncryption() {
    return usingEncryption;
  }

  /** @param usingEncryption The usingEncryption to set */
  public void setUsingEncryption(boolean usingEncryption) {
    this.usingEncryption = usingEncryption;
  }

  /**
   * Gets usingTrustAllCertificates
   *
   * @return value of usingTrustAllCertificates
   */
  public boolean isTrustAllCertificates() {
    return trustAllCertificates;
  }

  /** @param trustAllCertificates The trustAllCertificates to set */
  public void setTrustAllCertificates(boolean trustAllCertificates) {
    this.trustAllCertificates = trustAllCertificates;
  }

  /**
   * Gets manualUrls
   *
   * @return value of manualUrls
   */
  public List<String> getManualUrls() {
    return manualUrls;
  }

  /** @param manualUrls The manualUrls to set */
  public void setManualUrls(List<String> manualUrls) {
    this.manualUrls = manualUrls;
  }

  /**
   * Gets connectionLivenessCheckTimeout
   *
   * @return value of connectionLivenessCheckTimeout
   */
  public String getConnectionLivenessCheckTimeout() {
    return connectionLivenessCheckTimeout;
  }

  /** @param connectionLivenessCheckTimeout The connectionLivenessCheckTimeout to set */
  public void setConnectionLivenessCheckTimeout(String connectionLivenessCheckTimeout) {
    this.connectionLivenessCheckTimeout = connectionLivenessCheckTimeout;
  }

  /**
   * Gets maxConnectionLifetime
   *
   * @return value of maxConnectionLifetime
   */
  public String getMaxConnectionLifetime() {
    return maxConnectionLifetime;
  }

  /** @param maxConnectionLifetime The maxConnectionLifetime to set */
  public void setMaxConnectionLifetime(String maxConnectionLifetime) {
    this.maxConnectionLifetime = maxConnectionLifetime;
  }

  /**
   * Gets maxConnectionPoolSize
   *
   * @return value of maxConnectionPoolSize
   */
  public String getMaxConnectionPoolSize() {
    return maxConnectionPoolSize;
  }

  /** @param maxConnectionPoolSize The maxConnectionPoolSize to set */
  public void setMaxConnectionPoolSize(String maxConnectionPoolSize) {
    this.maxConnectionPoolSize = maxConnectionPoolSize;
  }

  /**
   * Gets connectionAcquisitionTimeout
   *
   * @return value of connectionAcquisitionTimeout
   */
  public String getConnectionAcquisitionTimeout() {
    return connectionAcquisitionTimeout;
  }

  /** @param connectionAcquisitionTimeout The connectionAcquisitionTimeout to set */
  public void setConnectionAcquisitionTimeout(String connectionAcquisitionTimeout) {
    this.connectionAcquisitionTimeout = connectionAcquisitionTimeout;
  }

  /**
   * Gets connectionTimeout
   *
   * @return value of connectionTimeout
   */
  public String getConnectionTimeout() {
    return connectionTimeout;
  }

  /** @param connectionTimeout The connectionTimeout to set */
  public void setConnectionTimeout(String connectionTimeout) {
    this.connectionTimeout = connectionTimeout;
  }

  /**
   * Gets maxTransactionRetryTime
   *
   * @return value of maxTransactionRetryTime
   */
  public String getMaxTransactionRetryTime() {
    return maxTransactionRetryTime;
  }

  /** @param maxTransactionRetryTime The maxTransactionRetryTime to set */
  public void setMaxTransactionRetryTime(String maxTransactionRetryTime) {
    this.maxTransactionRetryTime = maxTransactionRetryTime;
  }

  /**
   * Gets version4
   *
   * @return value of version4
   */
  public boolean isVersion4() {
    return version4;
  }

  /** @param version4 The version4 to set */
  public void setVersion4(boolean version4) {
    this.version4 = version4;
  }

  /**
   * Gets usingEncryptionVariable
   *
   * @return value of usingEncryptionVariable
   */
  public String getUsingEncryptionVariable() {
    return usingEncryptionVariable;
  }

  /** @param usingEncryptionVariable The usingEncryptionVariable to set */
  public void setUsingEncryptionVariable(String usingEncryptionVariable) {
    this.usingEncryptionVariable = usingEncryptionVariable;
  }

  /**
   * Gets trustAllCertificatesVariable
   *
   * @return value of trustAllCertificatesVariable
   */
  public String getTrustAllCertificatesVariable() {
    return trustAllCertificatesVariable;
  }

  /** @param trustAllCertificatesVariable The trustAllCertificatesVariable to set */
  public void setTrustAllCertificatesVariable(String trustAllCertificatesVariable) {
    this.trustAllCertificatesVariable = trustAllCertificatesVariable;
  }

  /**
   * Gets version4Variable
   *
   * @return value of version4Variable
   */
  public String getVersion4Variable() {
    return version4Variable;
  }

  /** @param version4Variable The version4Variable to set */
  public void setVersion4Variable(String version4Variable) {
    this.version4Variable = version4Variable;
  }
}
