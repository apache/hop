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

package org.apache.hop.server;

import java.util.List;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.IXml;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.www.SslConfiguration;
import org.w3c.dom.Node;

@HopMetadata(
    key = "server",
    name = "i18n::HopServer.name",
    description = "i18n::HopServer.description",
    image = "ui/images/server.svg",
    documentationUrl = "/metadata-types/hop-server.html",
    hopMetadataPropertyType = HopMetadataPropertyType.SERVER_DEFINITION)
public class HopServerMeta extends HopMetadataBase implements Cloneable, IXml, IHopMetadata {
  private static final Class<?> PKG = HopServerMeta.class;

  public static final String XML_TAG = "hop-server";
  public static final String SSL_MODE_TAG = "sslMode";
  private static final String CONST_SPACE = "        ";

  @HopMetadataProperty private String hostname;

  @HopMetadataProperty private String port;

  @HopMetadataProperty private String shutdownPort;

  @HopMetadataProperty private String webAppName;

  @HopMetadataProperty private String username;

  @HopMetadataProperty(password = true)
  private String password;

  @HopMetadataProperty private String proxyHostname;

  @HopMetadataProperty private String proxyPort;

  @HopMetadataProperty private String nonProxyHosts;

  @HopMetadataProperty private String propertiesMasterName;

  @HopMetadataProperty private boolean overrideExistingProperties;

  @HopMetadataProperty private boolean sslMode;

  @HopMetadataProperty private SslConfiguration sslConfig;

  public HopServerMeta() {
    super();
  }

  public HopServerMeta(
      String name,
      String hostname,
      String port,
      String shutdownPort,
      String username,
      String password) {
    this(name, hostname, port, shutdownPort, username, password, null, null, null, false);
  }

  public HopServerMeta(
      String name,
      String hostname,
      String port,
      String shutdownPort,
      String username,
      String password,
      String proxyHostname,
      String proxyPort,
      String nonProxyHosts) {
    this(
        name,
        hostname,
        port,
        shutdownPort,
        username,
        password,
        proxyHostname,
        proxyPort,
        nonProxyHosts,
        false);
  }

  public HopServerMeta(
      String name,
      String hostname,
      String port,
      String shutdownPort,
      String username,
      String password,
      String proxyHostname,
      String proxyPort,
      String nonProxyHosts,
      boolean sslMode) {
    this();
    this.name = name;
    this.hostname = hostname;
    this.port = port;
    this.shutdownPort = shutdownPort;
    this.username = username;
    this.password = password;
    this.proxyHostname = proxyHostname;
    this.proxyPort = proxyPort;
    this.nonProxyHosts = nonProxyHosts;
    this.sslMode = sslMode;
  }

  public HopServerMeta(Node node) {
    this();
    this.name = XmlHandler.getTagValue(node, "name");
    this.hostname = XmlHandler.getTagValue(node, "hostname");
    this.port = XmlHandler.getTagValue(node, "port");
    this.shutdownPort = XmlHandler.getTagValue(node, "shutdownPort");
    this.webAppName = XmlHandler.getTagValue(node, "webAppName");
    this.username = XmlHandler.getTagValue(node, "username");
    this.password =
        Encr.decryptPasswordOptionallyEncrypted(XmlHandler.getTagValue(node, "password"));
    this.proxyHostname = XmlHandler.getTagValue(node, "proxy_hostname");
    this.proxyPort = XmlHandler.getTagValue(node, "proxy_port");
    this.nonProxyHosts = XmlHandler.getTagValue(node, "non_proxy_hosts");
    this.propertiesMasterName = XmlHandler.getTagValue(node, "get_properties_from_master");
    this.overrideExistingProperties =
        "Y".equalsIgnoreCase(XmlHandler.getTagValue(node, "override_existing_properties"));

    setSslMode("Y".equalsIgnoreCase(XmlHandler.getTagValue(node, SSL_MODE_TAG)));
    Node sslConfig = XmlHandler.getSubNode(node, SslConfiguration.XML_TAG);
    if (sslConfig != null) {
      setSslMode(true);
      this.sslConfig = new SslConfiguration(sslConfig);
    }
  }

  public HopServerMeta(HopServerMeta server) {
    this();
    this.name = server.name;
    this.hostname = server.hostname;
    this.port = server.port;
    this.shutdownPort = server.shutdownPort;
    this.webAppName = server.webAppName;
    this.username = server.username;
    this.password = server.password;
    this.proxyHostname = server.proxyHostname;
    this.proxyPort = server.proxyPort;
    this.nonProxyHosts = server.nonProxyHosts;
    this.sslMode = server.sslMode;
  }

  @Override
  public String getXml(IVariables variables) {
    StringBuilder xml = new StringBuilder();

    xml.append("      ").append(XmlHandler.openTag(XML_TAG)).append(Const.CR);

    xml.append(CONST_SPACE).append(XmlHandler.addTagValue("name", name));
    xml.append(CONST_SPACE).append(XmlHandler.addTagValue("hostname", hostname));
    xml.append(CONST_SPACE).append(XmlHandler.addTagValue("port", port));
    xml.append(CONST_SPACE).append(XmlHandler.addTagValue("shutdownPort", shutdownPort));
    xml.append(CONST_SPACE).append(XmlHandler.addTagValue("webAppName", webAppName));
    xml.append(CONST_SPACE).append(XmlHandler.addTagValue("username", username));
    xml.append(
        XmlHandler.addTagValue(
            "password", Encr.encryptPasswordIfNotUsingVariables(password), false));
    xml.append(CONST_SPACE).append(XmlHandler.addTagValue("proxy_hostname", proxyHostname));
    xml.append(CONST_SPACE).append(XmlHandler.addTagValue("proxy_port", proxyPort));
    xml.append(CONST_SPACE).append(XmlHandler.addTagValue("non_proxy_hosts", nonProxyHosts));
    xml.append(CONST_SPACE).append(XmlHandler.addTagValue(SSL_MODE_TAG, isSslMode(), false));
    if (sslConfig != null) {
      xml.append(sslConfig.getXml());
    }

    xml.append("      ").append(XmlHandler.closeTag(XML_TAG)).append(Const.CR);

    return xml.toString();
  }

  @Override
  public HopServerMeta clone() {
    return new HopServerMeta(this);
  }

  public String toString() {
    return name;
  }

  @Override
  public boolean equals(Object obj) {
    if (!(obj instanceof HopServerMeta server)) {
      return false;
    }
    return name.equalsIgnoreCase(server.getName());
  }

  @Override
  public int hashCode() {
    return name.toLowerCase().hashCode();
  }

  public String getHostname() {
    return hostname;
  }

  public void setHostname(String urlString) {
    this.hostname = urlString;
  }

  /**
   * @return the password
   */
  public String getPassword() {
    return password;
  }

  /**
   * @param password the password to set
   */
  public void setPassword(String password) {
    this.password = password;
  }

  /**
   * @return the username
   */
  public String getUsername() {
    return username;
  }

  /**
   * @param username the username to set
   */
  public void setUsername(String username) {
    this.username = username;
  }

  /**
   * @return the web application name
   */
  public String getWebAppName() {
    return webAppName;
  }

  /**
   * @param webAppName the web application name to set
   */
  public void setWebAppName(String webAppName) {
    this.webAppName = webAppName;
  }

  /**
   * @return the nonProxyHosts
   */
  public String getNonProxyHosts() {
    return nonProxyHosts;
  }

  /**
   * @param nonProxyHosts the nonProxyHosts to set
   */
  public void setNonProxyHosts(String nonProxyHosts) {
    this.nonProxyHosts = nonProxyHosts;
  }

  /**
   * @return the proxyHostname
   */
  public String getProxyHostname() {
    return proxyHostname;
  }

  /**
   * @param proxyHostname the proxyHostname to set
   */
  public void setProxyHostname(String proxyHostname) {
    this.proxyHostname = proxyHostname;
  }

  /**
   * @return the proxyPort
   */
  public String getProxyPort() {
    return proxyPort;
  }

  /**
   * @param proxyPort the proxyPort to set
   */
  public void setProxyPort(String proxyPort) {
    this.proxyPort = proxyPort;
  }

  /**
   * @return the Master name for read properties
   */
  public String getPropertiesMasterName() {
    return propertiesMasterName;
  }

  /**
   * @return flag for read properties from Master
   */
  public boolean isOverrideExistingProperties() {
    return overrideExistingProperties;
  }

  /**
   * @return the port
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port the port to set
   */
  public void setShutdownPort(String port) {
    this.shutdownPort = port;
  }

  /**
   * @return the shutdown port
   */
  public String getShutdownPort() {
    return shutdownPort;
  }

  /**
   * @param port the shutdown port to set
   */
  public void setPort(String port) {
    this.port = port;
  }

  public static HopServerMeta findHopServer(List<HopServerMeta> hopServers, String name) {
    for (HopServerMeta hopServer : hopServers) {
      if (hopServer.getName() != null && hopServer.getName().equalsIgnoreCase(name)) {
        return hopServer;
      }
    }
    return null;
  }

  public static String[] getHopServerNames(IHopMetadataProvider metadataProvider)
      throws HopException {
    List<String> names = metadataProvider.getSerializer(HopServerMeta.class).listObjectNames();
    return names.toArray(new String[0]);
  }

  public String getDescription() {
    // NOT USED
    return null;
  }

  public void setDescription(String description) {
    // NOT USED
  }

  /**
   * Verify the name of the hop server and if required, change it if it already exists in the list
   * of hop servers.
   *
   * @param hopServers the hop servers to check against.
   * @param oldname the old name of the hop server
   * @return the new hop server name
   */
  public String verifyAndModifyHopServerName(List<HopServerMeta> hopServers, String oldname) {
    String name = getName();
    if (name.equalsIgnoreCase(oldname)) {
      return name; // nothing to see here: move along!
    }

    int nr = 2;
    while (HopServerMeta.findHopServer(hopServers, getName()) != null) {
      setName(name + " " + nr);
      nr++;
    }
    return getName();
  }

  /**
   * @param sslMode
   */
  public void setSslMode(boolean sslMode) {
    this.sslMode = sslMode;
  }

  /**
   * @return the sslMode
   */
  public boolean isSslMode() {
    return sslMode;
  }

  /**
   * @return the sslConfig
   */
  public SslConfiguration getSslConfig() {
    return sslConfig;
  }

  /**
   * @param sslConfig The sslConfig to set
   */
  public void setSslConfig(SslConfiguration sslConfig) {
    this.sslConfig = sslConfig;
  }

  /**
   * @param overrideExistingProperties The overrideExistingProperties to set
   */
  public void setOverrideExistingProperties(boolean overrideExistingProperties) {
    this.overrideExistingProperties = overrideExistingProperties;
  }

  /**
   * @param propertiesMasterName The propertiesMasterName to set
   */
  public void setPropertiesMasterName(String propertiesMasterName) {
    this.propertiesMasterName = propertiesMasterName;
  }
}
