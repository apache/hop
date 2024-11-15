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

import java.util.Date;
import java.util.List;
import lombok.Getter;
import lombok.Setter;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.xml.IXml;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.api.IHopMetadataProvider;
import org.apache.hop.www.SslConfiguration;
import org.w3c.dom.Node;

/**
 * Metadata describing how to reach a Hop server. The operations to interact with the remote server
 * live in {@link org.apache.hop.www.RemoteHopServer}.
 */
@Getter
@Setter
@HopMetadata(
    key = "server",
    name = "i18n::HopServer.name",
    description = "i18n::HopServer.description",
    image = "ui/images/server.svg",
    category = HopMetadataCategory.SERVERS,
    documentationUrl = "/metadata-types/hop-server.html",
    hopMetadataPropertyType = HopMetadataPropertyType.SERVER_DEFINITION,
    supportsGlobalReplace = true)
public class HopServerMeta extends HopMetadataBase implements Cloneable, IXml, IHopMetadata {
  public static final String STRING_HOP_SERVER = "Hop Server";
  public static final String XML_TAG = "hop-server";
  public static final String SSL_MODE_TAG = "sslMode";
  private static final String CONST_SPACE = "        ";

  @Setter @Getter @HopMetadataProperty private String hostname;

  @HopMetadataProperty private String port;

  @HopMetadataProperty private String webAppName;

  @HopMetadataProperty private String username;

  @HopMetadataProperty(password = true)
  private String password;

  /** enable authentication */
  @HopMetadataProperty private boolean enableAuth;

  @HopMetadataProperty private String proxyHostname;

  @HopMetadataProperty private String proxyPort;

  @HopMetadataProperty private String nonProxyHosts;

  private final Date changedDate;

  @HopMetadataProperty private boolean sslMode;

  @HopMetadataProperty private SslConfiguration sslConfig;

  public HopServerMeta() {
    this.changedDate = new Date();
  }

  public HopServerMeta(
      String name, String hostname, String port, String username, String password) {
    this(name, hostname, port, username, password, null, null, null, false);
  }

  public HopServerMeta(
      String name,
      String hostname,
      String port,
      String username,
      String password,
      String proxyHostname,
      String proxyPort,
      String nonProxyHosts) {
    this(name, hostname, port, username, password, proxyHostname, proxyPort, nonProxyHosts, false);
  }

  public HopServerMeta(
      String name,
      String hostname,
      String port,
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
    this.username = username;
    this.password = password;
    // default true
    this.enableAuth = true;

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
    this.webAppName = XmlHandler.getTagValue(node, "webAppName");
    this.username = XmlHandler.getTagValue(node, "username");
    this.password =
        Encr.decryptPasswordOptionallyEncrypted(XmlHandler.getTagValue(node, "password"));

    // if the enable_auth tag is empty or enable_auth=true, then it's true; otherwise false.
    String authValue = XmlHandler.getTagValue(node, "enable_auth");
    this.enableAuth = Utils.isEmpty(authValue) || "true".equalsIgnoreCase(authValue);

    this.proxyHostname = XmlHandler.getTagValue(node, "proxy_hostname");
    this.proxyPort = XmlHandler.getTagValue(node, "proxy_port");
    this.nonProxyHosts = XmlHandler.getTagValue(node, "non_proxy_hosts");

    setSslMode("Y".equalsIgnoreCase(XmlHandler.getTagValue(node, SSL_MODE_TAG)));
    Node sslConfig = XmlHandler.getSubNode(node, SslConfiguration.XML_TAG);
    if (sslConfig != null) {
      setSslMode(true);
      this.sslConfig = new SslConfiguration(sslConfig);
    }
  }

  @Override
  public String getXml(IVariables variables) {
    StringBuilder xml = new StringBuilder();

    xml.append("      ").append(XmlHandler.openTag(XML_TAG)).append(Const.CR);

    xml.append(CONST_SPACE).append(XmlHandler.addTagValue("name", name));
    xml.append(CONST_SPACE).append(XmlHandler.addTagValue("hostname", hostname));
    xml.append(CONST_SPACE).append(XmlHandler.addTagValue("port", port));
    xml.append(CONST_SPACE).append(XmlHandler.addTagValue("webAppName", webAppName));
    xml.append(CONST_SPACE).append(XmlHandler.addTagValue("username", username));
    xml.append(
        XmlHandler.addTagValue(
            "password", Encr.encryptPasswordIfNotUsingVariables(password), false));

    // if the enable_auth tag is empty or enable_auth=Y, then it's true; otherwise false.
    xml.append(CONST_SPACE).append(XmlHandler.addTagValue("enable_auth", enableAuth));

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

  public HopServerMeta(HopServerMeta server) {
    this();
    replaceMeta(server);
  }

  public void replaceMeta(HopServerMeta hopServer) {
    this.name = hopServer.name;
    this.hostname = hopServer.hostname;
    this.port = hopServer.port;
    this.webAppName = hopServer.webAppName;
    this.username = hopServer.username;
    this.password = hopServer.password;
    this.proxyHostname = hopServer.proxyHostname;
    this.proxyPort = hopServer.proxyPort;
    this.nonProxyHosts = hopServer.nonProxyHosts;
    this.sslMode = hopServer.sslMode;
    this.enableAuth = hopServer.enableAuth;
  }

  @Override
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
}
