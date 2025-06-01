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

package org.apache.hop.www;

import java.net.SocketException;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Map.Entry;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.xml.IXml;
import org.apache.hop.core.xml.XmlHandler;
import org.apache.hop.metadata.serializer.multi.MultiMetadataProvider;
import org.apache.hop.server.HopServerMeta;
import org.w3c.dom.Node;

public class HopServerConfig implements IXml {
  public static final String XML_TAG = "hop-server-config";

  public static final String XML_TAG_JETTY_OPTIONS = "jetty_options";
  public static final String XML_TAG_ACCEPTORS = "acceptors";
  public static final String XML_TAG_ACCEPT_QUEUE_SIZE = "acceptQueueSize";
  public static final String XML_TAG_LOW_RES_MAX_IDLE_TIME = "lowResourcesMaxIdleTime";
  public static final String XML_METADATA_FOLDER = "metadata_folder";

  private HopServerMeta hopServer;

  private boolean joining;

  private int maxLogLines;

  private int maxLogTimeoutMinutes;

  private int objectTimeoutMinutes;

  private String filename;

  private String passwordFile;

  private String metadataFolder;

  private IVariables variables;

  /**
   * This is provided by the server at runtime. It then usually points to nothing or a project or
   * environment metadata folder.
   */
  private MultiMetadataProvider metadataProvider;

  public HopServerConfig() {
    passwordFile = null; // force lookup by server in ~/.hop or local folder
    variables = Variables.getADefaultVariableSpace();
    // An empty list of metadata providers by default
    metadataProvider =
        new MultiMetadataProvider(Encr.getEncoder(), Collections.emptyList(), variables);
  }

  public HopServerConfig(HopServerMeta hopServer) {
    this();
    this.hopServer = hopServer;
  }

  @Override
  public String getXml(IVariables variables) {

    StringBuilder xml = new StringBuilder();

    xml.append(XmlHandler.openTag(XML_TAG));

    if (hopServer != null) {
      xml.append(hopServer.getXml(variables));
    }

    XmlHandler.addTagValue("joining", joining);
    XmlHandler.addTagValue("max_log_lines", maxLogLines);
    XmlHandler.addTagValue("max_log_timeout_minutes", maxLogTimeoutMinutes);
    XmlHandler.addTagValue("object_timeout_minutes", objectTimeoutMinutes);
    XmlHandler.addTagValue(XML_METADATA_FOLDER, metadataFolder);

    xml.append(XmlHandler.closeTag(XML_TAG));

    return xml.toString();
  }

  public HopServerConfig(ILogChannel log, Node node) {
    this();
    Node hopServerNode = XmlHandler.getSubNode(node, HopServerMeta.XML_TAG);
    if (hopServerNode != null) {
      hopServer = new HopServerMeta(hopServerNode);
      checkNetworkInterfaceSetting(log, hopServerNode, hopServer);
    }

    joining = XmlHandler.getTagBoolean(node, "joining", true);
    maxLogLines = Const.toInt(XmlHandler.getTagValue(node, "max_log_lines"), 0);
    maxLogTimeoutMinutes = Const.toInt(XmlHandler.getTagValue(node, "max_log_timeout_minutes"), 0);
    objectTimeoutMinutes = Const.toInt(XmlHandler.getTagValue(node, "object_timeout_minutes"), 0);
    metadataFolder = XmlHandler.getTagValue(node, XML_METADATA_FOLDER);

    // Set Jetty Options
    setUpJettyOptions(node);
  }

  /**
   * Set up jetty options to the system properties
   *
   * @param node The node to read settings from
   */
  protected void setUpJettyOptions(Node node) {
    Map<String, String> jettyOptions = parseJettyOptions(node);

    if (jettyOptions != null && !jettyOptions.isEmpty()) {
      for (Entry<String, String> jettyOption : jettyOptions.entrySet()) {
        System.setProperty(jettyOption.getKey(), jettyOption.getValue());
      }
    }
  }

  /**
   * Read and parse jetty options
   *
   * @param node that contains jetty options nodes
   * @return map of not empty jetty options
   */
  protected Map<String, String> parseJettyOptions(Node node) {

    Map<String, String> jettyOptions = null;

    Node jettyOptionsNode = XmlHandler.getSubNode(node, XML_TAG_JETTY_OPTIONS);

    if (jettyOptionsNode != null) {

      jettyOptions = new HashMap<>();
      if (XmlHandler.getTagValue(jettyOptionsNode, XML_TAG_ACCEPTORS) != null) {
        jettyOptions.put(
            Const.HOP_SERVER_JETTY_ACCEPTORS,
            XmlHandler.getTagValue(jettyOptionsNode, XML_TAG_ACCEPTORS));
      }
      if (XmlHandler.getTagValue(jettyOptionsNode, XML_TAG_ACCEPT_QUEUE_SIZE) != null) {
        jettyOptions.put(
            Const.HOP_SERVER_JETTY_ACCEPT_QUEUE_SIZE,
            XmlHandler.getTagValue(jettyOptionsNode, XML_TAG_ACCEPT_QUEUE_SIZE));
      }
      if (XmlHandler.getTagValue(jettyOptionsNode, XML_TAG_LOW_RES_MAX_IDLE_TIME) != null) {
        jettyOptions.put(
            Const.HOP_SERVER_JETTY_RES_MAX_IDLE_TIME,
            XmlHandler.getTagValue(jettyOptionsNode, XML_TAG_LOW_RES_MAX_IDLE_TIME));
      }
    }
    return jettyOptions;
  }

  private void checkNetworkInterfaceSetting(
      ILogChannel log, Node serverNode, HopServerMeta hopServer) {
    // See if we need to grab the network interface to use and then override the host name
    //
    String networkInterfaceName = XmlHandler.getTagValue(serverNode, "network_interface");
    if (!Utils.isEmpty(networkInterfaceName)) {
      // OK, so let's try to get the IP address for this network interface...
      //
      try {
        String newHostname = Const.getIPAddress(networkInterfaceName);
        if (newHostname != null) {
          hopServer.setHostname(newHostname);
          // Also change the name of the server...
          //
          hopServer.setName(hopServer.getName() + "-" + newHostname);
          log.logBasic(
              "Hostname for hop server ["
                  + hopServer.getName()
                  + "] is set to ["
                  + newHostname
                  + "], information derived from network "
                  + networkInterfaceName);
        }
      } catch (SocketException e) {
        log.logError(
            "Unable to get the IP address for network interface "
                + networkInterfaceName
                + " for hop server ["
                + hopServer.getName()
                + "]",
            e);
      }
    }
  }

  public HopServerConfig(String hostname, int port, int shutdownPort, boolean joining) {
    this();
    this.joining = joining;
    this.hopServer =
        new HopServerMeta(
            hostname + ":" + port, hostname, "" + port, "" + shutdownPort, null, null);
  }

  /**
   * @return the hop server.<br>
   *     The user name and password defined in here are used to contact this server by the masters.
   */
  public HopServerMeta getHopServer() {
    return hopServer;
  }

  /**
   * @param hopServer the hop server details to set.<br>
   *     The user name and password defined in here are used to contact this server by the masters.
   */
  public void setHopServer(HopServerMeta hopServer) {
    this.hopServer = hopServer;
  }

  /**
   * @return true if the webserver needs to join with the webserver threads (wait/block until
   *     finished)
   */
  public boolean isJoining() {
    return joining;
  }

  /**
   * @param joining Set to true if the webserver needs to join with the webserver threads
   *     (wait/block until finished)
   */
  public void setJoining(boolean joining) {
    this.joining = joining;
  }

  /**
   * @return the maxLogLines
   */
  public int getMaxLogLines() {
    return maxLogLines;
  }

  /**
   * @param maxLogLines the maxLogLines to set
   */
  public void setMaxLogLines(int maxLogLines) {
    this.maxLogLines = maxLogLines;
  }

  /**
   * @return the maxLogTimeoutMinutes
   */
  public int getMaxLogTimeoutMinutes() {
    return maxLogTimeoutMinutes;
  }

  /**
   * @param maxLogTimeoutMinutes the maxLogTimeoutMinutes to set
   */
  public void setMaxLogTimeoutMinutes(int maxLogTimeoutMinutes) {
    this.maxLogTimeoutMinutes = maxLogTimeoutMinutes;
  }

  /**
   * @return the objectTimeoutMinutes
   */
  public int getObjectTimeoutMinutes() {
    return objectTimeoutMinutes;
  }

  /**
   * @param objectTimeoutMinutes the objectTimeoutMinutes to set
   */
  public void setObjectTimeoutMinutes(int objectTimeoutMinutes) {
    this.objectTimeoutMinutes = objectTimeoutMinutes;
  }

  /**
   * @return the filename
   */
  public String getFilename() {
    return filename;
  }

  /**
   * @param filename the filename to set
   */
  public void setFilename(String filename) {
    this.filename = filename;
  }

  public String getPasswordFile() {
    return passwordFile;
  }

  public void setPasswordFile(String passwordFile) {
    this.passwordFile = passwordFile;
  }

  /**
   * Gets variables
   *
   * @return value of variables
   */
  public IVariables getVariables() {
    return variables;
  }

  /**
   * @param variables The variables to set
   */
  public void setVariables(IVariables variables) {
    this.variables = variables;
  }

  /**
   * Gets metadataFolder
   *
   * @return value of metadataFolder
   */
  public String getMetadataFolder() {
    return metadataFolder;
  }

  /**
   * @param metadataFolder The metadataFolder to set
   */
  public void setMetadataFolder(String metadataFolder) {
    this.metadataFolder = metadataFolder;
  }

  /**
   * Gets metadataProvider
   *
   * @return value of metadataProvider
   */
  public MultiMetadataProvider getMetadataProvider() {
    return metadataProvider;
  }

  /**
   * @param metadataProvider The metadataProvider to set
   */
  public void setMetadataProvider(MultiMetadataProvider metadataProvider) {
    this.metadataProvider = metadataProvider;
  }
}
