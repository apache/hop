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

package org.apache.hop.splunk;

import com.splunk.Service;
import com.splunk.ServiceArgs;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
    key = "splunk",
    name = "i18n::SplunkConnection.name",
    description = "i18n::SplunkConnection.description",
    image = "splunk.svg",
    documentationUrl = "/metadata-types/splunk-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.SPLUNK_CONNECTION)
public class SplunkConnection extends HopMetadataBase implements Cloneable, IHopMetadata {

  @HopMetadataProperty private String hostname;

  @HopMetadataProperty private String port;

  @HopMetadataProperty private String username;

  @HopMetadataProperty(password = true)
  private String password;

  public SplunkConnection() {
    super();
  }

  public SplunkConnection(SplunkConnection source) {
    this.name = source.name;
    this.hostname = source.hostname;
    this.port = source.port;
    this.username = source.username;
    this.password = source.password;
  }

  public SplunkConnection(
      String name, String hostname, String port, String username, String password) {
    this.name = name;
    this.hostname = hostname;
    this.port = port;
    this.username = username;
    this.password = password;
  }

  @Override
  public SplunkConnection clone() {
    return new SplunkConnection(this);
  }

  public void test(IVariables variables) throws HopException {
    try {
      Service.connect(getServiceArgs(variables));
    } catch (Exception e) {
      throw new HopException(
          "Error connecting to Splunk connection '"
              + name
              + "' on host '"
              + getRealHostname(variables)
              + "' and port '"
              + getRealPort(variables)
              + "' with user '"
              + getRealUsername(variables)
              + "'",
          e);
    }
  }

  public ServiceArgs getServiceArgs(IVariables variables) {
    ServiceArgs args = new ServiceArgs();
    args.setUsername(getRealUsername(variables));
    args.setPassword(Encr.decryptPasswordOptionallyEncrypted(getRealPassword(variables)));
    args.setHost(getRealHostname(variables));
    args.setPort(Const.toInt(getRealPort(variables), 8089));
    return args;
  }

  public String getRealHostname(IVariables variables) {
    return variables.resolve(hostname);
  }

  public String getRealPort(IVariables variables) {
    return variables.resolve(port);
  }

  public String getRealUsername(IVariables variables) {
    return variables.resolve(username);
  }

  public String getRealPassword(IVariables variables) {
    return Encr.decryptPasswordOptionallyEncrypted(variables.resolve(password));
  }

  /**
   * Gets hostname
   *
   * @return value of hostname
   */
  public String getHostname() {
    return hostname;
  }

  /**
   * @param hostname The hostname to set
   */
  public void setHostname(String hostname) {
    this.hostname = hostname;
  }

  /**
   * Gets port
   *
   * @return value of port
   */
  public String getPort() {
    return port;
  }

  /**
   * @param port The port to set
   */
  public void setPort(String port) {
    this.port = port;
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
   * @param username The username to set
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
   * @param password The password to set
   */
  public void setPassword(String password) {
    this.password = password;
  }
}
