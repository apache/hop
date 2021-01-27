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

package org.apache.hop.workflow.actions.util;

public interface IFtpConnection {
  /**
   * Gets serverName
   *
   * @return value of serverName
   */
  String getServerName();

  /**
   * Gets userName
   *
   * @return value of userName
   */
  String getUserName();

  /**
   * Gets password
   *
   * @return value of password
   */
  String getPassword();


  /**
   * Gets binaryMode
   *
   * @return value of binaryMode
   */
  boolean isBinaryMode();

  /**
   * Gets timeout
   *
   * @return value of timeout
   */
  int getTimeout();

  /**
   * Gets controlEncoding
   *
   * @return value of controlEncoding
   */
  String getControlEncoding();

  /**
   * Gets the server port
   *
   * @return value of the server port
   */
  String getServerPort();

  /**
   * Gets proxyHost
   *
   * @return value of proxyHost
   */
  String getProxyHost();

  /**
   * Gets proxyPort
   *
   * @return value of proxyPort
   */
  String getProxyPort();

  /**
   * Gets proxyUsername
   *
   * @return value of proxyUsername
   */
  String getProxyUsername();

  /**
   * Gets proxyPassword
   *
   * @return value of proxyPassword
   */
  String getProxyPassword();

  /**
   * Gets socksProxyHost
   *
   * @return value of socksProxyHost
   */
  String getSocksProxyHost();

  /**
   * Gets socksProxyPort
   *
   * @return value of socksProxyPort
   */
  String getSocksProxyPort();

  /**
   * Gets socksProxyUsername
   *
   * @return value of socksProxyUsername
   */
  String getSocksProxyUsername();

  /**
   * Gets socksProxyPassword
   *
   * @return value of socksProxyPassword
   */
  String getSocksProxyPassword();

  /**
   * Gets activeConnection
   *
   * @return value of activeConnection
   */
  public boolean isActiveConnection();

}
