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
package org.apache.hop.vfs.ftp;

import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.metadata.api.IEnumHasCode;
import org.apache.hop.metadata.api.IEnumHasCodeAndDescription;

/** How a connection secures the control and data channels. */
public enum FtpSecurityMode implements IEnumHasCodeAndDescription {
  /** Plain FTP, no encryption at all. */
  FTP("FTP", 21),

  /**
   * FTPS in explicit mode: connect on the plain port and upgrade with {@code AUTH TLS} before
   * logging in. This is what most servers mean by "FTPS".
   */
  FTPS_EXPLICIT("FTPS_EXPLICIT", 21),

  /**
   * FTPS in implicit mode: the connection is TLS from the first byte, traditionally on port 990.
   */
  FTPS_IMPLICIT("FTPS_IMPLICIT", 990),
  ;

  private final String code;
  private final int defaultPort;

  FtpSecurityMode(String code, int defaultPort) {
    this.code = code;
    this.defaultPort = defaultPort;
  }

  @Override
  public String getCode() {
    return code;
  }

  /** The port a server in this mode traditionally listens on. */
  public int getDefaultPort() {
    return defaultPort;
  }

  /**
   * The descriptions are looked up per call rather than cached in the constant: the language of the
   * UI is picked after this class is loaded.
   */
  @Override
  public String getDescription() {
    return BaseMessages.getString(FtpSecurityMode.class, "FtpSecurityMode." + code + ".Label");
  }

  /** The VFS scheme files behind a connection in this mode are served with. */
  public String getScheme() {
    return this == FTP ? "ftp" : "ftps";
  }

  public boolean isSecure() {
    return this != FTP;
  }

  public static String[] getDescriptions() {
    return IEnumHasCodeAndDescription.getDescriptions(FtpSecurityMode.class);
  }

  public static FtpSecurityMode lookupDescription(String description) {
    return IEnumHasCodeAndDescription.lookupDescription(FtpSecurityMode.class, description, FTP);
  }

  public static FtpSecurityMode lookupCode(String code) {
    return IEnumHasCode.lookupCode(FtpSecurityMode.class, code, FTP);
  }
}
