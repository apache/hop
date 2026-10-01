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

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

/**
 * The defaults on the connection contract are what the FTP actions get for the settings they never
 * had. They are the behaviour those actions had before the setting existed, so a change here
 * changes what a workflow written years ago does.
 */
class IFtpConnectionTest {

  @Test
  @DisplayName("An implementation which fills in nothing behaves like plain FTP always did")
  void theDefaultsAreTheOldBehaviour() {
    IFtpConnection connection = new MinimalConnection();

    assertSame(FtpSecurityMode.FTP, connection.getSecurityMode());
    assertSame(FtpDataChannelProtection.PRIVATE, connection.getDataChannelProtection());
    assertTrue(connection.isVerifyServerCertificate());
    assertTrue(connection.isRemoteVerification(), "the data connection was always checked");
    assertFalse(connection.isAutodetectUtf8(), "UTF-8 was never auto detected before");

    assertNull(connection.getSocketTimeout());
    assertNull(connection.getDataTimeout());
    assertNull(connection.getControlKeepAliveTimeout());
    assertNull(connection.getControlKeepAliveReplyTimeout());
    assertNull(connection.getActivePortRangeFrom());
    assertNull(connection.getActivePortRangeTo());

    assertNull(connection.getClientCertificateFile());
    assertNull(connection.getClientCertificatePassword());
    assertNull(connection.getClientCertificateAlias());
    assertNull(connection.getClientCertificateType());

    assertNull(connection.getEntryParser(), "the listing format was always auto detected");
    assertNull(connection.getServerLanguageCode());
    assertNull(connection.getServerTimeZone());
    assertNull(connection.getDefaultDateFormat());
    assertNull(connection.getRecentDateFormat());
    assertNull(connection.getShortMonthNames());
  }

  /** Only the settings the FTP actions have always carried. */
  private static class MinimalConnection implements IFtpConnection {
    @Override
    public String getFtpConnectionName() {
      return "minimal";
    }

    @Override
    public String getServerName() {
      return "ftp.example.com";
    }

    @Override
    public String getServerPort() {
      return "21";
    }

    @Override
    public String getUserName() {
      return "hop";
    }

    @Override
    public String getPassword() {
      return "secret";
    }

    @Override
    public boolean isBinaryMode() {
      return true;
    }

    @Override
    public boolean isActiveConnection() {
      return false;
    }

    @Override
    public String getControlEncoding() {
      return null;
    }

    @Override
    public String getConnectTimeout() {
      return null;
    }

    @Override
    public String getProxyHost() {
      return null;
    }

    @Override
    public String getProxyPort() {
      return null;
    }

    @Override
    public String getProxyUsername() {
      return null;
    }

    @Override
    public String getProxyPassword() {
      return null;
    }

    @Override
    public String getSocksProxyHost() {
      return null;
    }

    @Override
    public String getSocksProxyPort() {
      return null;
    }

    @Override
    public String getSocksProxyUsername() {
      return null;
    }

    @Override
    public String getSocksProxyPassword() {
      return null;
    }
  }
}
