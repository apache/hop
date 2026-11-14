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

package org.apache.hop.pipeline.transforms.salesforcebulk;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.mockito.Mockito.mock;

import com.sforce.soap.partner.LoginResult;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection;
import org.junit.jupiter.api.Test;

class SalesforceBulkSessionTest {

  @Test
  void stripsTheSoapPathFromTheServerUrl() {
    assertEquals(
        "https://na1.salesforce.com",
        SalesforceBulkSession.instanceRoot("https://na1.salesforce.com/services/Soap/u/64.0"));
    assertEquals(
        "http://salesforce_mock:8080",
        SalesforceBulkSession.instanceRoot("http://salesforce_mock:8080/"));
  }

  @Test
  void usesTheOauthTokenWithoutALogin() throws Exception {
    SalesforceConnection connection =
        new SalesforceConnection(
            mock(ILogChannel.class), "client", "secret", "token", "http://salesforce_mock:8080/");
    SalesforceBulkSession session = SalesforceBulkSession.from(connection);
    assertEquals("token", session.getAccessToken());
    assertEquals("http://salesforce_mock:8080", session.getInstanceUrl());
    assertEquals("64.0", session.getApiVersion());
  }

  @Test
  void usesTheSoapSessionIdAsABearerToken() throws Exception {
    SalesforceConnection connection =
        new SalesforceConnection(
            mock(ILogChannel.class),
            "https://login.salesforce.com/services/Soap/u/64.0",
            "user",
            "secret");
    LoginResult login = new LoginResult();
    login.setSessionId("session-id");
    login.setServerUrl("https://na1.salesforce.com/services/Soap/u/64.0");
    connection.setLoginResult(login);
    SalesforceBulkSession session = SalesforceBulkSession.from(connection);
    assertEquals("session-id", session.getAccessToken());
    assertEquals("https://na1.salesforce.com", session.getInstanceUrl());
  }
}
