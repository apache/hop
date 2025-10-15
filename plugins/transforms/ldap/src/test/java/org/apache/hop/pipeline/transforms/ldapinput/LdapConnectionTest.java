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
package org.apache.hop.pipeline.transforms.ldapinput;

import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.mockito.Mockito.when;

import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.variables.IVariables;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.AdditionalAnswers;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;

class LdapConnectionTest {

  private ILogChannel logChannelInterface;

  private IVariables variables;

  private LdapInputMeta meta;

  @BeforeEach
  void setUp() {
    logChannelInterface = Mockito.mock(ILogChannel.class);
    variables = Mockito.mock(IVariables.class);
  }

  @Test
  void testFake() {
    assertTrue(true, "To Keep ExtendWith annotation");
  }

  // @Test
  void testLdapConnect() {
    meta = new LdapInputMeta();
    meta.setProtocol("LDAP");
    meta.setHost("localhost");
    meta.setPort("1389");

    when(variables.resolve(ArgumentMatchers.<String>any()))
        .thenAnswer(AdditionalAnswers.returnsFirstArg());

    LdapConnection connection;
    try {
      connection = new LdapConnection(logChannelInterface, variables, meta, null);
      connection.connect("cn=Directory Manager", "password");
    } catch (HopException e) {
      fail(e.getMessage());
    }
  }

  // @Test
  void testLdapConnectBadCredential() throws HopException {
    meta = new LdapInputMeta();
    meta.setProtocol("LDAP");
    meta.setHost("localhost");
    meta.setPort("1389");

    when(variables.resolve(ArgumentMatchers.<String>any()))
        .thenAnswer(AdditionalAnswers.returnsFirstArg());

    LdapConnection connection = new LdapConnection(logChannelInterface, variables, meta, null);

    HopException exception =
        assertThrows(
            HopException.class,
            () -> {
              connection.connect("cn=Directory Manager", "idontknow");
            });
    assertTrue(
        exception.getMessage().contains("Invalid Credentials"),
        "Exception should contain 'Invalid Credentials'");
  }

  // Failing test case - TODO Need to mock Utils.resolvePassword
  public void testLdapsTrustAllConnect() {

    meta = new LdapInputMeta();
    meta.setProtocol("LDAP SSL");
    meta.setHost("localhost");
    meta.setPort("1636");
    meta.setTrustStorePath("self-signed.truststore");
    meta.setTrustStorePassword("changeit");
    meta.setUseCertificate(true);
    meta.setTrustAllCertificates(true);

    when(variables.resolve(ArgumentMatchers.<String>any()))
        .thenAnswer(AdditionalAnswers.returnsFirstArg());

    LdapConnection connection;
    try {
      connection = new LdapConnection(logChannelInterface, variables, meta, null);
      connection.connect("cn=Directory Manager", "password");
    } catch (HopException e) {
      fail(e.getMessage());
    }
  }

  // Failing test case - I expect connection to be successful
  public void testLdapsTrustOnlyStoreConnect() {

    meta = new LdapInputMeta();
    meta.setProtocol("LDAP SSL");
    meta.setHost("localhost");
    meta.setPort("1636");
    meta.setTrustStorePath(
        getClass().getClassLoader().getResource("self-signed.truststore").getPath());
    meta.setTrustStorePassword("changeit");
    meta.setUseCertificate(true);
    meta.setTrustAllCertificates(false);

    when(variables.resolve(ArgumentMatchers.<String>any()))
        .thenAnswer(AdditionalAnswers.returnsFirstArg());

    LdapConnection connection;
    try {
      connection = new LdapConnection(logChannelInterface, variables, meta, null);
      connection.connect("cn=Directory Manager", "password");
    } catch (HopException e) {
      fail(e.getMessage());
    }
  }
}
