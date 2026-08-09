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

package org.apache.hop.metadata.rest;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.encryption.HopTwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPluginType;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.plugins.PluginRegistry;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.metadata.rest.client.RestAuthType;
import org.apache.hop.metadata.rest.client.RestAuthenticator;
import org.apache.hop.metadata.rest.client.RestClientSettings;
import org.apache.hop.metadata.serializer.json.JsonMetadataParser;
import org.apache.hop.metadata.serializer.memory.MemoryMetadataProvider;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;

/**
 * A REST connection describes the whole client on its own: timeouts and proxy settings live here
 * rather than on the transform that selects it.
 */
class RestConnectionSettingsTest {

  @BeforeAll
  static void initPasswordEncoder() throws Exception {
    PluginRegistry.getInstance()
        .registerPluginClass(
            HopTwoWayPasswordEncoder.class.getName(),
            TwoWayPasswordEncoderPluginType.class,
            TwoWayPasswordEncoderPlugin.class);
    Encr.init("Hop");
  }

  private static RestConnection connection() {
    RestConnection connection = new RestConnection(new Variables());
    connection.setBaseUrl("https://api.example.com");
    return connection;
  }

  @Test
  void aConnectionStoredWithTheRemovedCertificateAuthTypeStillSendsNoAuthHeader() throws Exception {
    // "Certificate" used to be offered as an authentication type but never contributed anything:
    // mTLS comes from the keystore fields and applies whatever the auth type says. Now that the
    // option is gone, such a connection has to keep behaving exactly as it always did.
    RestConnection connection =
        fromJson("{\"base_url\":\"https://api.example.com\",\"auth_type\":\"Certificate\"}");

    RestClientSettings settings = connection.createClientSettings();

    assertEquals(RestAuthType.NONE, settings.getAuthType());
    Map<String, String> headers = new LinkedHashMap<>();
    new RestAuthenticator(settings).applyRequestHeaders(headers, "https://api.example.com/x");
    assertTrue(headers.isEmpty(), "an unknown auth type must not invent a header");
  }

  private static RestConnection fromJson(String json) throws Exception {
    JsonParser parser = new JsonFactory().createParser(json);
    parser.nextToken();
    return new JsonMetadataParser<>(RestConnection.class, new MemoryMetadataProvider())
        .loadJsonObject(RestConnection.class, parser);
  }

  @Test
  void timeoutsAreResolvedFromTheConnection() throws HopException {
    RestConnection connection = connection();
    connection.setConnectTimeout("1500");
    connection.setReadTimeout("2500");

    RestClientSettings settings = connection.createClientSettings();

    assertEquals(1500, settings.getConnectTimeout());
    assertEquals(2500, settings.getReadTimeout());
  }

  @Test
  void emptyTimeoutsAreLeftUnset() throws HopException {
    RestClientSettings settings = connection().createClientSettings();

    // Jersey rejects a negative timeout, so an empty field has to leave the property alone.
    assertNull(settings.getConnectTimeout());
    assertNull(settings.getReadTimeout());
  }

  @Test
  void proxySettingsAreResolvedFromTheConnection() throws HopException {
    RestConnection connection = connection();
    connection.setProxyScheme("https");
    connection.setProxyHost("proxy.example.com");
    connection.setProxyPort("3128");
    connection.setProxyUsername("proxyuser");
    connection.setProxyPassword("proxypass");
    connection.setNonProxyHosts("localhost|*.internal");

    RestClientSettings settings = connection.createClientSettings();

    assertEquals("https", settings.getProxyScheme());
    assertEquals("proxy.example.com", settings.getProxyHost());
    assertEquals(3128, settings.getProxyPort());
    assertEquals("proxyuser", settings.getProxyUsername());
    assertEquals("proxypass", settings.getProxyPassword());
    assertEquals("localhost|*.internal", settings.getNonProxyHosts());
  }

  @Test
  void proxyFieldsAreIgnoredWithoutAHost() throws HopException {
    RestConnection connection = connection();
    connection.setProxyPort("3128");
    connection.setProxyUsername("proxyuser");

    RestClientSettings settings = connection.createClientSettings();

    assertNull(settings.getProxyHost());
    assertNull(settings.getProxyUsername());
  }

  @Test
  void variablesAreResolvedInProxyAndTimeoutFields() throws HopException {
    Variables variables = new Variables();
    variables.setVariable("PROXY_HOST", "proxy.example.com");
    variables.setVariable("PROXY_PORT", "3128");
    variables.setVariable("READ_TIMEOUT", "4000");

    RestConnection connection = new RestConnection(variables);
    connection.setProxyHost("${PROXY_HOST}");
    connection.setProxyPort("${PROXY_PORT}");
    connection.setReadTimeout("${READ_TIMEOUT}");

    RestClientSettings settings = connection.createClientSettings();

    assertEquals("proxy.example.com", settings.getProxyHost());
    assertEquals(3128, settings.getProxyPort());
    assertEquals(4000, settings.getReadTimeout());
  }

  @Test
  void basicAuthIsPreemptiveByDefault() throws HopException {
    RestConnection connection = connection();
    connection.setAuthType(RestConnection.BASIC);
    connection.setUsername("user");
    connection.setPassword("password");

    assertTrue(connection.createClientSettings().isBasicPreemptive());
  }

  @Test
  void basicAuthCanBeMadeChallengeResponse() throws HopException {
    RestConnection connection = connection();
    connection.setAuthType(RestConnection.BASIC);
    connection.setUsername("user");
    connection.setPassword("password");
    connection.setPreemptiveBasicAuth(false);

    assertFalse(connection.createClientSettings().isBasicPreemptive());
  }

  @Test
  void aConnectionSavedBeforeTheseOptionsExistedKeepsItsBehaviour() throws Exception {
    // The new keys are additive. A connection stored before they existed has to deserialize with
    // preemptive Basic auth — which is what every REST call did — and no proxy or timeouts.
    RestConnection connection =
        fromJson(
            "{\"base_url\":\"https://api.example.com\",\"auth_type\":\"Basic\","
                + "\"username\":\"user\",\"password\":\"password\"}");

    assertTrue(connection.isPreemptiveBasicAuth());
    assertNull(connection.getProxyHost());
    assertNull(connection.getConnectTimeout());
    assertNull(connection.getReadTimeout());
  }

  @Test
  void anExplicitlyStoredPreemptiveFlagIsHonoured() throws Exception {
    assertFalse(
        fromJson("{\"base_url\":\"https://api.example.com\",\"non_preemptive_basic_auth\":true}")
            .isPreemptiveBasicAuth());
    assertTrue(
        fromJson("{\"base_url\":\"https://api.example.com\",\"non_preemptive_basic_auth\":false}")
            .isPreemptiveBasicAuth());
  }
}
