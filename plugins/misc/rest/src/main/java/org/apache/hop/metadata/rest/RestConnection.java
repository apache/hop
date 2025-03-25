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

import jakarta.ws.rs.client.Client;
import jakarta.ws.rs.client.ClientBuilder;
import jakarta.ws.rs.client.Invocation;
import jakarta.ws.rs.client.WebTarget;
import jakarta.ws.rs.core.HttpHeaders;
import jakarta.ws.rs.core.Response;
import java.security.KeyManagementException;
import java.security.NoSuchAlgorithmException;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.HttpClientManager;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

@Getter
@Setter
@HopMetadata(
    key = "restconnection",
    name = "i18n::RestConnection.name",
    description = "i18n::RestConnection.description",
    image = "rest.svg",
    documentationUrl = "/metadata-types/rest-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.REST_CONNECTION)
public class RestConnection extends HopMetadataBase implements IHopMetadata {

  private IVariables variables;
  private ClientBuilder builder;
  private Client client;

  @HopMetadataProperty(key = "base_url")
  private String baseUrl;

  @HopMetadataProperty(key = "test_url")
  private String testUrl;

  @HopMetadataProperty(key = "trustStoreFile")
  private String trustStoreFile;

  @HopMetadataProperty(key = "trustStorePassword", password = true)
  private String trustStorePassword;

  @HopMetadataProperty(key = "ignoreSsl")
  private boolean ignoreSsl;

  @HopMetadataProperty(key = "auth_type")
  private String authType;

  // Basic auth
  @HopMetadataProperty(key = "username")
  private String username;

  @HopMetadataProperty(key = "password", password = true)
  private String password;

  // Bearer auth
  @HopMetadataProperty(key = "bearer_token")
  private String bearerToken;

  // API auth
  @HopMetadataProperty(key = "auth_header_name")
  private String authorizationHeaderName;

  @HopMetadataProperty(key = "auth_header_prefix")
  private String authorizationPrefix;

  @HopMetadataProperty(key = "auth_header_value", password = true)
  private String authorizationHeaderValue;

  public RestConnection(IVariables variables) {
    this.variables = variables;
  }

  public Invocation.Builder getInvocationBuilder(String url) throws HopException {

    builder = ClientBuilder.newBuilder();

    if (isIgnoreSsl() || !Utils.isEmpty(getTrustStoreFile())) {
      builder.hostnameVerifier((s1, s2) -> true);
      try {
        builder.sslContext(HttpClientManager.getTrustAllSslContext());
      } catch (NoSuchAlgorithmException | KeyManagementException e) {
        throw new HopException("Error while setting up SSL context", e);
      }
    }
    client = builder.build();

    WebTarget target = client.target(url);
    Invocation.Builder invocationBuilder = target.request();

    // backwards compatibility with early version of this metadata type.
    if (StringUtils.isEmpty(authType)) {
      if (!StringUtils.isEmpty(authorizationHeaderName)
          && !StringUtils.isEmpty(authorizationHeaderValue)) {
        authType = "API Key";
      } else {
        authType = "No Auth";
      }
    }

    if (authType.equals("No Auth")) {
      // Nothing required
    }
    if (authType.equals("Basic")) {
      if (!StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {

        client.register(
            HttpAuthenticationFeature.basic(
                variables.resolve(username),
                Encr.decryptPasswordOptionallyEncrypted(variables.resolve(password))));
        target = client.target(url);
        invocationBuilder = target.request();
      }
    } else if (authType.equals("API Key")) {
      if (!StringUtils.isEmpty(variables.resolve(authorizationPrefix))) {
        invocationBuilder.header(
            variables.resolve(authorizationHeaderName),
            variables.resolve(authorizationPrefix)
                + " "
                + Encr.decryptPasswordOptionallyEncrypted(
                    variables.resolve(authorizationHeaderValue)));
      } else {
        invocationBuilder.header(
            variables.resolve(authorizationHeaderName),
            Encr.decryptPasswordOptionallyEncrypted(variables.resolve(authorizationHeaderValue)));
      }
    } else if (authType.equals("Bearer")) {
      if (!StringUtils.isEmpty(bearerToken)) {
        invocationBuilder.header(
            HttpHeaders.AUTHORIZATION, "Bearer " + variables.resolve(bearerToken));
      }
    }
    return invocationBuilder;
  }

  public String getResponse(String url) throws HopException {
    return getResponseFromUrl(url).readEntity(String.class);
  }

  public Response getResponseFromUrl(String url) throws HopException {
    Response response = getInvocationBuilder(url).get();

    if (response.getStatus() != Response.Status.OK.getStatusCode()) {
      throw new HopException("Error connecting to " + testUrl + ": " + response.getStatus());
    }
    return response;
  }

  public void testConnection() throws HopException {
    Response response = getInvocationBuilder(variables.resolve(testUrl)).get();
    response.close();
  }

  public void disconnect() throws HopException {
    client.close();
  }

  public RestConnection() {}

  public RestConnection(RestConnection connection) {
    this.baseUrl = connection.baseUrl;
  }

  @Override
  public String toString() {
    return name == null ? super.toString() : name;
  }

  @Override
  public int hashCode() {
    return name == null ? super.hashCode() : name.hashCode();
  }

  @Override
  public boolean equals(Object object) {

    if (object == this) {
      return true;
    }
    if (!(object instanceof RestConnection)) {
      return false;
    }

    RestConnection connection = (RestConnection) object;

    return name != null && name.equalsIgnoreCase(connection.name);
  }

  /**
   * Gets name
   *
   * @return value of name
   */
  @Override
  public String getName() {
    return name;
  }

  /**
   * @param name The name to set
   */
  @Override
  public void setName(String name) {
    this.name = name;
  }
}
