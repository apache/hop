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

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;
import javax.ws.rs.client.WebTarget;
import javax.ws.rs.core.Response;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

@HopMetadata(
    key = "restconnection",
    name = "i18n::RestConnection.name",
    description = "i18n::RestConnection.description",
    image = "rest.svg",
    documentationUrl = "/metadata-types/rest-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.REST_CONNECTION)
public class RestConnection extends HopMetadataBase implements IHopMetadata {

  private ClientBuilder builder;
  private Client client;

  @HopMetadataProperty(key = "base_url", injectionKey = "BASE_URL")
  private String baseUrl;

  @HopMetadataProperty(key = "test_url", injectionKey = "TEST_URL")
  private String testUrl;

  @HopMetadataProperty(key = "auth_header_name", injectionKey = "AUTH_HEADER_NAME")
  private String authorizationHeaderName;

  @HopMetadataProperty(key = "auth_header_prefix", injectionKey = "AUTH_HEADER_PREFIX")
  private String authorizationPrefix;

  @HopMetadataProperty(key = "auth_header_value", injectionKey = "AUTH_HEADER_VALUE")
  private String authorizationHeaderValue;

  public RestConnection() {
    builder = ClientBuilder.newBuilder();
    client = builder.build();
  }

  public String getResponse(String url) throws HopException {
    WebTarget target = client.target(testUrl);
    Invocation.Builder invocationBuilder = target.request();
    if (!StringUtils.isEmpty(authorizationPrefix)) {
      invocationBuilder.header(
          authorizationHeaderName, authorizationPrefix + " " + authorizationHeaderValue);
    } else {
      invocationBuilder.header(authorizationHeaderName, authorizationHeaderValue);
    }
    Response response = invocationBuilder.get();

    if (response.getStatus() != Response.Status.OK.getStatusCode()) {
      throw new HopException("Error connecting to " + testUrl + ": " + response.getStatus());
    }
    return response.readEntity(String.class);
  }

  public void disconnect() throws HopException {
    client.close();
  }

  public void testConnection() throws HopException {
    WebTarget target = client.target(testUrl);
    Invocation.Builder invocationBuilder = target.request();
    if (!StringUtils.isEmpty(authorizationPrefix)) {
      invocationBuilder.header(
          authorizationHeaderName, authorizationPrefix + " " + authorizationHeaderValue);
    } else {
      invocationBuilder.header(authorizationHeaderName, authorizationHeaderValue);
    }
    Response response = invocationBuilder.get();
    if (response.getStatus() != Response.Status.OK.getStatusCode()) {
      throw new HopException("Error connecting to " + testUrl + ": " + response.getStatus());
    }
    response.close();
  }

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

  public String getBaseUrl() {
    return baseUrl;
  }

  public void setBaseUrl(String baseUrl) {
    this.baseUrl = baseUrl;
  }

  public String getTestUrl() {
    return testUrl;
  }

  public void setTestUrl(String testUrl) {
    this.testUrl = testUrl;
  }

  public String getAuthorizationHeaderName() {
    return authorizationHeaderName;
  }

  public void setAuthorizationHeaderName(String authorizationHeaderName) {
    this.authorizationHeaderName = authorizationHeaderName;
  }

  public String getAuthHeaderPrefix() {
    return authorizationPrefix;
  }

  public void setAuthHeaderPrefix(String authHeaderPrefix) {
    this.authorizationPrefix = authHeaderPrefix;
  }

  public String getAuthorizationHeaderValue() {
    return authorizationHeaderValue;
  }

  public void setAuthorizationHeaderValue(String authorizationHeaderValue) {
    this.authorizationHeaderValue = authorizationHeaderValue;
  }
}
