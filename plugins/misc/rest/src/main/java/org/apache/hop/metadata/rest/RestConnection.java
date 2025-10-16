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
import jakarta.ws.rs.core.Response;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;

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

  public RestConnection(IVariables variables) {
    this.variables = variables;
    builder = ClientBuilder.newBuilder();
    client = builder.build();
  }

  public String getResponse(String url) throws HopException {
    WebTarget target = client.target(testUrl);
    Invocation.Builder invocationBuilder = target.request();
    if (!StringUtils.isEmpty(variables.resolve(authorizationPrefix))) {
      invocationBuilder.header(
          variables.resolve(authorizationHeaderName),
          variables.resolve(authorizationPrefix)
              + " "
              + variables.resolve(authorizationHeaderValue));
    } else {
      invocationBuilder.header(
          variables.resolve(authorizationHeaderName), variables.resolve(authorizationHeaderValue));
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
    WebTarget target = client.target(variables.resolve(testUrl));
    Invocation.Builder invocationBuilder = target.request();

    // only set the header if we have a header name
    if (!StringUtils.isEmpty(variables.resolve(authorizationHeaderName))) {
      if (!StringUtils.isEmpty(variables.resolve(authorizationPrefix))) {
        invocationBuilder.header(
            variables.resolve(authorizationHeaderName),
            variables.resolve(authorizationPrefix)
                + " "
                + variables.resolve(authorizationHeaderValue));
      } else {
        invocationBuilder.header(
            variables.resolve(authorizationHeaderName),
            variables.resolve(authorizationHeaderValue));
      }
    }
    Response response = invocationBuilder.get();
    if (response.getStatus() != Response.Status.OK.getStatusCode()) {
      throw new HopException("Error connecting to " + testUrl + ": " + response.getStatus());
    }
    response.close();
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
