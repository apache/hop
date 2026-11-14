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

import java.net.URI;
import lombok.Getter;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.pipeline.transforms.salesforce.SalesforceConnection;

/** Bearer token and instance root for Bulk API 2.0. */
@Getter
public class SalesforceBulkSession {
  private static final Class<?> PKG = SalesforceBulkSession.class;

  private final String instanceUrl;
  private final String accessToken;
  private final String apiVersion;

  public SalesforceBulkSession(String instanceUrl, String accessToken, String apiVersion) {
    this.instanceUrl = instanceUrl;
    this.accessToken = accessToken;
    this.apiVersion = apiVersion;
  }

  /**
   * Read the session from a connection that already has an OAuth token, or that has logged in with
   * a username and password. The SOAP session id is the bearer token. The REST host is the host of
   * the SOAP server URL.
   */
  public static SalesforceBulkSession from(SalesforceConnection connection) throws HopException {
    String token = connection.getOauthAccessToken();
    String instance = connection.getOauthInstanceUrl();
    if (Utils.isEmpty(token) && connection.getLoginResult() != null) {
      token = connection.getLoginResult().getSessionId();
    }
    if (Utils.isEmpty(instance) && connection.getLoginResult() != null) {
      instance = connection.getLoginResult().getServerUrl();
    }
    instance = instanceRoot(instance);
    if (Utils.isEmpty(token) || Utils.isEmpty(instance)) {
      throw new HopException(BaseMessages.getString(PKG, "SalesforceBulk.Error.MissingSession"));
    }
    return new SalesforceBulkSession(instance, token, apiVersion(connection.getApiVersion()));
  }

  /** Drop a SOAP or REST path so the result is {@code scheme://host[:port]}. */
  public static String instanceRoot(String url) {
    if (Utils.isEmpty(url)) {
      return null;
    }
    String trimmed = url.trim();
    int services = trimmed.indexOf("/services/");
    if (services > 0) {
      trimmed = trimmed.substring(0, services);
    }
    while (trimmed.endsWith("/")) {
      trimmed = trimmed.substring(0, trimmed.length() - 1);
    }
    try {
      URI uri = URI.create(trimmed);
      if (uri.getScheme() != null && uri.getRawAuthority() != null) {
        return uri.getScheme() + "://" + uri.getRawAuthority();
      }
    } catch (IllegalArgumentException ignored) {
      // Fall through to the trimmed text.
    }
    return trimmed;
  }

  static String apiVersion(String version) {
    if (Utils.isEmpty(version)) {
      return "64.0";
    }
    String trimmed = version.trim();
    if (trimmed.startsWith("v") || trimmed.startsWith("V")) {
      trimmed = trimmed.substring(1);
    }
    return trimmed;
  }
}
