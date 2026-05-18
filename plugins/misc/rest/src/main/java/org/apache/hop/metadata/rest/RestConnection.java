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
import jakarta.ws.rs.core.MultivaluedHashMap;
import jakarta.ws.rs.core.MultivaluedMap;
import jakarta.ws.rs.core.Response;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.glassfish.jersey.client.ClientProperties;
import org.glassfish.jersey.client.authentication.HttpAuthenticationFeature;

@Getter
@Setter
@HopMetadata(
    key = "restconnection",
    name = "i18n::RestConnection.name",
    description = "i18n::RestConnection.description",
    image = "rest.svg",
    documentationUrl = "/metadata-types/rest-connection.html",
    hopMetadataPropertyType = HopMetadataPropertyType.REST_CONNECTION,
    supportsGlobalReplace = true)
// It is optional to disable SSL/TLS
@SuppressWarnings({"java:S5527", "java:S4830", "java:S4423"})
public class RestConnection extends HopMetadataBase implements IHopMetadata {
  public static final String BASIC = "Basic";
  public static final String API_KEY = "API Key";
  public static final String BEARER = "Bearer";

  private IVariables variables;
  private ClientBuilder builder;
  private Client client;
  private transient ILogChannel log;

  private ILogChannel getLog() {
    if (log == null) {
      log = new LogChannel("RestConnection");
    }
    return log;
  }

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
  @HopMetadataProperty(key = "bearer_token", password = true)
  private String bearerToken;

  // API auth
  @HopMetadataProperty(key = "auth_header_name")
  private String authorizationHeaderName;

  @HopMetadataProperty(key = "auth_header_prefix")
  private String authorizationPrefix;

  @HopMetadataProperty(key = "auth_header_value", password = true)
  private String authorizationHeaderValue;

  // Client certificate (KeyStore) fields
  @HopMetadataProperty(key = "keyStoreFile")
  private String keyStoreFile;

  @HopMetadataProperty(key = "keyStorePassword", password = true)
  private String keyStorePassword;

  @HopMetadataProperty(key = "keyStoreType")
  private String keyStoreType;

  @HopMetadataProperty(key = "keyPassword", password = true)
  private String keyPassword;

  @HopMetadataProperty(key = "certificateAlias")
  private String certificateAlias;

  /** Optional API pagination semantics (NONE by default). */
  @HopMetadataProperty(key = "pagination_type")
  private RestPaginationType paginationType = RestPaginationType.NONE;

  /**
   * Query parameter name used for cursor-based paging or page-number paging ({@link
   * #paginationType}).
   */
  @HopMetadataProperty(key = "page_param_name")
  private String pageParamName;

  @HopMetadataProperty(key = "offset_param_name")
  private String offsetParamName;

  @HopMetadataProperty(key = "limit_param_name")
  private String limitParamName;

  /** Default page size used with {@link RestPaginationType#OFFSET_LIMIT} when not overridden. */
  @HopMetadataProperty(key = "default_limit")
  private int defaultLimit;

  /** JsonPath against the JSON response body to read the next cursor token. */
  @HopMetadataProperty(key = "cursor_json_path")
  private String cursorJsonPath;

  /** XPath against the XML response body to read the next cursor token. */
  @HopMetadataProperty(key = "cursor_x_path")
  private String cursorXPath;

  public RestConnection(IVariables variables) {
    this.variables = variables;
  }

  public Invocation.Builder getInvocationBuilder(String url) throws HopException {
    return getInvocationBuilder(url, null, 8080);
  }

  public Invocation.Builder getInvocationBuilder(String url, String proxyHost, Integer proxyPort)
      throws HopException {
    builder = ClientBuilder.newBuilder();
    setProxyHost(proxyHost, proxyPort);

    // Configure SSL if needed (client cert, trust store, or ignore SSL)
    if (needsSslConfiguration()) {
      try {
        // Build SSL context
        javax.net.ssl.SSLContext sslContext = buildSslContext();
        getLog()
            .logDetailed(
                "SSL context built. ignoreSsl="
                    + ignoreSsl
                    + ", trustStoreFile="
                    + Const.NVL(trustStoreFile, "<empty>")
                    + ", keyStoreFile="
                    + Const.NVL(keyStoreFile, "<empty>"));

        // Apply SSL context to client builder
        builder = builder.sslContext(sslContext);

        // Set hostname verifier if ignoring SSL or using custom truststore
        if (ignoreSsl || !Utils.isEmpty(trustStoreFile)) {
          getLog().logDetailed("Enabling permissive hostname verifier.");
          builder = builder.hostnameVerifier((hostname, session) -> true);
        }

        // Don't use Apache connector - default connector properly respects SSL context

      } catch (Exception e) {
        throw new HopException("Error configuring SSL for REST connection", e);
      }
    }

    client = builder.build();

    WebTarget target = client.target(url);
    Invocation.Builder invocationBuilder = target.request();

    // backwards compatibility with early version of this metadata type.
    if (StringUtils.isEmpty(authType)) {
      if (!StringUtils.isEmpty(authorizationHeaderName)
          && !StringUtils.isEmpty(authorizationHeaderValue)) {
        authType = API_KEY;
      } else {
        authType = "No Auth";
      }
    }

    if (authTypeEquals(BASIC) && !StringUtils.isEmpty(username) && !StringUtils.isEmpty(password)) {
      client.register(
          HttpAuthenticationFeature.basic(
              resolve(username), Encr.decryptPasswordOptionallyEncrypted(resolve(password))));
      target = client.target(url);
      invocationBuilder = target.request();
    } else {
      applyBearerAndApiKeyHeaders(invocationBuilder, null);
    }
    return invocationBuilder;
  }

  /**
   * Applies API-key or Bearer auth to outbound headers (when {@code outboundHeaders} is non-null)
   * or to {@code Invocation.Builder} via successive {@link Invocation.Builder#header} calls (when
   * {@code outboundHeaders} is null). Skips when credentials are already on the map ({@code
   * Authorization}, or the configured API-key header name). Intended for merging into maps before
   * {@link Invocation.Builder#headers(javax.ws.rs.core.MultivaluedMap)}, which replaces Jersey's
   * prior headers.
   */
  public void applyBearerAndApiKeyHeaders(
      Invocation.Builder invocationBuilder, MultivaluedMap<String, Object> outboundHeaders) {
    if (invocationBuilder == null || !supportsBearerOrApiKeyAuthHeaders()) {
      return;
    }
    if (authHeadersAlreadyProvidedFromMap(outboundHeaders)) {
      return;
    }

    MultivaluedMap<String, Object> authOnly =
        outboundHeaders != null ? outboundHeaders : new MultivaluedHashMap<>();

    if (authTypeEquals(API_KEY)) {
      String headerName = resolve(authorizationHeaderName);
      String headerValue =
          Encr.decryptPasswordOptionallyEncrypted(resolve(authorizationHeaderValue));
      if (Utils.isEmpty(headerName) || Utils.isEmpty(headerValue)) {
        return;
      }
      if (!StringUtils.isEmpty(resolve(authorizationPrefix))) {
        authOnly.putSingle(headerName, resolve(authorizationPrefix) + " " + headerValue);
      } else {
        authOnly.putSingle(headerName, headerValue);
      }
    } else if (authTypeEquals(BEARER)) {
      String token = Encr.decryptPasswordOptionallyEncrypted(resolve(bearerToken));
      if (Utils.isEmpty(token)) {
        return;
      }
      authOnly.putSingle(HttpHeaders.AUTHORIZATION, "Bearer " + token);
    }

    if (outboundHeaders == null) {
      authOnly.forEach(
          (name, values) -> {
            for (Object val : values) {
              invocationBuilder.header(name, val);
            }
          });
    }
  }

  /** True when this connection may add Bearer / API-key style headers before a request runs. */
  private boolean supportsBearerOrApiKeyAuthHeaders() {
    return authType != null && (authTypeEquals(API_KEY) || authTypeEquals(BEARER));
  }

  /**
   * When merging into an outbound header map, skip adding connection auth if the map already
   * supplies credentials: {@code Authorization}, or the connection's API-key header name (rows
   * win).
   */
  private boolean authHeadersAlreadyProvidedFromMap(
      MultivaluedMap<String, Object> outboundHeaders) {
    if (outboundHeaders == null || outboundHeaders.isEmpty()) {
      return false;
    }
    for (String key : outboundHeaders.keySet()) {
      if (key == null) {
        continue;
      }
      if ("Authorization".equalsIgnoreCase(key) && nonEmptyHeaderValue(outboundHeaders, key)) {
        return true;
      }
      if (authTypeEquals(API_KEY)) {
        String configuredName = resolve(authorizationHeaderName);
        if (!Utils.isEmpty(configuredName)
            && configuredName.equalsIgnoreCase(key)
            && nonEmptyHeaderValue(outboundHeaders, key)) {
          return true;
        }
      }
    }
    return false;
  }

  private boolean authTypeEquals(String canonicalLabel) {
    return authType != null
        && canonicalLabel != null
        && authType.trim().equalsIgnoreCase(canonicalLabel.trim());
  }

  private static boolean nonEmptyHeaderValue(MultivaluedMap<String, Object> headers, String key) {
    Object o = headers.getFirst(key);
    return o != null && !Utils.isEmpty(o.toString().trim());
  }

  private void setProxyHost(String proxyHost, Integer proxyPort) {
    if (!Utils.isEmpty(proxyHost) && proxyPort != null) {
      builder =
          builder.property(ClientProperties.PROXY_URI, "http://" + proxyHost + ":" + proxyPort);
    }
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

  /**
   * Verifies connectivity using {@link #testUrl}. Mirrors the outbound header sequence used by the
   * REST transform ({@link jakarta.ws.rs.client.Invocation.Builder#headers}: merge Bearer / API-key
   * headers into the map before applying it); that avoids misleading green tests where only {@link
   * jakarta.ws.rs.client.Invocation.Builder#header} ran and pagination later replaced headers
   * without auth.
   */
  public void testConnection() throws HopException {
    String url = resolve(testUrl);
    Invocation.Builder invocationBuilder = getInvocationBuilder(url);
    if (supportsBearerOrApiKeyAuthHeaders()) {
      MultivaluedHashMap<String, Object> outbound = new MultivaluedHashMap<>();
      applyBearerAndApiKeyHeaders(invocationBuilder, outbound);
      invocationBuilder.headers(outbound);
    }
    Response response = invocationBuilder.get();
    response.close();
  }

  public void disconnect() {
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

  /**
   * Determines if SSL configuration is needed
   *
   * @return true if SSL needs to be configured
   */
  private boolean needsSslConfiguration() {
    return ignoreSsl || !Utils.isEmpty(trustStoreFile) || !Utils.isEmpty(keyStoreFile);
  }

  /**
   * Builds SSL context with trust store and/or key store
   *
   * @return configured SSLContext
   * @throws Exception if SSL configuration fails
   */
  private javax.net.ssl.SSLContext buildSslContext() throws Exception {
    // Build SSL context - use "SSL" not "TLS" for better compatibility
    javax.net.ssl.SSLContext sslContext = javax.net.ssl.SSLContext.getInstance("SSL");

    // Load trust managers (for server certificate validation)
    // This will return trust-all managers if ignoreSsl=true
    TrustManager[] trustManagers = loadTrustManagers();

    // Load key managers (for client certificate authentication)
    // This will return null if no keystore is configured
    KeyManager[] keyManagers = loadKeyManagers();

    // Initialize SSL context with both managers
    sslContext.init(keyManagers, trustManagers, new java.security.SecureRandom());

    return sslContext;
  }

  /**
   * Loads trust managers for server certificate validation
   *
   * @return array of TrustManagers or null for default
   * @throws Exception if trust store loading fails
   */
  private TrustManager[] loadTrustManagers() throws Exception {
    // If ignoring SSL, create a trust-all manager
    if (ignoreSsl) {
      getLog().logDetailed("ignoreSsl=true -> using trust-all TrustManager.");
      return new TrustManager[] {
        new X509TrustManager() {
          @Override
          public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
          }

          @Override
          public void checkClientTrusted(X509Certificate[] certs, String authType) {
            // Trust all - do nothing
          }

          @Override
          public void checkServerTrusted(X509Certificate[] certs, String authType) {
            // Trust all - do nothing
          }
        }
      };
    }

    // If no trust store file specified, use system default
    if (Utils.isEmpty(trustStoreFile)) {
      getLog()
          .logDetailed("No trust store configured. Falling back to default system trust store.");
      // Use default system trust store
      return null;
    }

    // Load custom trust store
    String resolvedTrustStoreFile = resolve(trustStoreFile);
    getLog().logDetailed("Loading trust store from: " + resolvedTrustStoreFile);
    String resolvedTrustStorePassword =
        Encr.decryptPasswordOptionallyEncrypted(resolve(trustStorePassword));

    KeyStore trustStore = KeyStore.getInstance("JKS");
    try (FileInputStream fis = new FileInputStream(resolvedTrustStoreFile)) {
      trustStore.load(fis, resolvedTrustStorePassword.toCharArray());
      getLog().logDetailed("Trust store loaded successfully.");
    } catch (FileNotFoundException e) {
      throw new HopException("Trust store file not found: " + resolvedTrustStoreFile, e);
    } catch (Exception e) {
      throw new HopException(
          "Failed to load trust store from "
              + resolvedTrustStoreFile
              + ". Check file path and password.",
          e);
    }

    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);

    return tmf.getTrustManagers();
  }

  /**
   * Loads key managers for client certificate authentication
   *
   * @return array of KeyManagers or null if no client certificate
   * @throws Exception if key store loading fails
   */
  private KeyManager[] loadKeyManagers() throws Exception {
    // If no key store file specified, no client certificate
    if (Utils.isEmpty(keyStoreFile)) {
      getLog().logDetailed("No key store configured. Skipping client certificate setup.");
      return null;
    }

    String resolvedKeyStoreFile = resolve(keyStoreFile);
    getLog()
        .logDetailed(
            "Loading key store from: "
                + resolvedKeyStoreFile
                + " (type="
                + Const.NVL(keyStoreType, "PKCS12")
                + ")");
    String resolvedKeyStorePassword =
        Encr.decryptPasswordOptionallyEncrypted(resolve(keyStorePassword));

    // Determine key store type (default to PKCS12 if not specified)
    String storeType = Utils.isEmpty(keyStoreType) ? "PKCS12" : resolve(keyStoreType);

    // Load key store
    KeyStore keyStore = KeyStore.getInstance(storeType);
    try (FileInputStream fis = new FileInputStream(resolvedKeyStoreFile)) {
      keyStore.load(fis, resolvedKeyStorePassword.toCharArray());
      getLog().logDetailed("Key store loaded successfully.");
    } catch (FileNotFoundException e) {
      throw new HopException("Key store file not found: " + resolvedKeyStoreFile, e);
    } catch (Exception e) {
      throw new HopException(
          "Failed to load key store from "
              + resolvedKeyStoreFile
              + ". Check file path, password, and key store type ("
              + storeType
              + ").",
          e);
    }

    // Initialize key manager factory
    KeyManagerFactory kmf = KeyManagerFactory.getInstance(KeyManagerFactory.getDefaultAlgorithm());

    // Use separate key password if specified, otherwise use key store password
    char[] keyPass =
        Utils.isEmpty(keyPassword)
            ? resolvedKeyStorePassword.toCharArray()
            : Encr.decryptPasswordOptionallyEncrypted(resolve(keyPassword)).toCharArray();

    kmf.init(keyStore, keyPass);

    return kmf.getKeyManagers();
  }

  private String resolve(String value) {
    if (value == null) {
      return null;
    }
    if (variables != null) {
      return variables.resolve(value);
    }
    return value;
  }
}
