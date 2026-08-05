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

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.security.KeyStore;
import java.security.cert.X509Certificate;
import java.util.LinkedHashMap;
import java.util.Map;
import javax.net.ssl.KeyManager;
import javax.net.ssl.KeyManagerFactory;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import lombok.Getter;
import lombok.Setter;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.core5.http.HttpEntity;
import org.apache.hc.core5.http.HttpStatus;
import org.apache.hc.core5.http.io.entity.EntityUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.metadata.api.HopMetadata;
import org.apache.hop.metadata.api.HopMetadataBase;
import org.apache.hop.metadata.api.HopMetadataCategory;
import org.apache.hop.metadata.api.HopMetadataProperty;
import org.apache.hop.metadata.api.HopMetadataPropertyType;
import org.apache.hop.metadata.api.IHopMetadata;
import org.apache.hop.metadata.rest.client.RestAuthType;
import org.apache.hop.metadata.rest.client.RestAuthenticator;
import org.apache.hop.metadata.rest.client.RestClientFactory;
import org.apache.hop.metadata.rest.client.RestClientSettings;

@Getter
@Setter
@HopMetadata(
    key = "restconnection",
    name = "i18n::RestConnection.name",
    description = "i18n::RestConnection.description",
    image = "rest.svg",
    category = HopMetadataCategory.CONNECTIONS,
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

  /** Connect timeout in milliseconds. Empty leaves it unlimited. */
  @HopMetadataProperty(key = "connect_timeout")
  private String connectTimeout;

  /** Read timeout in milliseconds. Empty leaves it unlimited. */
  @HopMetadataProperty(key = "read_timeout")
  private String readTimeout;

  /** Scheme used to reach the proxy itself, not the target. Empty means {@code http}. */
  @HopMetadataProperty(key = "proxy_scheme")
  private String proxyScheme;

  @HopMetadataProperty(key = "proxy_host")
  private String proxyHost;

  /** Proxy port. Empty defaults to 8080 for an http proxy and 443 for an https one. */
  @HopMetadataProperty(key = "proxy_port")
  private String proxyPort;

  @HopMetadataProperty(key = "proxy_username")
  private String proxyUsername;

  @HopMetadataProperty(key = "proxy_password", password = true)
  private String proxyPassword;

  /**
   * Hosts that bypass the proxy, in JDK {@code http.nonProxyHosts} syntax: entries separated by
   * {@code |} (commas and semicolons work too), each optionally using {@code *} as a wildcard, for
   * example {@code localhost|127.*|*.internal.example.com}.
   */
  @HopMetadataProperty(key = "non_proxy_hosts")
  private String nonProxyHosts;

  @HopMetadataProperty(key = "auth_type")
  private String authType;

  // Basic auth
  @HopMetadataProperty(key = "username")
  private String username;

  @HopMetadataProperty(key = "password", password = true)
  private String password;

  /**
   * Wait for a 401 challenge before sending the Basic credentials, rather than sending them on the
   * first request.
   *
   * <p>Stored inverted on purpose. Deserialization sets an absent boolean key to false rather than
   * leaving a field initializer alone, so a connection saved before this option existed loads as
   * {@code false} — which has to mean preemptive, because that is what every REST call has always
   * done. Read it through {@link #isPreemptiveBasicAuth()}.
   */
  @HopMetadataProperty(key = "non_preemptive_basic_auth")
  private boolean nonPreemptiveBasicAuth;

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

  /**
   * JsonPath against the JSON response body to read the next page URL ({@link
   * RestPaginationType#BODY_NEXT_URL}).
   */
  @HopMetadataProperty(key = "next_page_url_json_path")
  private String nextPageUrlJsonPath;

  /**
   * XPath against the XML response body to read the next page URL ({@link
   * RestPaginationType#BODY_NEXT_URL}).
   */
  @HopMetadataProperty(key = "next_page_url_x_path")
  private String nextPageUrlXPath;

  public RestConnection(IVariables variables) {
    this.variables = variables;
  }

  /**
   * Translates this connection into the resolved settings needed to build an HTTP client. A
   * connection describes the whole client on its own: a transform that selects one contributes
   * nothing but the request.
   *
   * <p>The caller owns the resulting client: build one per transform copy with {@link
   * org.apache.hop.metadata.rest.client.RestClientFactory} and close it when the transform is
   * disposed, rather than one per request.
   */
  public RestClientSettings createClientSettings() throws HopException {
    normalizeAuthType();

    RestClientSettings settings = new RestClientSettings();

    // Only apply a timeout that is actually configured. An empty field resolves to -1, and Jersey
    // rejects a negative timeout; leaving the property unset keeps its default of 0 (infinite).
    int resolvedConnectTimeout = Const.toInt(resolve(connectTimeout), -1);
    if (resolvedConnectTimeout >= 0) {
      settings.setConnectTimeout(resolvedConnectTimeout);
    }
    int resolvedReadTimeout = Const.toInt(resolve(readTimeout), -1);
    if (resolvedReadTimeout >= 0) {
      settings.setReadTimeout(resolvedReadTimeout);
    }

    if (!Utils.isEmpty(proxyHost)) {
      settings.setProxyScheme(resolve(proxyScheme));
      settings.setProxyHost(resolve(proxyHost));
      int resolvedProxyPort = Const.toInt(resolve(proxyPort), -1);
      if (resolvedProxyPort > 0) {
        settings.setProxyPort(resolvedProxyPort);
      }
      settings.setProxyUsername(resolve(proxyUsername));
      settings.setProxyPassword(Encr.decryptPasswordOptionallyEncrypted(resolve(proxyPassword)));
      settings.setNonProxyHosts(resolve(nonProxyHosts));
    }

    // Configure SSL if needed (client cert, trust store, or ignore SSL)
    if (needsSslConfiguration()) {
      try {
        settings.setSslContext(buildSslContext());
        getLog()
            .logDetailed(
                "SSL context built. ignoreSsl="
                    + ignoreSsl
                    + ", trustStoreFile="
                    + Const.NVL(trustStoreFile, "<empty>")
                    + ", keyStoreFile="
                    + Const.NVL(keyStoreFile, "<empty>"));

        // Set hostname verifier if ignoring SSL or using custom truststore
        if (ignoreSsl || !Utils.isEmpty(trustStoreFile)) {
          getLog().logDetailed("Enabling permissive hostname verifier.");
          settings.setPermissiveHostnameVerifier(true);
        }
      } catch (Exception e) {
        throw new HopException("Error configuring SSL for REST connection", e);
      }
    }

    // The credentials are bound to this connection's own base URL: a REST transform taking its URL
    // from an input field must not hand them to whatever host a row happens to name.
    settings.setAuthOrigin(resolve(baseUrl));
    if (isBasicAuthConfigured()) {
      settings.setAuthType(RestAuthType.BASIC);
      settings.setBasicUsername(resolve(username));
      settings.setBasicPassword(Encr.decryptPasswordOptionallyEncrypted(resolve(password)));
      settings.setBasicPreemptive(isPreemptiveBasicAuth());
    } else if (authTypeEquals(BEARER)) {
      settings.setAuthType(RestAuthType.BEARER);
      settings.setBearerToken(Encr.decryptPasswordOptionallyEncrypted(resolve(bearerToken)));
    } else if (authTypeEquals(API_KEY)) {
      settings.setAuthType(RestAuthType.API_KEY);
      settings.setApiKeyHeaderName(resolve(authorizationHeaderName));
      settings.setApiKeyHeaderPrefix(resolve(authorizationPrefix));
      settings.setApiKeyHeaderValue(
          Encr.decryptPasswordOptionallyEncrypted(resolve(authorizationHeaderValue)));
    }
    return settings;
  }

  /**
   * Applies this connection's authentication to the headers of one request.
   *
   * <p>The credentials are scoped to {@code url} rather than to the connection's base URL: this is
   * a one-shot call against an explicitly named URL, so asking for it is consent to authenticate
   * against it. Origin scoping exists for the transform's per-row path, where one client serves a
   * URL that changes underneath it.
   */
  public void applyAuthentication(Map<String, String> headers, String url) throws HopException {
    RestClientSettings settings = createClientSettings();
    settings.setAuthOrigin(url);
    new RestAuthenticator(settings).applyRequestHeaders(headers, url);
  }

  /** Backwards compatibility with early versions of this metadata type. */
  private void normalizeAuthType() {
    if (StringUtils.isEmpty(authType)) {
      if (!StringUtils.isEmpty(authorizationHeaderName)
          && !StringUtils.isEmpty(authorizationHeaderValue)) {
        authType = API_KEY;
      } else {
        authType = "No Auth";
      }
    }
  }

  private boolean isBasicAuthConfigured() {
    return authTypeEquals(BASIC)
        && !StringUtils.isEmpty(username)
        && !StringUtils.isEmpty(password);
  }

  /** Whether Basic credentials go out on the first request. See {@link #nonPreemptiveBasicAuth}. */
  public boolean isPreemptiveBasicAuth() {
    return !nonPreemptiveBasicAuth;
  }

  public void setPreemptiveBasicAuth(boolean preemptiveBasicAuth) {
    this.nonPreemptiveBasicAuth = !preemptiveBasicAuth;
  }

  private boolean authTypeEquals(String canonicalLabel) {
    return authType != null
        && canonicalLabel != null
        && authType.trim().equalsIgnoreCase(canonicalLabel.trim());
  }

  /** Performs a GET against the URL with this connection's authentication, returning the body. */
  public String getResponse(String url) throws HopException {
    RestClientSettings settings = createClientSettings();
    Map<String, String> headers = new LinkedHashMap<>();
    applyAuthentication(headers, url);

    HttpUriRequestBase request = new HttpUriRequestBase("GET", URI.create(url));
    headers.forEach(request::addHeader);

    try (CloseableHttpClient httpClient = RestClientFactory.createClient(settings)) {
      return httpClient.execute(
          request,
          response -> {
            if (response.getCode() != HttpStatus.SC_OK) {
              throw new IOException("Error connecting to " + url + ": " + response.getCode());
            }
            HttpEntity entity = response.getEntity();
            return entity == null ? "" : EntityUtils.toString(entity);
          });
    } catch (Exception e) {
      throw new HopException("Error connecting to " + url, e);
    }
  }

  /**
   * Verifies connectivity using {@link #testUrl}. It goes through the same authenticator as a real
   * request, so a green test cannot mean anything other than that the credentials work.
   */
  public void testConnection() throws HopException {
    getResponse(resolve(testUrl));
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
    // "TLS" negotiates the best protocol the JVM and the server agree on. It used to ask for "SSL"
    // here, which on a modern JVM resolves to the same TLS implementation but reads as a request
    // for a protocol family that has been insecure for a decade.
    javax.net.ssl.SSLContext sslContext = javax.net.ssl.SSLContext.getInstance("TLS");

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
