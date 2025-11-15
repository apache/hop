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
import org.apache.commons.lang.StringUtils;
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
  @HopMetadataProperty(key = "bearer_token")
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

  public RestConnection(IVariables variables) {
    this.variables = variables;
  }

  public Invocation.Builder getInvocationBuilder(String url) throws HopException {

    builder = ClientBuilder.newBuilder();

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
                resolve(username), Encr.decryptPasswordOptionallyEncrypted(resolve(password))));
        target = client.target(url);
        invocationBuilder = target.request();
      }
    } else if (authType.equals("API Key")) {
      if (!StringUtils.isEmpty(resolve(authorizationPrefix))) {
        invocationBuilder.header(
            resolve(authorizationHeaderName),
            resolve(authorizationPrefix)
                + " "
                + Encr.decryptPasswordOptionallyEncrypted(resolve(authorizationHeaderValue)));
      } else {
        invocationBuilder.header(
            resolve(authorizationHeaderName),
            Encr.decryptPasswordOptionallyEncrypted(resolve(authorizationHeaderValue)));
      }
    } else if (authType.equals("Bearer")) {
      if (!StringUtils.isEmpty(bearerToken)) {
        invocationBuilder.header(HttpHeaders.AUTHORIZATION, "Bearer " + resolve(bearerToken));
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
    Response response = getInvocationBuilder(resolve(testUrl)).get();
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
          public X509Certificate[] getAcceptedIssuers() {
            return new X509Certificate[0];
          }

          public void checkClientTrusted(X509Certificate[] certs, String authType) {
            // Trust all - do nothing
          }

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
      return null; // Use default system trust store
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
