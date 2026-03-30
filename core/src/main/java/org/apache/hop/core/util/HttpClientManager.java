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

package org.apache.hop.core.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.security.KeyManagementException;
import java.security.KeyStore;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import javax.net.ssl.TrustManager;
import javax.net.ssl.TrustManagerFactory;
import javax.net.ssl.X509TrustManager;
import org.apache.hc.client5.http.auth.AuthScope;
import org.apache.hc.client5.http.auth.UsernamePasswordCredentials;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.auth.BasicCredentialsProvider;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.BasicHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.protocol.RedirectStrategy;
import org.apache.hc.client5.http.socket.ConnectionSocketFactory;
import org.apache.hc.client5.http.socket.PlainConnectionSocketFactory;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.client5.http.ssl.SSLConnectionSocketFactory;
import org.apache.hc.core5.http.HttpHost;
import org.apache.hc.core5.http.config.Registry;
import org.apache.hc.core5.http.config.RegistryBuilder;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.ssl.TrustStrategy;
import org.apache.hc.core5.util.Timeout;
import org.apache.hop.core.logging.ILogChannel;

/**
 * Single entry point for all {@link org.apache.hc.client5.http.classic.HttpClient HttpClient
 * instances} usages in Hop projects. Contains {@link
 * org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager Connection pool} of 200
 * connections. Maximum connections per one route is 100. Provides inner builder class for creating
 * {@link org.apache.hc.client5.http.classic.HttpClient HttpClients}.
 */
public class HttpClientManager {
  private static final int CONNECTIONS_PER_ROUTE = 100;
  private static final int TOTAL_CONNECTIONS = 200;

  private static HttpClientManager httpClientManager;
  private static PoolingHttpClientConnectionManager manager;

  private HttpClientManager() {
    manager = new PoolingHttpClientConnectionManager();
    manager.setDefaultMaxPerRoute(CONNECTIONS_PER_ROUTE);
    manager.setMaxTotal(TOTAL_CONNECTIONS);
  }

  public static HttpClientManager getInstance() {
    if (httpClientManager == null) {
      httpClientManager = new HttpClientManager();
    }
    return httpClientManager;
  }

  public CloseableHttpClient createDefaultClient() {
    return HttpClients.custom().setConnectionManager(manager).build();
  }

  public HttpClientBuilderFacade createBuilder() {
    return new HttpClientBuilderFacade();
  }

  public class HttpClientBuilderFacade {
    private RedirectStrategy redirectStrategy;
    private BasicCredentialsProvider provider;
    private int connectionTimeout;
    private int socketTimeout;
    private HttpHost proxy;
    private boolean ignoreSsl;

    public HttpClientBuilderFacade setConnectionTimeout(int connectionTimeout) {
      this.connectionTimeout = connectionTimeout;
      return this;
    }

    public HttpClientBuilderFacade setSocketTimeout(int socketTimeout) {
      this.socketTimeout = socketTimeout;
      return this;
    }

    public HttpClientBuilderFacade setCredentials(
        String user, String password, AuthScope authScope) {
      BasicCredentialsProvider provider = new BasicCredentialsProvider();
      char[] passwordChars = password != null ? password.toCharArray() : new char[0];
      UsernamePasswordCredentials credentials =
          new UsernamePasswordCredentials(user, passwordChars);
      provider.setCredentials(authScope, credentials);
      this.provider = provider;
      return this;
    }

    public HttpClientBuilderFacade setCredentials(String user, String password) {
      return setCredentials(user, password, new AuthScope(null, null, -1, null, null));
    }

    public HttpClientBuilderFacade setProxy(String proxyHost, int proxyPort) {
      setProxy(proxyHost, proxyPort, "http");
      return this;
    }

    public HttpClientBuilderFacade setProxy(String proxyHost, int proxyPort, String scheme) {
      this.proxy = new HttpHost(scheme, proxyHost, proxyPort);
      return this;
    }

    public HttpClientBuilderFacade setRedirect(RedirectStrategy redirectStrategy) {
      this.redirectStrategy = redirectStrategy;
      return this;
    }

    public void ignoreSsl(boolean ignoreSsl) {
      this.ignoreSsl = ignoreSsl;
    }

    public void ignoreSsl(HttpClientBuilder httpClientBuilder) {
      TrustStrategy acceptingTrustStrategy = (cert, authType) -> true;
      SSLContext sslContext;
      try {
        sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
      } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
        throw new RuntimeException(e);
      }

      SSLConnectionSocketFactory sslsf =
          new SSLConnectionSocketFactory(sslContext, NoopHostnameVerifier.INSTANCE);

      Registry<ConnectionSocketFactory> socketFactoryRegistry =
          RegistryBuilder.<ConnectionSocketFactory>create()
              .register("https", sslsf)
              .register("http", new PlainConnectionSocketFactory())
              .build();

      BasicHttpClientConnectionManager connectionManager =
          new BasicHttpClientConnectionManager(socketFactoryRegistry);

      httpClientBuilder.setConnectionManager(connectionManager);
    }

    public CloseableHttpClient build() {
      HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
      httpClientBuilder.setConnectionManager(manager);

      RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
      if (socketTimeout > 0) {
        requestConfigBuilder.setResponseTimeout(Timeout.ofMilliseconds(socketTimeout));
      }
      if (connectionTimeout > 0) {
        requestConfigBuilder.setConnectTimeout(Timeout.ofMilliseconds(connectionTimeout));
      }
      if (proxy != null) {
        requestConfigBuilder.setProxy(proxy);
      }
      httpClientBuilder.setDefaultRequestConfig(requestConfigBuilder.build());

      if (provider != null) {
        httpClientBuilder.setDefaultCredentialsProvider(provider);
      }
      if (redirectStrategy != null) {
        httpClientBuilder.setRedirectStrategy(redirectStrategy);
      }
      if (ignoreSsl) {
        ignoreSsl(httpClientBuilder);
      }

      return httpClientBuilder.build();
    }
  }

  public static SSLContext getSslContextWithTrustStoreFile(
      FileInputStream trustFileStream, String trustStorePassword)
      throws NoSuchAlgorithmException,
          KeyStoreException,
          IOException,
          CertificateException,
          KeyManagementException {
    TrustManagerFactory tmf =
        TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    // Using null here initialises the TMF with the default trust store.
    tmf.init((KeyStore) null);

    // Get hold of the default trust manager
    X509TrustManager defaultTm = null;
    for (TrustManager tm : tmf.getTrustManagers()) {
      if (tm instanceof X509TrustManager x509TrustManager) {
        defaultTm = x509TrustManager;
        break;
      }
    }

    // Load the trustStore which needs to be imported
    KeyStore trustStore = KeyStore.getInstance(KeyStore.getDefaultType());
    trustStore.load(trustFileStream, trustStorePassword.toCharArray());

    trustFileStream.close();

    tmf = TrustManagerFactory.getInstance(TrustManagerFactory.getDefaultAlgorithm());
    tmf.init(trustStore);

    // Get hold of the default trust manager
    X509TrustManager trustManager = null;
    for (TrustManager tm : tmf.getTrustManagers()) {
      if (tm instanceof X509TrustManager x509TrustManager) {
        trustManager = x509TrustManager;
        break;
      }
    }

    final X509TrustManager finalDefaultTm = defaultTm;
    final X509TrustManager finalTrustManager = trustManager;
    X509TrustManager customTm =
        new X509TrustManager() {
          @Override
          public X509Certificate[] getAcceptedIssuers() {
            return finalDefaultTm.getAcceptedIssuers();
          }

          @Override
          public void checkServerTrusted(X509Certificate[] chain, String authType)
              throws CertificateException {
            try {
              finalTrustManager.checkServerTrusted(chain, authType);
            } catch (CertificateException e) {
              finalDefaultTm.checkServerTrusted(chain, authType);
            }
          }

          @Override
          public void checkClientTrusted(X509Certificate[] chain, String authType)
              throws CertificateException {
            finalDefaultTm.checkClientTrusted(chain, authType);
          }
        };

    SSLContext sslContext = SSLContext.getInstance("TLSv1.2");
    sslContext.init(null, new TrustManager[] {customTm}, null);

    return sslContext;
  }

  /**
   * This SSLContext is used when a user chooses to TrustAll and ignore SSL and Certificate
   * validation
   *
   * @return SSLContext
   * @throws NoSuchAlgorithmException
   * @throws KeyManagementException
   */
  @SuppressWarnings({"java:S4830", "java:S4423"})
  public static SSLContext getTrustAllSslContext()
      throws NoSuchAlgorithmException, KeyManagementException {
    TrustManager[] trustAllCerts =
        new TrustManager[] {
          new X509TrustManager() {
            public java.security.cert.X509Certificate[] getAcceptedIssuers() {
              return null;
            }

            public void checkClientTrusted(X509Certificate[] certs, String authType) {
              // Do nothing
            }

            public void checkServerTrusted(X509Certificate[] certs, String authType) {
              // Do nothing
            }
          }
        };

    SSLContext sc = SSLContext.getInstance("SSL");
    sc.init(null, trustAllCerts, new java.security.SecureRandom());
    return sc;
  }

  public static HostnameVerifier getHostnameVerifier(boolean isDebug, ILogChannel log) {
    return (hostname, session) -> {
      if (isDebug) {
        log.logDebug("Warning: URL Host: " + hostname + " vs. " + session.getPeerHost());
      }
      return true;
    };
  }
}
