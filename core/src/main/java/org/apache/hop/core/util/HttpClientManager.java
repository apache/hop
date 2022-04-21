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

import org.apache.http.HttpHost;
import org.apache.http.auth.AuthScope;
import org.apache.http.auth.UsernamePasswordCredentials;
import org.apache.http.client.CredentialsProvider;
import org.apache.http.client.RedirectStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.config.Registry;
import org.apache.http.config.RegistryBuilder;
import org.apache.http.conn.socket.ConnectionSocketFactory;
import org.apache.http.conn.socket.PlainConnectionSocketFactory;
import org.apache.http.conn.ssl.NoopHostnameVerifier;
import org.apache.http.conn.ssl.SSLConnectionSocketFactory;
import org.apache.http.impl.client.BasicCredentialsProvider;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.BasicHttpClientConnectionManager;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.ssl.SSLContexts;
import org.apache.http.ssl.TrustStrategy;

import javax.net.ssl.SSLContext;
import java.security.KeyManagementException;
import java.security.KeyStoreException;
import java.security.NoSuchAlgorithmException;

/**
 * Single entry point for all {@link org.apache.http.client.HttpClient HttpClient instances} usages
 * in Hop projects. Contains {@link org.apache.http.impl.conn.PoolingHttpClientConnectionManager
 * Connection pool} of 200 connections. Maximum connections per one route is 100. Provides inner
 * builder class for creating {@link org.apache.http.client.HttpClient HttpClients}.
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
    private CredentialsProvider provider;
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
      CredentialsProvider provider = new BasicCredentialsProvider();
      UsernamePasswordCredentials credentials = new UsernamePasswordCredentials(user, password);
      provider.setCredentials(authScope, credentials);
      this.provider = provider;
      return this;
    }

    public HttpClientBuilderFacade setCredentials(String user, String password) {
      return setCredentials(user, password, AuthScope.ANY);
    }

    public HttpClientBuilderFacade setProxy(String proxyHost, int proxyPort) {
      setProxy(proxyHost, proxyPort, "http");
      return this;
    }

    public HttpClientBuilderFacade setProxy(String proxyHost, int proxyPort, String scheme) {
      this.proxy = new HttpHost(proxyHost, proxyPort, scheme);
      return this;
    }

    public HttpClientBuilderFacade setRedirect(RedirectStrategy redirectStrategy) {
      this.redirectStrategy = redirectStrategy;
      return this;
    }

    public void ignoreSsl(boolean ignoreSsl) {
      this.ignoreSsl = ignoreSsl;
    }

    public void ignoreSsl(HttpClientBuilder httpClientBuilder){
      TrustStrategy acceptingTrustStrategy = (cert, authType) -> true;
      SSLContext sslContext;
      try {
        sslContext = SSLContexts.custom().loadTrustMaterial(null, acceptingTrustStrategy).build();
      } catch (NoSuchAlgorithmException | KeyManagementException | KeyStoreException e) {
        throw new RuntimeException(e);
      }

      SSLConnectionSocketFactory sslsf = new SSLConnectionSocketFactory(sslContext,
          NoopHostnameVerifier.INSTANCE);

      Registry<ConnectionSocketFactory> socketFactoryRegistry =
          RegistryBuilder.<ConnectionSocketFactory>create()
              .register("https", sslsf)
              .register("http", new PlainConnectionSocketFactory())
              .build();

      BasicHttpClientConnectionManager connectionManager =
          new BasicHttpClientConnectionManager(socketFactoryRegistry);

      httpClientBuilder.setSSLSocketFactory(sslsf).setConnectionManager(connectionManager);
    }

    public CloseableHttpClient build() {
      HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
      httpClientBuilder.setConnectionManager(manager);

      RequestConfig.Builder requestConfigBuilder = RequestConfig.custom();
      if (socketTimeout > 0) {
        requestConfigBuilder.setSocketTimeout(socketTimeout);
      }
      if (connectionTimeout > 0) {
        requestConfigBuilder.setConnectTimeout(socketTimeout);
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
}
