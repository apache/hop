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

package org.apache.hop.metadata.rest.client;

import org.apache.hc.client5.http.auth.CredentialsProvider;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.ClientTlsStrategyBuilder;
import org.apache.hc.client5.http.ssl.DefaultHostnameVerifier;
import org.apache.hc.client5.http.ssl.HostnameVerificationPolicy;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.util.Timeout;
import org.apache.hop.core.util.Utils;

/**
 * Builds HTTP clients from {@link RestClientSettings}. The only place in the REST plugins where a
 * client is created.
 *
 * <p>A client is bound to a configuration, not to a URL, so one client serves any number of targets
 * — including the dynamic per-row URLs of the REST transform. Callers are expected to build one per
 * transform copy and close it when they are done, rather than one per request.
 */
@SuppressWarnings("java:S5527") // The permissive host name verifier is an explicit user choice.
public final class RestClientFactory {

  private RestClientFactory() {
    // Utility class
  }

  /**
   * Builds the client described by the settings. Close it when finished: it owns a connection pool.
   */
  public static CloseableHttpClient createClient(RestClientSettings settings) {
    HttpClientBuilder builder =
        HttpClients.custom().setConnectionManager(createConnectionManager(settings));

    RequestConfig.Builder requestConfig = RequestConfig.custom();
    if (settings.getReadTimeout() != null) {
      requestConfig.setResponseTimeout(Timeout.ofMilliseconds(settings.getReadTimeout()));
    }
    builder.setDefaultRequestConfig(requestConfig.build());

    if (!Utils.isEmpty(settings.getProxyHost())) {
      // A route planner rather than RequestConfig.setProxy(): the decision whether to use the
      // proxy belongs to the target, so a bypass list can only be honoured per request.
      builder.setRoutePlanner(new RestProxyRoutePlanner(settings));
    }

    // Challenge-response Basic authentication and proxy authentication are answered from a
    // credentials provider scoped to the host they belong to. Preemptive Basic, Bearer and API-key
    // auth are request headers instead, applied by RestAuthenticator per request.
    CredentialsProvider credentialsProvider =
        new RestAuthenticator(settings).createCredentialsProvider();
    if (credentialsProvider != null) {
      builder.setDefaultCredentialsProvider(credentialsProvider);
    }

    return builder.build();
  }

  /**
   * The connection pool for one client, carrying the connect timeout and the TLS configuration.
   * Deliberately not the process-wide pool in {@code HttpClientManager}: closing a client closes
   * the manager it was given, so a shared one would be torn down by the first transform to finish.
   */
  private static org.apache.hc.client5.http.io.HttpClientConnectionManager createConnectionManager(
      RestClientSettings settings) {
    PoolingHttpClientConnectionManagerBuilder connectionManager =
        PoolingHttpClientConnectionManagerBuilder.create();

    if (settings.getConnectTimeout() != null) {
      connectionManager.setDefaultConnectionConfig(
          ConnectionConfig.custom()
              .setConnectTimeout(Timeout.ofMilliseconds(settings.getConnectTimeout()))
              .build());
    }

    ClientTlsStrategyBuilder tls = ClientTlsStrategyBuilder.create();
    if (settings.getSslContext() != null) {
      tls.setSslContext(settings.getSslContext());
    }

    // Host names are checked by HttpClient, not by the JDK's built-in endpoint identification.
    // The built-in check runs inside the handshake and converts the host through
    // java.net.IDN.toASCII, which rejects anything that is not LDH ASCII: an underscore fails with
    // "Illegal given domain name" before the certificate is even looked at. It also aborts the
    // handshake before any HostnameVerifier is consulted, so "ignore SSL" could not ignore a name
    // mismatch. HttpClient's own verifier compares against the certificate's SANs directly and has
    // neither limitation. Certificate chain validation is unaffected — that is the trust manager's
    // job, not the endpoint identification algorithm's.
    tls.setHostVerificationPolicy(HostnameVerificationPolicy.CLIENT);
    // CLIENT policy performs no check at all when the verifier is null, so one is always supplied.
    tls.setHostnameVerifier(
        settings.isPermissiveHostnameVerifier()
            ? NoopHostnameVerifier.INSTANCE
            : new DefaultHostnameVerifier());

    connectionManager.setTlsSocketStrategy(tls.buildClassic());

    return connectionManager.build();
  }
}
