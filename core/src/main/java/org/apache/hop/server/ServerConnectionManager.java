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

package org.apache.hop.server;

import org.apache.http.client.HttpClient;
import org.apache.http.impl.client.HttpClients;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;

import javax.net.ssl.KeyManager;
import javax.net.ssl.SSLContext;
import javax.net.ssl.X509TrustManager;
import java.security.SecureRandom;
import java.security.cert.CertificateException;
import java.security.cert.X509Certificate;

/**
 * Encapsulates the Apache commons HTTP connection manager with a singleton. We can use this to
 * limit the number of open connections to hop servers.
 */
public class ServerConnectionManager {

  private static final String SSL = "SSL";
  private static final String KEYSTORE_SYSTEM_PROPERTY = "javax.net.ssl.keyStore";

  private static ServerConnectionManager serverConnectionManager;

  private PoolingHttpClientConnectionManager manager;

  private ServerConnectionManager() {
    if (needToInitializeSSLContext()) {
      try {
        SSLContext context = SSLContext.getInstance(SSL);
        context.init(
            new KeyManager[0],
            new X509TrustManager[] {getDefaultTrustManager()},
            new SecureRandom());
        SSLContext.setDefault(context);
      } catch (Exception ignored) {
      }
    }
    manager = new PoolingHttpClientConnectionManager();
    manager.setDefaultMaxPerRoute(100);
    manager.setMaxTotal(200);
  }

  private static boolean needToInitializeSSLContext() {
    return System.getProperty(KEYSTORE_SYSTEM_PROPERTY) == null;
  }

  public static ServerConnectionManager getInstance() {
    if (serverConnectionManager == null) {
      serverConnectionManager = new ServerConnectionManager();
    }
    return serverConnectionManager;
  }

  public HttpClient createHttpClient() {
    return HttpClients.custom().setConnectionManager(manager).build();
  }

  public void shutdown() {
    manager.shutdown();
  }

  private static X509TrustManager getDefaultTrustManager() {
    return new X509TrustManager() {
      @Override
      public void checkClientTrusted(X509Certificate[] certs, String param)
          throws CertificateException {}

      @Override
      public void checkServerTrusted(X509Certificate[] certs, String param)
          throws CertificateException {
        for (X509Certificate cert : certs) {
          cert.checkValidity(); // validate date
        }
      }

      @Override
      public X509Certificate[] getAcceptedIssuers() {
        return null;
      }
    };
  }

  static void reset() {
    serverConnectionManager = null;
  }
}
