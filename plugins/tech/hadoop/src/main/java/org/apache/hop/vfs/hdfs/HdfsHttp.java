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
package org.apache.hop.vfs.hdfs;

import java.util.List;
import java.util.concurrent.ExecutorService;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.util.Timeout;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.vfs.hdfs.client.HdfsWebHdfsClient;
import org.apache.hop.vfs.hdfs.kerberos.HdfsKerberosSession;
import org.apache.hop.vfs.hdfs.metadata.HdfsMeta;

/** Shared HTTP client construction for the VFS provider and the metadata Test buttons. */
public final class HdfsHttp {
  private static final Class<?> PKG = HdfsTransport.class;

  private HdfsHttp() {}

  public static CloseableHttpClient createClient(IVariables variables, HdfsMeta meta)
      throws FileSystemException {
    HdfsTransport transport =
        meta.getTransport() == null ? HdfsTransport.HttpFS : meta.getTransport();
    boolean https = meta.isHttps() || transport.defaultHttps();
    HttpClientBuilder builder = HttpClientBuilder.create();
    builder.setDefaultRequestConfig(
        RequestConfig.custom()
            .setConnectTimeout(Timeout.ofSeconds(30))
            .setResponseTimeout(Timeout.ofSeconds(300))
            .build());
    try {
      if (https) {
        SSLContext sslContext = HdfsTls.sslContext(variables, meta);
        HostnameVerifier verifier =
            meta.isHostnameVerification() ? null : NoopHostnameVerifier.INSTANCE;
        DefaultClientTlsStrategy tls =
            verifier == null
                ? new DefaultClientTlsStrategy(sslContext)
                : new DefaultClientTlsStrategy(sslContext, verifier);
        builder.setConnectionManager(
            PoolingHttpClientConnectionManagerBuilder.create().setTlsSocketStrategy(tls).build());
      }
      return builder.build();
    } catch (Exception e) {
      throw new FileSystemException("Unable to create HTTP client for HDFS VFS", e);
    }
  }

  public static HdfsWebHdfsClient createWebHdfsClient(
      IVariables variables, HdfsMeta meta, CloseableHttpClient httpClient, ExecutorService executor)
      throws FileSystemException {
    return createWebHdfsClient(variables, meta, httpClient, executor, null);
  }

  public static HdfsWebHdfsClient createWebHdfsClient(
      IVariables variables,
      HdfsMeta meta,
      CloseableHttpClient httpClient,
      ExecutorService executor,
      HdfsKerberosSession kerberosSession)
      throws FileSystemException {
    HdfsTransport transport =
        meta.getTransport() == null ? HdfsTransport.HttpFS : meta.getTransport();
    String host = variables.resolve(Const.NVL(meta.getEndpointHostname(), ""));
    if (StringUtils.isEmpty(host)) {
      logMissingHost(meta);
    }
    int port = Const.toInt(variables.resolve(meta.getEndpointPort()), transport.defaultPort());
    boolean https = meta.isHttps() || transport.defaultHttps();
    String httpScheme = https ? "https" : "http";
    String basePath = variables.resolve(Const.NVL(meta.getBasePath(), ""));
    String simpleUser = variables.resolve(Const.NVL(meta.getSimpleUser(), "hop"));
    List<String> endpoints =
        HdfsWebHdfsClient.endpointList(
            host, port, variables.resolve(Const.NVL(meta.getHaNamenodes(), "")));
    HdfsKerberosSession session = kerberosSession;
    if (meta.isKerberosEnabled() && session == null) {
      session = new HdfsKerberosSession(variables, meta);
    }
    return new HdfsWebHdfsClient(
        httpClient,
        transport,
        endpoints,
        httpScheme,
        basePath,
        simpleUser,
        meta.isKerberosEnabled(),
        session,
        executor);
  }

  static void logMissingHost(HdfsMeta meta) {
    LogChannel.GENERAL.logError(
        BaseMessages.getString(PKG, "Hdfs.Error.MissingHost", Const.NVL(meta.getName(), "")));
  }
}
