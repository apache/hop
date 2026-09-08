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

import java.io.InputStream;
import java.security.KeyStore;
import java.util.Collection;
import java.util.List;
import javax.net.ssl.HostnameVerifier;
import javax.net.ssl.SSLContext;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.vfs2.Capability;
import org.apache.commons.vfs2.FileName;
import org.apache.commons.vfs2.FileSystem;
import org.apache.commons.vfs2.FileSystemException;
import org.apache.commons.vfs2.FileSystemOptions;
import org.apache.commons.vfs2.provider.AbstractOriginatingFileProvider;
import org.apache.hc.client5.http.config.RequestConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClientBuilder;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.client5.http.ssl.DefaultClientTlsStrategy;
import org.apache.hc.client5.http.ssl.NoopHostnameVerifier;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hc.core5.util.Timeout;
import org.apache.hop.core.Const;
import org.apache.hop.core.logging.LogChannel;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.vfs.hdfs.client.HdfsWebHdfsClient;
import org.apache.hop.vfs.hdfs.kerberos.HdfsKerberosSession;
import org.apache.hop.vfs.hdfs.metadata.HdfsMeta;

public class HdfsFileProvider extends AbstractOriginatingFileProvider {
  private static final Class<?> PKG = HdfsTransport.class;
  private static final FileSystemOptions DEFAULT_OPTIONS = new FileSystemOptions();

  private final IVariables variables;
  private final HdfsMeta meta;

  public HdfsFileProvider() {
    this(null, null);
  }

  public HdfsFileProvider(IVariables variables, HdfsMeta meta) {
    super();
    this.variables = variables;
    this.meta = meta;
    setFileNameParser(HdfsFileNameParser.getInstance());
  }

  @Override
  protected FileSystem doCreateFileSystem(FileName name, FileSystemOptions fileSystemOptions)
      throws FileSystemException {
    FileSystemOptions options = fileSystemOptions != null ? fileSystemOptions : DEFAULT_OPTIONS;
    HdfsFileSystem fileSystem = new HdfsFileSystem(name, options);
    if (meta == null || variables == null) {
      return fileSystem;
    }

    HdfsTransport transport =
        meta.getTransport() == null ? HdfsTransport.HttpFS : meta.getTransport();
    String host = variables.resolve(Const.NVL(meta.getEndpointHostname(), ""));
    if (StringUtils.isEmpty(host)) {
      LogChannel.GENERAL.logError(
          BaseMessages.getString(PKG, "Hdfs.Error.MissingHost", Const.NVL(meta.getName(), "")));
    }
    int port = Const.toInt(variables.resolve(meta.getEndpointPort()), transport.defaultPort());
    boolean https = meta.isHttps() || transport.defaultHttps();
    String httpScheme = https ? "https" : "http";
    String basePath = variables.resolve(Const.NVL(meta.getBasePath(), ""));
    String defaultRoot = variables.resolve(Const.NVL(meta.getDefaultRoot(), ""));
    String simpleUser = variables.resolve(Const.NVL(meta.getSimpleUser(), "hop"));
    List<String> endpoints =
        HdfsWebHdfsClient.endpointList(
            host, port, variables.resolve(Const.NVL(meta.getHaNamenodes(), "")));

    HdfsKerberosSession kerberosSession = null;
    if (meta.isKerberosEnabled()) {
      kerberosSession = new HdfsKerberosSession(variables, meta);
    }

    CloseableHttpClient httpClient = buildHttpClient(https, meta);
    HdfsWebHdfsClient client =
        new HdfsWebHdfsClient(
            httpClient,
            transport,
            endpoints,
            httpScheme,
            basePath,
            simpleUser,
            meta.isKerberosEnabled(),
            kerberosSession,
            fileSystem.getExecutor());
    fileSystem.setClient(client);
    fileSystem.setDefaultRoot(defaultRoot);
    return fileSystem;
  }

  private CloseableHttpClient buildHttpClient(boolean https, HdfsMeta hdfsMeta)
      throws FileSystemException {
    HttpClientBuilder builder = HttpClientBuilder.create();
    builder.setDefaultRequestConfig(
        RequestConfig.custom()
            .setConnectTimeout(Timeout.ofSeconds(30))
            .setResponseTimeout(Timeout.ofSeconds(300))
            .build());
    try {
      if (https) {
        SSLContext sslContext = sslContext(hdfsMeta);
        HostnameVerifier verifier =
            hdfsMeta.isHostnameVerification() ? null : NoopHostnameVerifier.INSTANCE;
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

  private SSLContext sslContext(HdfsMeta hdfsMeta) throws Exception {
    String truststore = variables.resolve(Const.NVL(hdfsMeta.getTruststorePath(), ""));
    if (truststore.isEmpty()) {
      return SSLContexts.createDefault();
    }
    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    char[] password =
        Const.NVL(variables.resolve(hdfsMeta.getTruststorePassword()), "").toCharArray();
    try (InputStream in = HopVfs.getInputStream(truststore, variables)) {
      keyStore.load(in, password);
    }
    return SSLContexts.custom().loadTrustMaterial(keyStore, null).build();
  }

  @Override
  public Collection<Capability> getCapabilities() {
    return HdfsFileSystem.CAPABILITIES;
  }
}
