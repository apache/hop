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

import java.net.InetSocketAddress;
import java.util.Date;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import javax.net.ssl.SSLContext;
import javax.net.ssl.SSLParameters;
import javax.net.ssl.SSLSocket;
import javax.net.ssl.SSLSocketFactory;
import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;
import org.apache.hop.vfs.hdfs.client.HdfsFileStatus;
import org.apache.hop.vfs.hdfs.client.HdfsWebHdfsClient;
import org.apache.hop.vfs.hdfs.kerberos.HdfsKerberosSession;
import org.apache.hop.vfs.hdfs.metadata.HdfsMeta;

/** Probes the live editor model for the HDFS connection Test buttons. */
public final class HdfsConnectionTester {
  private static final Class<?> PKG = HdfsTransport.class;

  private HdfsConnectionTester() {}

  public static String testCluster(IVariables variables, HdfsMeta meta) throws Exception {
    String host = variables.resolve(Const.NVL(meta.getEndpointHostname(), ""));
    if (StringUtils.isBlank(host)) {
      throw new IllegalArgumentException(
          BaseMessages.getString(PKG, "Hdfs.Error.MissingHost", Const.NVL(meta.getName(), "")));
    }
    HdfsTransport transport =
        meta.getTransport() == null ? HdfsTransport.HttpFS : meta.getTransport();
    int port = Const.toInt(variables.resolve(meta.getEndpointPort()), transport.defaultPort());
    String path = variables.resolve(Const.NVL(meta.getDefaultRoot(), "/"));
    if (path.isBlank()) {
      path = "/";
    }
    ExecutorService executor = Executors.newCachedThreadPool();
    try (CloseableHttpClient http = HdfsHttp.createClient(variables, meta)) {
      HdfsWebHdfsClient client = HdfsHttp.createWebHdfsClient(variables, meta, http, executor);
      HdfsFileStatus status = client.getFileStatus(path);
      return BaseMessages.getString(
          PKG,
          "Hdfs.Test.Cluster.Success",
          transport.name(),
          host,
          Integer.toString(port),
          path,
          status.isDirectory() ? "DIRECTORY" : "FILE",
          Long.toString(status.getLength()));
    } finally {
      executor.shutdownNow();
    }
  }

  public static String testKerberos(IVariables variables, HdfsMeta meta) throws Exception {
    if (!meta.isKerberosEnabled()) {
      throw new IllegalArgumentException(
          BaseMessages.getString(PKG, "Hdfs.Test.Kerberos.NotEnabled"));
    }
    String principal = variables.resolve(Const.NVL(meta.getPrincipal(), ""));
    if (principal.isBlank()) {
      throw new IllegalArgumentException(
          BaseMessages.getString(PKG, "Hdfs.Test.Kerberos.MissingPrincipal"));
    }
    if (!meta.isUseTicketCache()
        && variables.resolve(Const.NVL(meta.getKeytabPath(), "")).isBlank()) {
      throw new IllegalArgumentException(
          BaseMessages.getString(PKG, "Hdfs.Test.Kerberos.MissingKeytab"));
    }
    HdfsKerberosSession session = new HdfsKerberosSession(variables, meta);
    session.login();
    Date end = session.ticketEndTime();
    String until = end == null ? "-" : end.toString();
    return BaseMessages.getString(PKG, "Hdfs.Test.Kerberos.Success", session.getPrincipal(), until);
  }

  public static String testTls(IVariables variables, HdfsMeta meta) throws Exception {
    HdfsTls.TrustMaterial trust = HdfsTls.load(variables, meta);
    String path = variables.resolve(Const.NVL(meta.getTruststorePath(), ""));
    String loaded =
        BaseMessages.getString(
            PKG,
            "Hdfs.Test.Tls.Loaded",
            trust.kind(),
            Integer.toString(trust.certificateCount()),
            path.isBlank() ? "-" : path);
    HdfsTransport transport =
        meta.getTransport() == null ? HdfsTransport.HttpFS : meta.getTransport();
    boolean https = meta.isHttps() || transport.defaultHttps();
    String host = variables.resolve(Const.NVL(meta.getEndpointHostname(), ""));
    if (!https || host.isBlank()) {
      return loaded;
    }
    int port = Const.toInt(variables.resolve(meta.getEndpointPort()), transport.defaultPort());
    handshake(trust.sslContext(), host, port, meta.isHostnameVerification());
    return loaded
        + Const.CR
        + BaseMessages.getString(PKG, "Hdfs.Test.Tls.Handshake", host, Integer.toString(port));
  }

  static void handshake(SSLContext sslContext, String host, int port, boolean verifyHostname)
      throws Exception {
    SSLSocketFactory factory = sslContext.getSocketFactory();
    try (SSLSocket socket = (SSLSocket) factory.createSocket()) {
      socket.connect(new InetSocketAddress(host, port), 10_000);
      if (verifyHostname) {
        SSLParameters parameters = socket.getSSLParameters();
        parameters.setEndpointIdentificationAlgorithm("HTTPS");
        socket.setSSLParameters(parameters);
      }
      socket.startHandshake();
    }
  }
}
