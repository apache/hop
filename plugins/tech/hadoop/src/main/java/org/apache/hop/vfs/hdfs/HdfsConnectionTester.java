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

import java.io.IOException;
import java.net.InetSocketAddress;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
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
import org.apache.hop.vfs.hdfs.client.HdfsSpnego;
import org.apache.hop.vfs.hdfs.client.HdfsWebHdfsClient;
import org.apache.hop.vfs.hdfs.kerberos.HdfsKerberosSession;
import org.apache.hop.vfs.hdfs.metadata.HdfsMeta;

/** Probes the live editor model for the HDFS connection Test buttons. */
public final class HdfsConnectionTester {
  private static final Class<?> PKG = HdfsTransport.class;

  private HdfsConnectionTester() {}

  public static String testCluster(IVariables variables, HdfsMeta meta) throws Exception {
    List<String> succeeded = new ArrayList<>();
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
    boolean https = meta.isHttps() || transport.defaultHttps();

    if (https) {
      try {
        HdfsTls.TrustMaterial trust = HdfsTls.load(variables, meta);
        handshake(trust.sslContext(), host, port, meta.isHostnameVerification());
        succeeded.add(
            BaseMessages.getString(
                PKG,
                "Hdfs.Test.Ok.Tls",
                host,
                Integer.toString(port),
                trust.kind(),
                Integer.toString(trust.certificateCount())));
      } catch (Exception e) {
        throw failed(succeeded, "TLS", host + ":" + port, e);
      }
    } else {
      succeeded.add(BaseMessages.getString(PKG, "Hdfs.Test.Ok.Http"));
    }

    HdfsKerberosSession session = null;
    if (meta.isKerberosEnabled()) {
      try {
        session = loginSession(variables, meta);
        succeeded.add(kerberosOk(session));
      } catch (Exception e) {
        throw failed(succeeded, "Kerberos", host, e);
      }
      try {
        session.doAs(() -> HdfsSpnego.authorizationHeader(host));
        succeeded.add(BaseMessages.getString(PKG, "Hdfs.Test.Ok.Spnego", host));
      } catch (Exception e) {
        throw failed(succeeded, "SPNEGO", "HTTP@" + host, e);
      }
    } else {
      succeeded.add(
          BaseMessages.getString(
              PKG,
              "Hdfs.Test.Ok.Simple",
              variables.resolve(Const.NVL(meta.getSimpleUser(), "hop"))));
    }

    ExecutorService executor = Executors.newCachedThreadPool();
    try (CloseableHttpClient http = HdfsHttp.createClient(variables, meta)) {
      HdfsWebHdfsClient client =
          HdfsHttp.createWebHdfsClient(variables, meta, http, executor, session);
      HdfsFileStatus status;
      try {
        status = client.getFileStatus(path);
      } catch (Exception e) {
        throw failed(succeeded, "GETFILESTATUS", path, e);
      }
      succeeded.add(
          BaseMessages.getString(
              PKG,
              "Hdfs.Test.Ok.Status",
              path,
              status.isDirectory() ? "DIRECTORY" : "FILE",
              Long.toString(status.getLength())));
    } finally {
      executor.shutdownNow();
    }

    StringBuilder report = new StringBuilder();
    report
        .append(
            BaseMessages.getString(
                PKG,
                "Hdfs.Test.Cluster.Success",
                transport.name(),
                host,
                Integer.toString(port),
                path))
        .append(Const.CR);
    appendSucceeded(report, succeeded);
    return report.toString().trim();
  }

  public static String testKerberos(IVariables variables, HdfsMeta meta) throws Exception {
    HdfsKerberosSession session = loginSession(variables, meta);
    return BaseMessages.getString(
        PKG, "Hdfs.Test.Kerberos.Success", session.getPrincipal(), ticketUntil(session));
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

  private static HdfsKerberosSession loginSession(IVariables variables, HdfsMeta meta)
      throws Exception {
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
    return session;
  }

  private static String kerberosOk(HdfsKerberosSession session) {
    return BaseMessages.getString(
        PKG, "Hdfs.Test.Ok.Kerberos", session.getPrincipal(), ticketUntil(session));
  }

  private static String ticketUntil(HdfsKerberosSession session) {
    Date end = session.ticketEndTime();
    return end == null ? "-" : end.toString();
  }

  static IOException failed(List<String> succeeded, String step, String detail, Throwable cause) {
    StringBuilder report = new StringBuilder();
    appendSucceeded(report, succeeded);
    String failedStep = StringUtils.isBlank(detail) ? step : step + " " + detail;
    report.append(BaseMessages.getString(PKG, "Hdfs.Test.Report.Failed", failedStep));
    if (cause != null && StringUtils.isNotBlank(cause.getMessage())) {
      report.append(Const.CR).append(cause.getMessage());
    }
    String hint = hintFor(step, detail);
    if (hint != null) {
      report.append(Const.CR).append(Const.CR).append(hint);
    }
    return new IOException(report.toString().trim(), cause);
  }

  private static void appendSucceeded(StringBuilder report, List<String> succeeded) {
    if (succeeded.isEmpty()) {
      return;
    }
    report.append(BaseMessages.getString(PKG, "Hdfs.Test.Report.Succeeded")).append(Const.CR);
    for (String line : succeeded) {
      report.append("- ").append(line).append(Const.CR);
    }
  }

  private static String hintFor(String step, String detail) {
    return switch (step) {
      case "TLS" -> BaseMessages.getString(PKG, "Hdfs.Test.Hint.Tls", Const.NVL(detail, ""));
      case "Kerberos" -> BaseMessages.getString(PKG, "Hdfs.Test.Hint.Kerberos");
      case "SPNEGO" ->
          BaseMessages.getString(PKG, "Hdfs.Test.Hint.Spnego", Const.NVL(detail, "HTTP@host"));
      case "GETFILESTATUS" -> BaseMessages.getString(PKG, "Hdfs.Test.Hint.Get");
      default -> null;
    };
  }
}
