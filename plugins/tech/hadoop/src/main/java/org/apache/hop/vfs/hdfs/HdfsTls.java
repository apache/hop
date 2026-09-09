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

import java.io.ByteArrayInputStream;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import javax.net.ssl.SSLContext;
import org.apache.hc.core5.ssl.SSLContexts;
import org.apache.hop.core.Const;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.vfs.HopVfs;
import org.apache.hop.vfs.hdfs.metadata.HdfsMeta;

/**
 * Builds an {@link SSLContext} from the HDFS connection TLS settings. Accepts a Java keystore
 * (JKS/PKCS12) or a PEM/CRT file of CA certificates (CDP Auto-TLS often ships {@code ca.pem}).
 */
public final class HdfsTls {
  public static final String KIND_DEFAULT = "JVM default";
  public static final String KIND_PEM = "PEM";
  public static final String KIND_KEYSTORE = "Java keystore";

  private static final Pattern PEM_CERT =
      Pattern.compile(
          "-----BEGIN(?: TRUSTED)? CERTIFICATE-----[\\s\\S]*?-----END(?: TRUSTED)? CERTIFICATE-----");

  private HdfsTls() {}

  public record TrustMaterial(SSLContext sslContext, String kind, int certificateCount) {}

  public static TrustMaterial load(IVariables variables, HdfsMeta meta) throws Exception {
    String path = variables.resolve(Const.NVL(meta.getTruststorePath(), ""));
    if (path.isBlank()) {
      return new TrustMaterial(SSLContexts.createDefault(), KIND_DEFAULT, 0);
    }
    byte[] bytes;
    try (InputStream in = HopVfs.getInputStream(path, variables)) {
      bytes = in.readAllBytes();
    }
    if (looksLikePem(path, bytes)) {
      return loadPem(bytes);
    }
    return loadKeyStore(bytes, variables.resolve(Const.NVL(meta.getTruststorePassword(), "")));
  }

  public static SSLContext sslContext(IVariables variables, HdfsMeta meta) throws Exception {
    return load(variables, meta).sslContext();
  }

  static boolean looksLikePem(String path, byte[] bytes) {
    String lower = path.toLowerCase(Locale.ROOT);
    if (lower.endsWith(".pem")
        || lower.endsWith(".crt")
        || lower.endsWith(".cer")
        || lower.endsWith(".cert")) {
      return true;
    }
    String head = new String(bytes, 0, Math.min(bytes.length, 256), StandardCharsets.US_ASCII);
    return head.stripLeading().startsWith("-----BEGIN");
  }

  static TrustMaterial loadPem(byte[] bytes) throws Exception {
    String text = new String(bytes, StandardCharsets.US_ASCII);
    Matcher matcher = PEM_CERT.matcher(text);
    CertificateFactory factory = CertificateFactory.getInstance("X.509");
    List<Certificate> certs = new ArrayList<>();
    while (matcher.find()) {
      byte[] block = matcher.group().getBytes(StandardCharsets.US_ASCII);
      certs.add(factory.generateCertificate(new ByteArrayInputStream(block)));
    }
    if (certs.isEmpty()) {
      throw new IllegalArgumentException(
          "No certificates found in the PEM file (expected -----BEGIN CERTIFICATE----- blocks)");
    }
    KeyStore keyStore = KeyStore.getInstance(KeyStore.getDefaultType());
    keyStore.load(null);
    int i = 0;
    for (Certificate cert : certs) {
      keyStore.setCertificateEntry("pem-" + i++, cert);
    }
    SSLContext context = SSLContexts.custom().loadTrustMaterial(keyStore, null).build();
    return new TrustMaterial(context, KIND_PEM, certs.size());
  }

  static TrustMaterial loadKeyStore(byte[] bytes, String password) throws Exception {
    char[] pwd = password == null ? new char[0] : password.toCharArray();
    Exception last = null;
    for (String type : new String[] {"PKCS12", "JKS", KeyStore.getDefaultType()}) {
      try {
        KeyStore keyStore = KeyStore.getInstance(type);
        keyStore.load(new ByteArrayInputStream(bytes), pwd);
        SSLContext context = SSLContexts.custom().loadTrustMaterial(keyStore, null).build();
        return new TrustMaterial(context, KIND_KEYSTORE + " (" + type + ")", keyStore.size());
      } catch (Exception e) {
        last = e;
      }
    }
    throw last == null ? new IllegalArgumentException("Unable to load trust material") : last;
  }
}
