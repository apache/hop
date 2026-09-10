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

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.ByteArrayInputStream;
import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.KeyStore;
import java.security.cert.Certificate;
import java.security.cert.CertificateFactory;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.junit.rules.RestoreHopEnvironmentExtension;
import org.apache.hop.vfs.hdfs.metadata.HdfsMeta;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.junit.jupiter.api.io.TempDir;

@ExtendWith(RestoreHopEnvironmentExtension.class)
class HdfsTlsTest {

  // Self-signed CA generated for unit tests only (CN=Hop HDFS test CA).
  private static final String TEST_CA_PEM =
      """
      -----BEGIN CERTIFICATE-----
      MIIDFzCCAf+gAwIBAgIUUlmpiWjBdw9URBA86fRDaedJeRUwDQYJKoZIhvcNAQEL
      BQAwGzEZMBcGA1UEAwwQSG9wIEhERlMgdGVzdCBDQTAeFw0yNjA5MDkwODIwMDla
      Fw0zNjA5MDYwODIwMDlaMBsxGTAXBgNVBAMMEEhvcCBIREZTIHRlc3QgQ0EwggEi
      MA0GCSqGSIb3DQEBAQUAA4IBDwAwggEKAoIBAQDIfcmHQUHsVeZwFnyKDHVxex7z
      DOJ4pjYqE+nAqFZzr+fJ1hk+urDBZCtIDdm/Q8CqNY1neorauiwFHcRPmvVrdw+P
      cOr8WUtmfFKbOiEOibexBMnSqF+IvCl3tqEb+RIHgP97kprdP/pZsjn6lprpPjod
      QYmIrPCj+Ut90Z775K5kWgmcyAmAuxqjjqnnZVyDiyMA0IFQEiwclu1C/9YAbdoT
      FCJdtLU6LaYisFNpSCHHYXYSJ07r9FJhD2eqzJUAGe0GaBaQsQ76ZbRhHtwL/nEn
      Emsahkb5yj9gC86E9A0yrcHOHtzXNWoU677hWmOaQ8Qo0SszJ9km0Z9RojrVAgMB
      AAGjUzBRMB0GA1UdDgQWBBRmgOyQ+Kz9YuBZRaI5ZtpzAe9n8zAfBgNVHSMEGDAW
      gBRmgOyQ+Kz9YuBZRaI5ZtpzAe9n8zAPBgNVHRMBAf8EBTADAQH/MA0GCSqGSIb3
      DQEBCwUAA4IBAQB1sf8bKpgEeLap9U9o+Kl33NsFalctxaJWwCZhaJ/1+7V+TdsP
      Uj+h1yuq/wXje0UstehMJpICfkkAnTDH2X/B48SlgGMyXoQL2UbukAHwicVkJxHo
      CrYopDDrqqvntGtmMgdb0p6v4ge9Dmfe7RhcFPF1OzIW0vGcyCP1F5Xwc3MTGAPd
      a01Mhdj55QW3tOJiaxk8cMg+hyQrmqkgp5k9oocePwZaM5NcF/kp3Mz6TeEwlw+n
      mCRDCK18DOdLn+hyWW5gDTrLFWjUtzfoBoHbfn70vfdVds+FgDcv2RhZFEX7MGyd
      pYmypW0hYhsULTkPZSJWMBK8zliHXFl0c8Nt
      -----END CERTIFICATE-----
      """;

  @TempDir Path tempDir;

  @Test
  void emptyPathUsesJvmDefault() throws Exception {
    HdfsTls.TrustMaterial material = HdfsTls.load(new Variables(), new HdfsMeta());
    assertEquals(HdfsTls.KIND_DEFAULT, material.kind());
    assertEquals(0, material.certificateCount());
  }

  @Test
  void pemExtensionIsDetected() {
    assertTrue(HdfsTls.looksLikePem("ca.pem", new byte[] {0}));
    assertTrue(HdfsTls.looksLikePem("/tmp/auto-tls.crt", new byte[0]));
    assertTrue(HdfsTls.looksLikePem("trust.bin", "-----BEGIN CERTIFICATE-----\n".getBytes()));
  }

  @Test
  void loadsPemCertificates() throws Exception {
    Path pem = tempDir.resolve("ca.pem");
    Files.writeString(pem, TEST_CA_PEM);
    HdfsMeta meta = new HdfsMeta();
    meta.setTruststorePath(pem.toString());
    HdfsTls.TrustMaterial material = HdfsTls.load(new Variables(), meta);
    assertEquals(HdfsTls.KIND_PEM, material.kind());
    assertEquals(1, material.certificateCount());
    String report = HdfsConnectionTester.testTls(new Variables(), meta);
    assertTrue(report.contains("PEM"));
  }

  @Test
  void decryptsEncryptedKeystorePassword() throws Exception {
    CertificateFactory factory = CertificateFactory.getInstance("X.509");
    Certificate cert =
        factory.generateCertificate(
            new ByteArrayInputStream(TEST_CA_PEM.getBytes(StandardCharsets.US_ASCII)));
    KeyStore keyStore = KeyStore.getInstance("PKCS12");
    keyStore.load(null, "secret".toCharArray());
    keyStore.setCertificateEntry("ca", cert);
    Path p12 = tempDir.resolve("trust.p12");
    try (OutputStream out = Files.newOutputStream(p12)) {
      keyStore.store(out, "secret".toCharArray());
    }
    HdfsMeta meta = new HdfsMeta();
    meta.setTruststorePath(p12.toString());
    meta.setTruststorePassword(Encr.encryptPasswordIfNotUsingVariables("secret"));
    HdfsTls.TrustMaterial material = HdfsTls.load(new Variables(), meta);
    assertTrue(material.kind().startsWith(HdfsTls.KIND_KEYSTORE));
    assertTrue(material.certificateCount() >= 1);
  }

  @Test
  void pemWithoutCertificateFails() {
    byte[] onlyKey =
        "-----BEGIN PRIVATE KEY-----\nAAAA\n-----END PRIVATE KEY-----\n"
            .getBytes(StandardCharsets.US_ASCII);
    assertThrows(IllegalArgumentException.class, () -> HdfsTls.loadPem(onlyKey));
  }
}
