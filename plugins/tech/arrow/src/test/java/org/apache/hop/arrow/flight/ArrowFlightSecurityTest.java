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
 *
 */

package org.apache.hop.arrow.flight;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;

import java.io.OutputStream;
import java.nio.charset.StandardCharsets;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.vfs.HopVfs;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class ArrowFlightSecurityTest {

  @TempDir private java.nio.file.Path tempDir;

  private final ILogChannel log = mock(ILogChannel.class);

  @Test
  void noOptionsMeansNoTlsAndNoAuthentication() throws HopException {
    ArrowFlightSecurity security = new ArrowFlightSecurity();

    assertFalse(security.isTlsEnabled());
    assertFalse(security.isMutualTlsEnabled());
    assertFalse(security.isAuthenticationEnabled());
    assertDoesNotThrow(() -> security.validate(log));
  }

  @Test
  void certificateAndKeyTogetherEnableTls() throws HopException {
    ArrowFlightSecurity security = new ArrowFlightSecurity();
    security.setCertificateFile("/tmp/server.crt");
    security.setPrivateKeyFile("/tmp/server.key");

    assertTrue(security.isTlsEnabled());
    security.validate(log);
  }

  @Test
  void aCertificateWithoutAKeyIsRejected() {
    ArrowFlightSecurity security = new ArrowFlightSecurity();
    security.setCertificateFile("/tmp/server.crt");

    assertThrows(HopException.class, () -> security.validate(log));
  }

  @Test
  void aKeyWithoutACertificateIsRejected() {
    ArrowFlightSecurity security = new ArrowFlightSecurity();
    security.setPrivateKeyFile("/tmp/server.key");

    assertThrows(HopException.class, () -> security.validate(log));
  }

  @Test
  void clientCertificateVerificationWithoutTlsIsRejected() {
    ArrowFlightSecurity security = new ArrowFlightSecurity();
    security.setClientCertificateAuthorityFile("/tmp/ca.crt");

    assertTrue(security.isMutualTlsEnabled());
    assertThrows(HopException.class, () -> security.validate(log));
  }

  @Test
  void aUsernameWithoutAPasswordIsRejected() {
    ArrowFlightSecurity security = new ArrowFlightSecurity();
    security.setUsername("hop");

    assertTrue(security.isAuthenticationEnabled());
    assertThrows(HopException.class, () -> security.validate(log));
  }

  @Test
  void certificateFilesAreReadThroughVfs() throws Exception {
    String filename = tempDir.resolve("certificate.pem").toString();
    byte[] content =
        "-----BEGIN CERTIFICATE-----\nnot a real one\n".getBytes(StandardCharsets.UTF_8);
    try (OutputStream outputStream = HopVfs.getOutputStream(filename, false)) {
      outputStream.write(content);
    }

    assertArrayEquals(content, ArrowFlightSecurity.readPemFile(filename));
  }

  @Test
  void readingAMissingCertificateFileThrows() {
    assertThrows(
        HopException.class,
        () -> ArrowFlightSecurity.readPemFile(tempDir.resolve("does-not-exist.pem").toString()));
  }
}
