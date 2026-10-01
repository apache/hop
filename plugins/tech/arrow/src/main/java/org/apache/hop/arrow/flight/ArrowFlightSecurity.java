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

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import lombok.AllArgsConstructor;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.apache.arrow.flight.CallStatus;
import org.apache.arrow.flight.auth2.BasicCallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.CallHeaderAuthenticator;
import org.apache.arrow.flight.auth2.GeneratedBearerTokenAuthenticator;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.logging.ILogChannel;
import org.apache.hop.core.util.Utils;
import org.apache.hop.core.vfs.HopVfs;

/**
 * The transport security and authentication settings of the Hop Apache Arrow Flight server. Both
 * are optional and disabled when nothing is configured, which keeps the plain gRPC behavior of
 * earlier versions.
 */
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class ArrowFlightSecurity {

  /** Neither TLS nor authentication: plain gRPC on an open port. */
  public static final ArrowFlightSecurity NONE = new ArrowFlightSecurity();

  /** The PEM file with the server certificate chain. Enables TLS together with the private key. */
  private String certificateFile;

  /** The PEM file with the (PKCS#8) private key belonging to the server certificate. */
  private String privateKeyFile;

  /**
   * The PEM file with the certificate authority used to verify client certificates. Enables mutual
   * TLS.
   */
  private String clientCertificateAuthorityFile;

  /** The user name clients need to present, or empty to accept unauthenticated clients. */
  private String username;

  /** The password belonging to {@link #username}. */
  private String password;

  public boolean isTlsEnabled() {
    return !Utils.isEmpty(certificateFile) || !Utils.isEmpty(privateKeyFile);
  }

  public boolean isMutualTlsEnabled() {
    return !Utils.isEmpty(clientCertificateAuthorityFile);
  }

  public boolean isAuthenticationEnabled() {
    return !Utils.isEmpty(username);
  }

  /**
   * Verify that the combination of options makes sense and warn about the ones that are legal but
   * unwise.
   *
   * @param log the channel to log warnings on
   * @throws HopException in case the options can't be used to start a server
   */
  public void validate(ILogChannel log) throws HopException {
    if (!Utils.isEmpty(certificateFile) && Utils.isEmpty(privateKeyFile)) {
      throw new HopException(
          "Please also specify the TLS private key file for the Arrow Flight server certificate.");
    }
    if (Utils.isEmpty(certificateFile) && !Utils.isEmpty(privateKeyFile)) {
      throw new HopException(
          "Please also specify the TLS certificate file for the Arrow Flight server private key.");
    }
    if (isMutualTlsEnabled() && !isTlsEnabled()) {
      throw new HopException(
          "Client certificate verification needs TLS: please also specify a certificate and private key for the Arrow Flight server.");
    }
    if (isAuthenticationEnabled() && Utils.isEmpty(password)) {
      throw new HopException(
          "Please also specify a password for Arrow Flight server user '" + username + "'.");
    }

    if (!isTlsEnabled()) {
      if (isAuthenticationEnabled()) {
        log.logMinimal(
            "WARNING: the Arrow Flight server is not using TLS: credentials and data are sent in the clear.");
      } else {
        log.logMinimal(
            "WARNING: the Arrow Flight server has no TLS and no authentication configured: anything that can reach the port can read the data it streams.");
      }
    }
  }

  /**
   * Builds the authenticator which validates the credentials clients present and hands them a
   * bearer token for the calls that follow.
   *
   * @return the authenticator, never null when {@link #isAuthenticationEnabled()}
   */
  public CallHeaderAuthenticator createAuthenticator() {
    String expectedUsername = username;
    String expectedPassword = password;
    return new GeneratedBearerTokenAuthenticator(
        new BasicCallHeaderAuthenticator(
            (user, pass) -> {
              if (equalsConstantTime(expectedUsername, user)
                  && equalsConstantTime(expectedPassword, pass)) {
                return () -> user;
              }
              throw CallStatus.UNAUTHENTICATED
                  .withDescription("Invalid credentials for the Hop Arrow Flight server")
                  .toRuntimeException();
            }));
  }

  /**
   * Reads a certificate or key file through Apache VFS. The content is kept in memory: these files
   * are small and the Flight builders only consume the streams when the server or client is built.
   *
   * @param filename the VFS filename to read
   * @return the file content
   * @throws HopException in case the file could not be read
   */
  public static byte[] readPemFile(String filename) throws HopException {
    try (InputStream inputStream = HopVfs.getInputStream(filename)) {
      return inputStream.readAllBytes();
    } catch (IOException e) {
      throw new HopException("Unable to read certificate or key file '" + filename + "'", e);
    }
  }

  private static boolean equalsConstantTime(String expected, String actual) {
    if (expected == null || actual == null) {
      return false;
    }
    return MessageDigest.isEqual(
        expected.getBytes(StandardCharsets.UTF_8), actual.getBytes(StandardCharsets.UTF_8));
  }
}
