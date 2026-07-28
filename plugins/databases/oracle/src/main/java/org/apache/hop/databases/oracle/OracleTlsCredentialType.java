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

package org.apache.hop.databases.oracle;

/**
 * Where the Oracle JDBC driver should take its TLS certificates from when connecting over TCPS.
 *
 * <p>Wallet and Java KeyStore are deliberately exclusive: the driver gives {@code
 * oracle.net.wallet_location} precedence over the {@code javax.net.ssl.*} properties, so offering
 * both at once would let half the dialog be silently ignored.
 *
 * <p>The constant names are shown as-is in the connection dialog and are what gets serialized, so
 * renaming one breaks existing connections.
 */
public enum OracleTlsCredentialType {
  /**
   * No certificates configured on the connection. The server certificate is validated against the
   * JVM's default trust store, and the server does not ask the client for one.
   */
  NONE,

  /** An Oracle Wallet directory holding {@code cwallet.sso} or {@code ewallet.p12}. */
  WALLET,

  /** Java KeyStore files, i.e. the {@code javax.net.ssl.trustStore} / {@code keyStore} pair. */
  JKS
}
