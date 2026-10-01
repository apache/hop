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

package org.apache.hop.vfs.sftp.provider;

import com.jcraft.jsch.JSch;
import com.jcraft.jsch.JSchException;
import java.util.Arrays;
import java.util.Objects;

/**
 * Structure for an identity based on byte arrays.
 *
 * @since 2.4
 */
public class BytesIdentityInfo implements IdentityProvider {

  private final byte[] passphrase;

  private final byte[] privateKey;

  private final byte[] publicKey;

  /**
   * Constructs an identity info with private and passphrase for the private key.
   *
   * @param privateKey Private key bytes
   * @param passphrase The passphrase to decrypt the private key (can be {@code null} if no
   *     passphrase is used)
   */
  public BytesIdentityInfo(final byte[] privateKey, final byte[] passphrase) {
    this(privateKey, null, passphrase);
  }

  /**
   * Constructs an identity info with private and public key and passphrase for the private key.
   *
   * @param privateKey Private key bytes
   * @param publicKey The public key part used for connections with exchange of certificates (can be
   *     {@code null})
   * @param passphrase The passphrase to decrypt the private key (can be {@code null} if no
   *     passphrase is used)
   */
  public BytesIdentityInfo(
      final byte[] privateKey, final byte[] publicKey, final byte[] passphrase) {
    this.privateKey = Utils.clone(privateKey);
    this.publicKey = Utils.clone(publicKey);
    this.passphrase = Utils.clone(passphrase);
  }

  @Override
  public void addIdentity(final JSch jsch) throws JSchException {
    jsch.addIdentity("PrivateKey", privateKey, publicKey, passphrase);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof BytesIdentityInfo)) {
      return false;
    }
    final BytesIdentityInfo other = (BytesIdentityInfo) obj;
    return Arrays.equals(passphrase, other.passphrase)
        && Arrays.equals(privateKey, other.privateKey)
        && Arrays.equals(publicKey, other.publicKey);
  }

  /**
   * Gets the passphrase.
   *
   * @return the passphrase.
   * @since 2.10.0
   */
  public byte[] getPassphrase() {
    return Utils.clone(passphrase);
  }

  /**
   * Gets the passphrase.
   *
   * @return the passphrase.
   * @deprecated Use {@link #getPassphrase()}.
   */
  @Deprecated
  public byte[] getPassPhrase() {
    return Utils.clone(passphrase);
  }

  /**
   * Gets the private key.
   *
   * @return the private key.
   */
  public byte[] getPrivateKeyBytes() {
    return Utils.clone(privateKey);
  }

  /**
   * Gets the public key.
   *
   * @return the public key.
   */
  public byte[] getPublicKeyBytes() {
    return Utils.clone(publicKey);
  }

  @Override
  public int hashCode() {
    return Objects.hash(
        Arrays.hashCode(passphrase), Arrays.hashCode(privateKey), Arrays.hashCode(publicKey));
  }
}
