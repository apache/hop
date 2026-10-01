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
import java.io.File;
import java.util.Arrays;
import java.util.Objects;

/**
 * Structure for an identity based on Files.
 *
 * @since 2.1
 */
public class IdentityInfo implements IdentityProvider {

  private final byte[] passphrase;
  private final File privateKey;
  private final File publicKey;

  /**
   * Constructs an identity info with private key.
   *
   * <p>The key is not passphrase protected.
   *
   * <p>We use java.io.File because JSch cannot deal with VFS FileObjects.
   *
   * @param privateKey The file with the private key
   * @since 2.1
   */
  public IdentityInfo(final File privateKey) {
    this(privateKey, null, null);
  }

  /**
   * Constructs an identity info with private key and its passphrase.
   *
   * <p>We use java.io.File because JSch cannot deal with VFS FileObjects.
   *
   * @param privateKey The file with the private key
   * @param passphrase The passphrase to decrypt the private key (can be {@code null} if no
   *     passphrase is used)
   * @since 2.1
   */
  public IdentityInfo(final File privateKey, final byte[] passphrase) {
    this(privateKey, null, passphrase);
  }

  /**
   * Constructs an identity info with private and public key and passphrase for the private key.
   *
   * <p>We use java.io.File because JSch cannot deal with VFS FileObjects.
   *
   * @param privateKey The file with the private key
   * @param publicKey The public key part used for connections with exchange of certificates (can be
   *     {@code null})
   * @param passphrase The passphrase to decrypt the private key (can be {@code null} if no
   *     passphrase is used)
   * @since 2.1
   */
  public IdentityInfo(final File privateKey, final File publicKey, final byte[] passphrase) {
    this.privateKey = getAbsoluteFile(privateKey);
    this.publicKey = getAbsoluteFile(publicKey);
    this.passphrase = Utils.clone(passphrase);
  }

  /**
   * @since 2.4
   */
  @Override
  public void addIdentity(final JSch jsch) throws JSchException {
    jsch.addIdentity(getAbsolutePath(privateKey), getAbsolutePath(publicKey), passphrase);
  }

  @Override
  public boolean equals(final Object obj) {
    if (this == obj) {
      return true;
    }
    if (!(obj instanceof IdentityInfo)) {
      return false;
    }
    final IdentityInfo other = (IdentityInfo) obj;
    return Arrays.equals(passphrase, other.passphrase)
        && Objects.equals(privateKey, other.privateKey)
        && Objects.equals(publicKey, other.publicKey);
  }

  private File getAbsoluteFile(final File privateKey) {
    return privateKey != null ? privateKey.getAbsoluteFile() : null;
  }

  private String getAbsolutePath(final File file) {
    return file != null ? file.getAbsolutePath() : null;
  }

  /**
   * Gets the passphrase of the private key.
   *
   * @return the passphrase
   * @since 2.10.0
   */
  public byte[] getPassphrase() {
    return Utils.clone(passphrase);
  }

  /**
   * Gets the passphrase of the private key.
   *
   * @return the passphrase
   * @since 2.1
   * @deprecated Use {@link #getPassphrase()}.
   */
  @Deprecated
  public byte[] getPassPhrase() {
    return Utils.clone(passphrase);
  }

  /**
   * Gets the file with the private key.
   *
   * @return the file
   * @since 2.1
   */
  public File getPrivateKey() {
    return privateKey;
  }

  /**
   * Gets the file with the public key.
   *
   * @return the file
   * @since 2.1
   */
  public File getPublicKey() {
    return publicKey;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + Arrays.hashCode(passphrase);
    return prime * result + Objects.hash(privateKey, publicKey);
  }
}
