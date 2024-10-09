/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 *
 */

package org.apache.hop.passwords.aes;

import java.nio.charset.StandardCharsets;
import java.security.MessageDigest;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Base64;
import java.util.List;
import javax.crypto.Cipher;
import javax.crypto.spec.GCMParameterSpec;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.lang.StringUtils;
import org.apache.hop.core.encryption.ITwoWayPasswordEncoder;
import org.apache.hop.core.encryption.TwoWayPasswordEncoderPlugin;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.Variables;

/**
 * We expect a few variables to be set for this plugin to be picked up: 1.
 * HOP_PASSWORD_ENCODER_PLUGIN set to the ID of this plugin:"AES" 2. HOP_AES_ENCODER_KEY set to the
 * key of your choice.
 */
@TwoWayPasswordEncoderPlugin(
    id = "AES2",
    name = "AES2 Password encoder",
    description = "Allows for 128/192/256 bit password encryption of passwords in Hop")
public class Aes2TwoWayPasswordEncoder implements ITwoWayPasswordEncoder {

  public static final String VARIABLE_HOP_AES_ENCODER_KEY = "HOP_AES_ENCODER_KEY";
  public static final String AES_PREFIX = "AES2 ";
  public static final String AES_ALGORITHM = "AES/GCM/NoPadding";

  private Cipher encryptCipher;
  private Cipher decryptCipher;

  @Override
  public void init() throws HopException {

    try {
      String aesKey = System.getProperty(VARIABLE_HOP_AES_ENCODER_KEY, null);
      if (StringUtils.isEmpty(aesKey)) {
        noKeySpecified();
      }
      String realAesKey = Variables.getADefaultVariableSpace().resolve(aesKey);
      if (StringUtils.isEmpty(realAesKey)) {
        noKeySpecified();
      }
      byte[] key = realAesKey.getBytes(StandardCharsets.UTF_8);
      MessageDigest messageDigest = MessageDigest.getInstance("SHA-512");
      byte[] digestKey = messageDigest.digest(key);
      byte[] copiedKey = Arrays.copyOf(digestKey, 16);
      SecretKeySpec secretKeySpec = new SecretKeySpec(copiedKey, "AES");
      GCMParameterSpec parameterSpec = new GCMParameterSpec(128, copiedKey);

      // Create the cyphers that will do the encoding/decoding below...
      //
      encryptCipher = Cipher.getInstance(AES_ALGORITHM);
      encryptCipher.init(Cipher.ENCRYPT_MODE, secretKeySpec, parameterSpec);

      decryptCipher = Cipher.getInstance(AES_ALGORITHM);
      decryptCipher.init(Cipher.DECRYPT_MODE, secretKeySpec, parameterSpec);
    } catch (Exception e) {
      throw new HopException("Error initializing AES password encoder plugin", e);
    }
  }

  private void noKeySpecified() throws HopException {
    throw new HopException(
        "Please specify a key to encrypt/decrypt with by setting variable "
            + VARIABLE_HOP_AES_ENCODER_KEY
            + " in the system properties");
  }

  @Override
  public String encode(String password) {
    return encode(password, true);
  }

  @Override
  public String encode(String password, boolean includePrefix) {
    if (StringUtils.isEmpty(password)) {
      return password;
    }
    try {
      if (includePrefix) {
        return encryptPasswordIfNotUsingVariablesInternal(password);
      } else {
        return encodeInternal(password);
      }
    } catch (Exception e) {
      throw new RuntimeException("Error encoding password using AES", e);
    }
  }

  private String encodeInternal(String password) {
    if (StringUtils.isEmpty(password)) {
      return password;
    }
    try {
      return Base64.getEncoder()
          .encodeToString(encryptCipher.doFinal(password.getBytes(StandardCharsets.UTF_8)));
    } catch (Exception e) {
      throw new RuntimeException("Error encoding password using AES", e);
    }
  }

  @Override
  public String decode(String encodedPassword, boolean optionallyEncrypted) {
    if (StringUtils.isEmpty(encodedPassword)) {
      return encodedPassword;
    }
    if (optionallyEncrypted) {
      if (encodedPassword.startsWith(AES_PREFIX)) {
        encodedPassword = encodedPassword.substring(AES_PREFIX.length());
        return decodeOnly(encodedPassword);
      } else {
        return encodedPassword;
      }
    } else {
      return decodeOnly(encodedPassword);
    }
  }

  @Override
  public String decode(String encodedPassword) {
    if (StringUtils.isEmpty(encodedPassword)) {
      return encodedPassword;
    }
    if (encodedPassword.startsWith(AES_PREFIX)) {
      encodedPassword = encodedPassword.substring(AES_PREFIX.length());
    }

    return decodeOnly(encodedPassword);
  }

  /**
   * Encrypt the password, but only if the password doesn't contain any variables.
   *
   * @param password The password to encrypt
   * @return The encrypted password or the
   */
  protected final String encryptPasswordIfNotUsingVariablesInternal(String password) {
    String encryptedPassword = "";
    List<String> varList = new ArrayList<>();
    StringUtil.getUsedVariables(password, varList, true);
    if (varList.isEmpty()) {
      encryptedPassword = AES_PREFIX + encodeInternal(password);
    } else {
      encryptedPassword = password;
    }

    return encryptedPassword;
  }

  private String decodeOnly(String encodedPassword) {
    try {
      return new String(decryptCipher.doFinal(Base64.getDecoder().decode(encodedPassword)));
    } catch (Exception e) {
      throw new RuntimeException("Error decoding password using AES", e);
    }
  }

  @Override
  public String[] getPrefixes() {
    return new String[] {AES_PREFIX};
  }
}
