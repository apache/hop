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

package org.apache.hop.pipeline.transforms.maskfields;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.security.GeneralSecurityException;
import java.util.HexFormat;
import java.util.Locale;
import javax.crypto.Mac;
import javax.crypto.spec.SecretKeySpec;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.encryption.Encr;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.exception.HopValueException;
import org.apache.hop.core.row.IValueMeta;
import org.apache.hop.core.util.StringUtil;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.i18n.BaseMessages;

/**
 * Builds the key a remembering pattern finds a source value by. The key ignores the field's
 * conversion mask, can ignore spaces around the value and its case, and is an HMAC of the value
 * when the pattern has a hash secret.
 */
public final class MaskingKey {

  private static final Class<?> PKG = MaskingKey.class;

  static final String HMAC_PREFIX = "hmac-sha256:";
  private static final String ALGORITHM = "HmacSHA256";

  private final boolean trim;
  private final boolean ignoreCase;
  private final Mac mac;

  public MaskingKey(boolean trim, boolean ignoreCase, String secret) throws HopException {
    this.trim = trim;
    this.ignoreCase = ignoreCase;
    if (StringUtils.isEmpty(secret)) {
      this.mac = null;
    } else {
      try {
        Mac hmac = Mac.getInstance(ALGORITHM);
        hmac.init(new SecretKeySpec(secret.getBytes(StandardCharsets.UTF_8), ALGORITHM));
        this.mac = hmac;
      } catch (GeneralSecurityException e) {
        throw new HopException("Unable to initialize " + ALGORITHM, e);
      }
    }
  }

  /**
   * The key for a pattern. Only a database pattern hashes its keys. A hash secret that resolves to
   * nothing, still holds a variable or does not decrypt is an error: hashing with the literal text
   * or falling back to plain text would both go unnoticed.
   */
  public static MaskingKey forPattern(MaskingPattern pattern, IVariables variables)
      throws HopException {
    String secret = null;
    if (pattern.getStorage() == MaskingStorage.DATABASE
        && StringUtils.isNotEmpty(pattern.getHashSecret())) {
      String resolved = variables.resolve(pattern.getHashSecret());
      if (!StringUtils.isEmpty(resolved) && !StringUtil.containsVariableToken(resolved)) {
        // An encrypted value that is not a valid ciphertext decrypts to an empty string.
        secret = Encr.decryptPasswordOptionallyEncrypted(resolved);
      }
      if (StringUtils.isEmpty(secret)) {
        throw new HopException(
            BaseMessages.getString(
                PKG, "MaskFields.Error.HashSecretUnresolved", pattern.getName()));
      }
    }
    return new MaskingKey(pattern.isTrimKey(), pattern.isIgnoreCase(), secret);
  }

  /** Plain key: no trimming, case-sensitive and not hashed. */
  public static MaskingKey plain() {
    try {
      return new MaskingKey(false, false, null);
    } catch (HopException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * @return the value as the pattern compares it, or null or empty when there is nothing to mask
   */
  public String normalize(IValueMeta valueMeta, Object value) throws HopValueException {
    if (value == null) {
      return null;
    }
    String text = canonical(valueMeta, value);
    if (text == null) {
      return null;
    }
    if (trim) {
      text = text.trim();
    }
    if (ignoreCase) {
      text = text.toLowerCase(Locale.ROOT);
    }
    return text;
  }

  /** The key stored in a mapping, built from {@link #normalize(IValueMeta, Object)}. */
  public String storeKey(String normalized) {
    if (mac == null) {
      return normalized;
    }
    byte[] digest;
    synchronized (mac) {
      digest = mac.doFinal(normalized.getBytes(StandardCharsets.UTF_8));
    }
    return HMAC_PREFIX + HexFormat.of().formatHex(digest);
  }

  /**
   * The key earlier versions stored: the value as the field formats it, in plain text. A mapping
   * table can still hold rows with that key.
   */
  public static String legacyKey(IValueMeta valueMeta, Object value) throws HopValueException {
    return valueMeta.getString(value);
  }

  private static String canonical(IValueMeta valueMeta, Object value) throws HopValueException {
    if (!valueMeta.isNumeric()) {
      return valueMeta.getString(value);
    }
    Object number = valueMeta.getNativeDataType(value);
    if (number instanceof Long longValue) {
      return Long.toString(longValue);
    }
    if (number instanceof Double doubleValue && Double.isFinite(doubleValue)) {
      return BigDecimal.valueOf(doubleValue).stripTrailingZeros().toPlainString();
    }
    if (number instanceof BigDecimal bigDecimal) {
      return bigDecimal.stripTrailingZeros().toPlainString();
    }
    return valueMeta.getString(value);
  }
}
