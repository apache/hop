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

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import org.apache.commons.lang3.StringUtils;
import org.apache.hop.core.Const;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.variables.IVariables;
import org.apache.hop.core.variables.Variables;
import org.apache.hop.core.vfs.HopVfs;

/**
 * Shared key material resolution for AES / AES2 password encoders.
 *
 * <p>Order: {@code HOP_AES_ENCODER_KEY} (variables then system property), then contents of {@code
 * HOP_AES_ENCODER_KEY_FILE}.
 */
final class AesEncoderKeyUtil {

  private AesEncoderKeyUtil() {
    // utility
  }

  static String resolveKey(IVariables variables) throws HopException {
    if (variables == null) {
      variables = Variables.getADefaultVariableSpace();
    }

    String key = firstNonEmpty(variables.getVariable(Const.HOP_AES_ENCODER_KEY));
    if (StringUtils.isEmpty(key)) {
      key = firstNonEmpty(System.getProperty(Const.HOP_AES_ENCODER_KEY));
    }
    if (StringUtils.isNotEmpty(key)) {
      String resolved = variables.resolve(key);
      if (StringUtils.isNotEmpty(resolved)) {
        return resolved;
      }
    }

    String keyFile = firstNonEmpty(variables.getVariable(Const.HOP_AES_ENCODER_KEY_FILE));
    if (StringUtils.isEmpty(keyFile)) {
      keyFile = firstNonEmpty(System.getProperty(Const.HOP_AES_ENCODER_KEY_FILE));
    }
    if (StringUtils.isNotEmpty(keyFile)) {
      String resolvedPath = variables.resolve(keyFile);
      if (StringUtils.isNotEmpty(resolvedPath)) {
        return readKeyFile(resolvedPath);
      }
    }

    throw new HopException(
        "Please specify a key to encrypt/decrypt with by setting variable "
            + Const.HOP_AES_ENCODER_KEY
            + " or "
            + Const.HOP_AES_ENCODER_KEY_FILE
            + " (system properties or project/environment variables)");
  }

  private static String readKeyFile(String path) throws HopException {
    try (InputStream inputStream = HopVfs.getInputStream(path)) {
      byte[] bytes = inputStream.readAllBytes();
      String content = new String(bytes, StandardCharsets.UTF_8);
      // Trim trailing newlines common in secret files; do not trim leading/internal spaces.
      return content.replaceAll("[\\r\\n]+$", "");
    } catch (Exception e) {
      throw new HopException("Error reading AES encoder key from file '" + path + "'", e);
    }
  }

  private static String firstNonEmpty(String value) {
    return StringUtils.isEmpty(value) ? null : value;
  }
}
