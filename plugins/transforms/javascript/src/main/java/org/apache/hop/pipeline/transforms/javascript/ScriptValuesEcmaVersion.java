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

package org.apache.hop.pipeline.transforms.javascript;

import java.util.Arrays;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;
import org.apache.hop.i18n.BaseMessages;
import org.mozilla.javascript.Context;

/**
 * Supported Mozilla Rhino ECMAScript / JavaScript language levels for the JavaScript transform.
 *
 * <p>Stable codes are stored in pipeline XML. The engine default used by recent Rhino releases (and
 * by this transform when the field is empty) is {@link #ES6}.
 */
public enum ScriptValuesEcmaVersion {
  /** Legacy Rhino default; limited modern syntax (for example no {@code let}). */
  DEFAULT("DEFAULT", Context.VERSION_DEFAULT),
  JS_1_5("1.5", Context.VERSION_1_5),
  JS_1_6("1.6", Context.VERSION_1_6),
  JS_1_7("1.7", Context.VERSION_1_7),
  JS_1_8("1.8", Context.VERSION_1_8),
  /**
   * ECMAScript 6 and later features implemented by Rhino. Default for new transforms and for Rhino
   * itself since 1.8.
   */
  ES6("ES6", Context.VERSION_ES6),
  /**
   * Latest ECMAScript level supported by the embedded Rhino engine (may enable stricter checks than
   * {@link #ES6}).
   */
  ECMASCRIPT("ECMASCRIPT", Context.VERSION_ECMASCRIPT);

  private static final Class<?> PKG = ScriptValuesMeta.class;

  /** Stable code stored in metadata / XML for new transforms (matches Rhino's default). */
  public static final String DEFAULT_CODE = ES6.code;

  private final String code;
  private final int rhinoVersion;

  ScriptValuesEcmaVersion(String code, int rhinoVersion) {
    this.code = code;
    this.rhinoVersion = rhinoVersion;
  }

  public String getCode() {
    return code;
  }

  public int getRhinoVersion() {
    return rhinoVersion;
  }

  /** Localized UI label for this level. */
  public String getDescription() {
    return BaseMessages.getString(PKG, "ScriptValuesMod.LanguageVersion." + code);
  }

  /** All localized descriptions in enum order (for combo boxes). */
  public static String[] getDescriptions() {
    return Arrays.stream(values())
        .map(ScriptValuesEcmaVersion::getDescription)
        .toArray(String[]::new);
  }

  /** Codes only, for tests and non-UI lists. */
  public static String[] getCodes() {
    return Arrays.stream(values()).map(v -> v.code).toArray(String[]::new);
  }

  /**
   * Resolve a stored code, description, or numeric Rhino version to a known level. Empty input maps
   * to {@link #ES6} (engine default / compatible with existing pipelines on recent Rhino).
   */
  public static ScriptValuesEcmaVersion fromCode(String value) throws HopException {
    if (Utils.isEmpty(value) || Utils.isEmpty(value.trim())) {
      return ES6;
    }
    String trimmed = value.trim();

    for (ScriptValuesEcmaVersion version : values()) {
      if (version.code.equalsIgnoreCase(trimmed)) {
        return version;
      }
    }

    for (ScriptValuesEcmaVersion version : values()) {
      if (version.getDescription().equals(trimmed)) {
        return version;
      }
    }

    // Numeric Rhino language version (e.g. 200, 250)
    try {
      int numeric = Integer.parseInt(trimmed);
      for (ScriptValuesEcmaVersion version : values()) {
        if (version.rhinoVersion == numeric) {
          return version;
        }
      }
    } catch (NumberFormatException ignored) {
      // fall through to error
    }

    throw new HopException(
        BaseMessages.getString(
            PKG, "ScriptValuesMetaMod.Exception.UnsupportedLanguageVersion", trimmed));
  }

  /**
   * Map a UI description back to the stable storage code. Falls back to the text itself when it is
   * already a code or a variable expression.
   */
  public static String codeFromDescription(String description) {
    if (Utils.isEmpty(description)) {
      return DEFAULT_CODE;
    }
    String trimmed = description.trim();
    for (ScriptValuesEcmaVersion version : values()) {
      if (version.getDescription().equals(trimmed) || version.code.equalsIgnoreCase(trimmed)) {
        return version.code;
      }
    }
    return trimmed;
  }

  /** Apply this language level to a Rhino {@link Context}. */
  public void applyTo(Context context) {
    context.setLanguageVersion(rhinoVersion);
  }

  /**
   * Resolve {@code languageVersion} (code or description) and set it on the context.
   *
   * @param context Rhino context
   * @param languageVersion language version code/description (already variable-resolved)
   */
  public static void apply(Context context, String languageVersion) throws HopException {
    fromCode(languageVersion).applyTo(context);
  }
}
