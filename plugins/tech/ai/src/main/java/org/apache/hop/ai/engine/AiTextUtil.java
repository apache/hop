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

package org.apache.hop.ai.engine;

import java.util.regex.Pattern;
import org.apache.hop.core.util.Utils;

public final class AiTextUtil {

  private static final String SECRET_NAME =
      "password|pass|pwd|secret|apikey|api[_-]?key|token|access[_-]?token";
  private static final Pattern SECRET_XML_TAG =
      Pattern.compile("(?is)<(" + SECRET_NAME + ")>.*?</\\1>");
  private static final Pattern SECRET_XML_ATTR =
      Pattern.compile("(?i)(" + SECRET_NAME + ")=\"[^\"]*\"");
  private static final Pattern SECRET_KEY_VALUE =
      Pattern.compile(
          "(?i)(\"?(?:" + SECRET_NAME + ")\"?\\s*[:=]\\s*)(\"[^\"]*\"|'[^']*'|[^\\s,;}]+)");

  private AiTextUtil() {}

  public static String truncate(String value, int maxChars) {
    if (Utils.isEmpty(value) || maxChars <= 0) {
      return value != null ? value : "";
    }
    if (value.length() <= maxChars) {
      return value;
    }
    return value.substring(0, maxChars) + "\n... [truncated]";
  }

  public static String jsonString(String value) {
    if (value == null) {
      return "null";
    }
    return "\""
        + value
            .replace("\\", "\\\\")
            .replace("\"", "\\\"")
            .replace("\r", "\\r")
            .replace("\n", "\\n")
            .replace("\t", "\\t")
        + "\"";
  }

  public static String redactSecrets(String value) {
    if (Utils.isEmpty(value)) {
      return value != null ? value : "";
    }
    String redacted = SECRET_XML_TAG.matcher(value).replaceAll("<$1>***</$1>");
    redacted = SECRET_XML_ATTR.matcher(redacted).replaceAll("$1=\"***\"");
    redacted = SECRET_KEY_VALUE.matcher(redacted).replaceAll("$1\"***\"");
    return redacted;
  }
}
