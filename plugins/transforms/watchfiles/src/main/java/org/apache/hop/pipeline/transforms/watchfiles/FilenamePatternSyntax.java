/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License. You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hop.pipeline.transforms.watchfiles;

import java.util.regex.Pattern;

/** Explicit filename syntax avoids guessing whether an existing expression is a wildcard. */
public enum FilenamePatternSyntax {
  REGEXP,
  WILDCARD;

  public String toRegex(String value) {
    if (value == null || value.isEmpty()) {
      return "";
    }
    if (this == REGEXP) {
      return value;
    }
    StringBuilder regex = new StringBuilder("(?s)");
    value
        .codePoints()
        .forEach(
            character -> {
              switch (character) {
                case '*' -> regex.append(".*");
                case '?' -> regex.append('.');
                default -> regex.append(Pattern.quote(new String(Character.toChars(character))));
              }
            });
    return regex.toString();
  }
}
