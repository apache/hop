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

package org.apache.hop.core.naming;

import org.apache.commons.lang3.StringUtils;

/** Helpers for {@link NamingSchemeKind} and field-level naming type codes. */
public final class NamingSchemeKinds {

  public static final String FILE = "file";
  public static final String FOLDER = "folder";

  private NamingSchemeKinds() {
    // utility
  }

  public static boolean isFile(String kind) {
    return FILE.equalsIgnoreCase(kind);
  }

  public static boolean isFolder(String kind) {
    return FOLDER.equalsIgnoreCase(kind);
  }

  /**
   * @param type a class (may be null)
   * @return the first {@link NamingSchemeKind} on the class or a superclass, or null
   */
  public static String kindOf(Class<?> type) {
    if (type == null) {
      return null;
    }
    Class<?> cursor = type;
    while (cursor != null && cursor != Object.class) {
      NamingSchemeKind kind = cursor.getAnnotation(NamingSchemeKind.class);
      if (kind != null && StringUtils.isNotEmpty(kind.value())) {
        return kind.value();
      }
      cursor = cursor.getSuperclass();
    }
    return null;
  }
}
