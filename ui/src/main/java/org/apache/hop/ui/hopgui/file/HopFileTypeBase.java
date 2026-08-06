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

package org.apache.hop.ui.hopgui.file;

import java.util.Locale;
import org.apache.hop.core.exception.HopException;
import org.apache.hop.core.util.Utils;

public abstract class HopFileTypeBase implements IHopFileType {

  @Override
  public boolean equals(Object obj) {
    if (obj == this) {
      return true;
    }
    // same class is enough
    return obj != null && obj.getClass().equals(this.getClass());
  }

  @Override
  public boolean isHandledBy(String filename, boolean checkContent) throws HopException {
    try {
      if (checkContent) {
        throw new HopException(
            "Generic file content validation is not possible at this time for file '"
                + filename
                + "'");
      } else {
        // Pure string matching — do not open the path via VFS (explorer lists thousands of files).
        String fileExtension = extractExtension(filename);
        if (Utils.isEmpty(fileExtension)) {
          return false;
        }

        String[] filters = getFilterExtensions();
        if (filters == null) {
          return false;
        }
        for (String typeExtension : filters) {
          if (Utils.isEmpty(typeExtension)) {
            continue;
          }
          // Support compound filters like "*.xlsx;*.xls"
          for (String part : typeExtension.split(";")) {
            String normalized = part.trim().toLowerCase(Locale.ROOT);
            if (normalized.endsWith(fileExtension)) {
              return true;
            }
          }
        }

        return false;
      }
    } catch (HopException e) {
      throw e;
    } catch (Exception e) {
      throw new HopException(
          "Unable to verify file handling of file '" + filename + "' by extension", e);
    }
  }

  /**
   * Extract the file extension from a path or URI without touching VFS. Returns lower-case
   * extension without the dot, or empty string when there is none (including names like {@code
   * .gitignore} where the only dot is the leading one).
   */
  public static String extractExtension(String filename) {
    if (Utils.isEmpty(filename)) {
      return "";
    }
    String name = filename;
    int query = name.indexOf('?');
    if (query >= 0) {
      name = name.substring(0, query);
    }
    int slash = Math.max(name.lastIndexOf('/'), name.lastIndexOf('\\'));
    if (slash >= 0 && slash < name.length() - 1) {
      name = name.substring(slash + 1);
    } else if (slash >= 0) {
      return "";
    }
    int dot = name.lastIndexOf('.');
    // dot at 0 is a hidden file without a real extension (e.g. .gitignore)
    if (dot <= 0 || dot >= name.length() - 1) {
      return "";
    }
    return name.substring(dot + 1).toLowerCase(Locale.ROOT);
  }

  /** Base name (last path segment) without VFS. */
  public static String extractBaseName(String filename) {
    if (Utils.isEmpty(filename)) {
      return "";
    }
    String name = filename;
    int query = name.indexOf('?');
    if (query >= 0) {
      name = name.substring(0, query);
    }
    int slash = Math.max(name.lastIndexOf('/'), name.lastIndexOf('\\'));
    if (slash >= 0 && slash < name.length() - 1) {
      return name.substring(slash + 1);
    }
    if (slash >= 0) {
      return "";
    }
    return name;
  }

  @Override
  public boolean hasCapability(String capability) {
    if (getCapabilities() == null) {
      return false;
    }
    Object available = getCapabilities().get(capability);
    if (available == null) {
      return false;
    }
    return "true".equalsIgnoreCase(available.toString());
  }
}
